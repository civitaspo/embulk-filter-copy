package org.embulk.filter.copy;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerProvider;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.grpc.ProduceRequest;
import org.embulk.grpc.ProduceResponse;
import org.embulk.grpc.RecordQueueGrpc;
import org.embulk.grpc.service.RecordQueueService;
import org.embulk.plugin.PluginClassLoader;
import org.embulk.plugin.common.DefaultColumnVisitor;
import org.embulk.service.EmbulkExecutorService;
import org.embulk.service.PageCopyService;
import org.embulk.service.PageQueueService;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.channels.MulticastChannel;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;

public class CopyFilterPlugin
        implements FilterPlugin
{
    private final static Logger logger = Exec.getLogger(CopyFilterPlugin.class);

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);


        try {
            new RecordQueueServer().start();
        }
        catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        logger.warn("server is built.");
        RecordQueueClient client = new RecordQueueClient();
        client.produce();

        Schema outputSchema = inputSchema;

        control.run(task.dump(), outputSchema);
    }

    interface Sendable<T>
    {
        T build();
    }

    private class RecordQueueClient
    {
        private final ManagedChannel channel;
        private final RecordQueueGrpc.RecordQueueBlockingStub blockingStub;

        RecordQueueClient()
        {
            channel = ManagedChannelBuilder
                    .forAddress("localhost", 50051)
                    .usePlaintext(true)
                    .build();
            blockingStub = RecordQueueGrpc.newBlockingStub(channel);
        }

        public void shutdown() throws InterruptedException {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }

        public void produce() {
            ProduceRequest req = ProduceRequest.newBuilder().build();
            ProduceResponse res;
            try {
                res = blockingStub.produce(req);
            } catch (StatusRuntimeException e) {
                logger.warn("RPC failed: {}", e.getStatus());
                return;
            }
            logger.info("{}", res);
        }

    }

    private class RecordQueueServer
    {
        private Server server;

        RecordQueueServer()
        {
        }

        private void start()
                throws IOException
        {
            /* The port on which the server should run */
            int port = 50051;

            server = ServerBuilder.foPPort(port)
                    .addService(new RecordQueueService())
                    .build()
                    .start();
            logger.info("Server started, listening on " + port);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                    System.err.println("*** shutting down gRPC server since JVM is shutting down");
                    RecordQueueServer.this.stop();
                    System.err.println("*** server shut down");
                }
            });
        }

        private void stop() {
            if (server != null) {
                server.shutdown();
            }
        }
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final EmbulkExecutorService embulkExecutorService = new EmbulkExecutorService(1, Exec.getInjector());

        final String queueName = String.format("%s-%d", Thread.currentThread().getName(), System.currentTimeMillis());
        PageQueueService.openNewTaskQueue(queueName);

        ConfigSource inputConfig = Exec.newConfigSource();
        inputConfig.set("type", "page");
        inputConfig.set("queue_name", queueName);
        inputConfig.set("columns", inputSchema);

        ConfigSource config = Exec.newConfigSource();
        config.set("in", inputConfig);
        config.set("filters", task.getConfig().getFilterConfig());
        config.set("out", task.getConfig().getOutputConfig());

        embulkExecutorService.executeAsync(config);

        return new PageOutput()
        {
            private final PageReader pageReader = new PageReader(inputSchema);
            private final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private final ColumnVisitor visitor = new DefaultColumnVisitor(pageReader, pageBuilder);

            @Override
            public void add(Page page)
            {
                logger.info("enqueue!!!");
                Page newPage = PageCopyService.copy(page);
                boolean isEnqueued = PageQueueService.enqueue(queueName, newPage);
                if (!isEnqueued) {
                    logger.warn("enqueue failed!");
                }
                pageReader.setPage(page);

                while (pageReader.nextRecord()) {
                    outputSchema.visitColumns(visitor);
                    pageBuilder.addRecord();
                }
            }

            @Override
            public void finish()
            {
                pageBuilder.finish();
            }

            @Override
            public void close()
            {
                pageBuilder.close();
                embulkExecutorService.waitExecutionFinished();
                embulkExecutorService.shutdown();
            }
        };
    }

    public interface PluginTask
            extends Task
    {
        @Config("config")
        EmbulkConfig getConfig();
    }

    public interface EmbulkConfig
            extends Task
    {
        @Config("filters")
        @ConfigDefault("[]")
        List<ConfigSource> getFilterConfig();

        @Config("out")
        ConfigSource getOutputConfig();
    }
}
