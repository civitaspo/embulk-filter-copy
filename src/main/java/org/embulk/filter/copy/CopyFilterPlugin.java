package org.embulk.filter.copy;

import com.google.common.collect.Lists;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.plugin.common.DefaultColumnVisitor;
import org.embulk.queue.PageEnqueueClient;
import org.embulk.service.EmbulkExecutorService;
import org.embulk.spi.Buffer;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.msgpack.value.ImmutableValue;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

public class CopyFilterPlugin
        implements FilterPlugin
{
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

    private final static Logger logger = Exec.getLogger(CopyFilterPlugin.class);

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema outputSchema = inputSchema;

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final EmbulkExecutorService embulkExecutorService = new EmbulkExecutorService(1, Exec.getInjector());
        String host = "localhost";
        int port = getAvailablePort();
        final PageEnqueueClient enqueueClient = new PageEnqueueClient(host, port);

        ConfigSource inputConfig = Exec.newConfigSource();
        inputConfig.set("type", "page");
        inputConfig.setNested("queue_server", Exec.newConfigSource()
                .set("host", host)
                .set("port", port));
        inputConfig.setNested("life_cycle_server", Exec.newConfigSource()
                .set("host", "localhost")
                .set("port", 0));
        inputConfig.set("columns", inputSchema);

        ConfigSource config = Exec.newConfigSource();
        config.set("in", inputConfig);
        config.set("filters", task.getConfig().getFilterConfig());
        config.set("out", task.getConfig().getOutputConfig());

        embulkExecutorService.executeAsync(config);

        try {
            Thread.sleep(1 * 1000);
        }
        catch (InterruptedException e) {
            logger.warn(e.getMessage(), e);
        }

        return new PageOutput()
        {
            private final PageReader pageReader = new PageReader(inputSchema);
            private final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private final ColumnVisitor visitor = new DefaultColumnVisitor(pageReader, pageBuilder);

            @Override
            public void add(Page page)
            {
                logger.info("enqueue!!!");
                boolean isEnqueued = enqueueClient.enqueue(copyPage(page));
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

    private Page copyPage(Page page)
    {
//        for (String s : page.getStringReferences()) {
//            logger.warn("copy:src: {}", s);
//        }
//        for (String s : page.getStringReferences()) {
//            logger.warn("copy:src2: {}", s);
//        }
        byte[] raw = page.buffer().array().clone();
        return Page.wrap(Buffer.wrap(page.buffer().array().clone()));
//        Buffer newBuf = Buffer.allocate(raw.length);
//        newBuf.setBytes(0, raw, 0, raw.length);
//        Page wrap = Page.wrap(newBuf)
//                .setStringReferences(Lists.<String>newArrayList())
//                .setValueReferences(Lists.<ImmutableValue>newArrayList());
//        for (String s : wrap.getStringReferences()) {
//            logger.warn("copy:wrap: {}", s);
//        }
//        return wrap;
    }

    private int getAvailablePort()
    {
        int maxRetry = 3; // TODO
        int numRetry = 0;
        while (true) {
            try (Socket socket = new Socket()) {
                socket.bind(null);
                return socket.getLocalPort();
            }
            catch (IOException e) {
                if (++numRetry >= maxRetry) {
                    throw new RuntimeException(e);
                }
                String m = String.format("Retry: %d, Message: %s", numRetry, e.getMessage());
                logger.warn(m, e);
            }
        }
    }
}
