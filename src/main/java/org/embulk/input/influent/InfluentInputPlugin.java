package org.embulk.input.influent;

import influent.forward.ForwardCallback;
import influent.forward.ForwardServer;
import org.embulk.config.Config;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.plugin.common.DefaultColumnVisitor;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class InfluentInputPlugin
        implements InputPlugin
{
    private final static Logger logger = Exec.getLogger(InfluentInputPlugin.class);

    public interface PluginTask
            extends Task
    {
        @Config("columns")
        SchemaConfig getColumns();

        @ConfigInject
        BufferAllocator getBufferAllocator();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();
        int taskCount = 1;  // number of run() method calls

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        PageBuilder pageBuilder = new PageBuilder(task.getBufferAllocator(), schema, output);
        PageReader pageReader = new PageReader(schema);
        ColumnVisitor visitor = new DefaultColumnVisitor(pageReader, pageBuilder);

        try {
            logger.info("embulk-input-influent: start dequeue");
            // lifecycle がいる！！

            ForwardServer server = new ForwardServer.Builder(
                    ForwardCallback.ofSyncConsumer(eventStream -> {
                        eventStream.getEntries().forEach(eventEntry -> {
                            eventEntry.getRecord().entrySet().forEach(valueValueEntry -> {
                                logger.info("{}", valueValueEntry);
                                logger.info("{}", valueValueEntry);
                            });
                        });
                        }, Executors.newFixedThreadPool(1)))
                    .localAddress(24224)
                    .build();

            server.start();

            AtomicBoolean isShutdown = new AtomicBoolean(false);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> isShutdown.set(true)));

            while (!isShutdown.get()) {
                logger.info("embulk-input-influent: running yet");
                try {
                    Thread.sleep(1000L);
                }
                catch (InterruptedException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
            logger.info("embulk-input-influent: input finished!");

            server.shutdown();
        }
        finally {
            pageBuilder.finish();
        }

        return Exec.newTaskReport(); // TODO
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }
}
