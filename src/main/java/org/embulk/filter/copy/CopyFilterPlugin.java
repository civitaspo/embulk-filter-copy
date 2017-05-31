package org.embulk.filter.copy;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
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

import java.util.List;

public class CopyFilterPlugin
        implements FilterPlugin
{
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

        final String queueName = String.format("%s-%d", Thread.currentThread().getName(), System.currentTimeMillis());
        PageQueueService.openNewTaskQueue(queueName);

        ConfigSource inputConfig = Exec.newConfigSource();
        inputConfig.set("type", "influent");
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
