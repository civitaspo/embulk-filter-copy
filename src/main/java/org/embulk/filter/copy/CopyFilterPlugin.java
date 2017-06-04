package org.embulk.filter.copy;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.filter.copy.service.EmbulkExecutorService;
import org.embulk.filter.copy.service.OutForwardEventBuilder;
import org.embulk.filter.copy.service.OutForwardService;
import org.embulk.filter.copy.service.OutForwardVisitor;
import org.embulk.filter.copy.service.StandardColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampFormatter;
import org.slf4j.Logger;

import java.util.List;

public class CopyFilterPlugin
        implements FilterPlugin
{
    private final static Logger logger = Exec.getLogger(CopyFilterPlugin.class);

    public interface EmbulkConfig
            extends Task
    {
        @Config("filters")
        @ConfigDefault("[]")
        List<ConfigSource> getFilterConfig();

        @Config("out")
        ConfigSource getOutputConfig();
    }

    public interface PluginTask
            extends Task, TimestampFormatter.Task, OutForwardService.Task
    {
        @Config("config")
        EmbulkConfig getConfig();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        ConfigSource inputConfig = Exec.newConfigSource();
        inputConfig.set("type", "internal_forward");
        inputConfig.set("columns", inputSchema);
        inputConfig.set("default_timestamp_format", task.getDefaultTimestampFormat());
        inputConfig.set("default_timezone", task.getDefaultTimeZone());

        ConfigSource anotherConfig = Exec.newConfigSource();
        anotherConfig.set("in", inputConfig);
        anotherConfig.set("filters", task.getConfig().getFilterConfig());
        anotherConfig.set("out", task.getConfig().getOutputConfig());
        EmbulkExecutorService embulkExecutorService = new EmbulkExecutorService(Exec.getInjector());
        embulkExecutorService.executeAsync(anotherConfig);

        Schema outputSchema = inputSchema;

        control.run(task.dump(), outputSchema);

        OutForwardService.sendShutdownMessage(task);

        embulkExecutorService.waitExecutionFinished();
        embulkExecutorService.shutdown();
        logger.info("filter-copy: transaction finished.");
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        TimestampFormatter timestampFormatter = new TimestampFormatter(
                task.getJRuby(),
                task.getDefaultTimestampFormat(),
                task.getDefaultTimeZone());

        return new PageOutput()
        {
            private final PageReader pageReader = new PageReader(inputSchema);
            private final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private final StandardColumnVisitor standardColumnVisitor = new StandardColumnVisitor(pageReader, pageBuilder);
            private final OutForwardService outForwardService = new OutForwardService(task);
            private final OutForwardEventBuilder eventBuilder = new OutForwardEventBuilder(outputSchema, timestampFormatter);
            private final OutForwardVisitor outForwardVisitor = new OutForwardVisitor(pageReader, eventBuilder);

            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    outputSchema.visitColumns(outForwardVisitor);
                    eventBuilder.emitMessage(outForwardService);

                    outputSchema.visitColumns(standardColumnVisitor);
                    pageBuilder.addRecord();
                }
            }

            @Override
            public void finish()
            {
                outForwardService.finish();
                pageBuilder.finish();
            }

            @Override
            public void close()
            {
                outForwardService.close();
                pageBuilder.close();
            }

        };
    }
}
