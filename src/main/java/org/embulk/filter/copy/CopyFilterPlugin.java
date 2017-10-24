package org.embulk.filter.copy;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.filter.copy.forward.InForwardService;
import org.embulk.filter.copy.forward.OutForwardEventBuilder;
import org.embulk.filter.copy.forward.OutForwardService;
import org.embulk.filter.copy.plugin.InternalForwardInputPlugin;
import org.embulk.filter.copy.runner.AsyncEmbulkRunnerService;
import org.embulk.filter.copy.runner.EmbulkRunner;
import org.embulk.filter.copy.spi.PageBuilder;
import org.embulk.filter.copy.spi.PageReader;
import org.embulk.filter.copy.spi.StandardColumnVisitor;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import java.util.List;

public class CopyFilterPlugin
        implements FilterPlugin
{
    private final static Logger logger = Exec.getLogger(CopyFilterPlugin.class);

    public interface EmbulkConfig
            extends Task
    {
        @Config("exec")
        @ConfigDefault("{}")
        ConfigSource getExecConfig();

        @Config("filters")
        @ConfigDefault("[]")
        List<ConfigSource> getFilterConfig();

        @Config("out")
        ConfigSource getOutputConfig();
    }

    public interface PluginTask
            extends Task, OutForwardService.Task, InForwardService.Task
    {
        @Config("config")
        EmbulkConfig getConfig();
    }

    private EmbulkRunner configure(PluginTask task, Schema schema)
    {
        ConfigSource inputConfig = Exec.newConfigSource();
        inputConfig.set("type", InternalForwardInputPlugin.PLUGIN_NAME);
        inputConfig.set("columns", schema);
        inputConfig.set("message_tag", task.getMessageTag());
        inputConfig.set("shutdown_tag", task.getShutdownTag());
        inputConfig.set("in_forward", task.getInForwardTask());

        return EmbulkRunner.builder()
                .execConfig(task.getConfig().getExecConfig())
                .inputConfig(inputConfig)
                .filterConfig(task.getConfig().getFilterConfig())
                .outputConfig(task.getConfig().getOutputConfig())
                .build();
    }

    private void withEmbulkRun(AsyncEmbulkRunnerService service, Runnable r)
    {
        service.startAsync();
        service.awaitRunning();
        r.run();
        service.awaitTerminated();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        EmbulkRunner embulkRunner = configure(task, inputSchema);
        AsyncEmbulkRunnerService embulkRunnerService = new AsyncEmbulkRunnerService(embulkRunner);

        withEmbulkRun(embulkRunnerService, () -> {
            Schema outputSchema = inputSchema;
            control.run(task.dump(), outputSchema);
            OutForwardService.sendShutdownMessage(task);
        });
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        return new PageOutput()
        {
            private final PageReader pageReader = new PageReader(inputSchema);
            private final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private final StandardColumnVisitor standardColumnVisitor = new StandardColumnVisitor(pageReader, pageBuilder);
            private final OutForwardService outForwardService = new OutForwardService(task);
            private final OutForwardEventBuilder eventBuilder = new OutForwardEventBuilder(outputSchema, outForwardService);
            private final ColumnVisitor outForwardVisitor = new StandardColumnVisitor(pageReader, eventBuilder);

            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    outputSchema.visitColumns(outForwardVisitor);
                    eventBuilder.addRecord();

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
