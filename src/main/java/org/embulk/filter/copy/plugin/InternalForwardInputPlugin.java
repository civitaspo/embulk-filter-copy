package org.embulk.filter.copy.plugin;

import org.embulk.config.Config;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.filter.copy.forward.InForwardEventReader;
import org.embulk.filter.copy.forward.InForwardService;
import org.embulk.filter.copy.spi.PageBuilder;
import org.embulk.filter.copy.spi.StandardColumnVisitor;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.time.TimestampParser;
import org.slf4j.Logger;

import java.util.List;

public class InternalForwardInputPlugin
        implements InputPlugin
{
    public final static String PLUGIN_NAME = "internal_forward";
    private final static Logger logger = Exec.getLogger(InternalForwardInputPlugin.class);

    public interface PluginTask
            extends Task, TimestampParser.Task, InForwardService.Task
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

        try (PageBuilder pageBuilder = new PageBuilder(task.getBufferAllocator(), schema, output)) {
            InForwardEventReader eventReader = new InForwardEventReader(schema);
            ColumnVisitor inForwardVisitor = new StandardColumnVisitor(eventReader, pageBuilder);

            InForwardService inForwardService = InForwardService.builder()
                    .task(task)
                    .forEachEventCallback(
                            event ->
                            {
                                // TODO: here is not thread-safe
                                eventReader.setEvent(event);
                                while (eventReader.nextRecord()) {
                                    schema.visitColumns(inForwardVisitor);
                                    pageBuilder.addRecord();
                                }
                            }
                    )
                    .build();

            inForwardService.startAsync().awaitTerminated();

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
