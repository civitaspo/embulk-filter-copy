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
import org.embulk.filter.copy.forward.InForwardVisitor;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.time.TimestampParser;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class InternalForwardInputPlugin
        implements InputPlugin
{
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
        logger.info("input-copy: cleanup");
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        try (PageBuilder pageBuilder = new PageBuilder(task.getBufferAllocator(), schema, output)) {
            TimestampParser timestampParser = new TimestampParser(
                    task.getJRuby(),
                    task.getDefaultTimestampFormat(),
                    task.getDefaultTimeZone());
            InForwardEventReader eventReader = new InForwardEventReader(schema, timestampParser);
            InForwardVisitor inForwardVisitor = new InForwardVisitor(eventReader, pageBuilder);

            AtomicBoolean isShutdown = new AtomicBoolean(false);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> isShutdown.set(true)));

            InForwardService.builder()
                    .task(task)
                    .forEachEventCallback(
                            event ->
                            {
                                // TODO: here is not thread-safe
                                eventReader.setEvent(event);
                                while (eventReader.nextMessage()) {
                                    schema.visitColumns(inForwardVisitor);
                                    pageBuilder.addRecord();
                                }
                            }
                    )
                    .build()
                    .runUntilShouldShutdown();

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
