package org.embulk.input.page;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.plugin.common.DefaultColumnVisitor;
import org.embulk.service.PageQueueService;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.slf4j.Logger;

import java.util.List;

public class PageInputPlugin
        implements InputPlugin
{
    private final static Logger logger = Exec.getLogger(PageInputPlugin.class);

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
            logger.info("embulk-input-page: start dequeue");
            // lifecycle がいる！！
            while (!PageQueueService.isTaskQueueClosed(task.getQueueName())) {
                logger.info("embulk-input-page: server is running");

                logger.info("try to dequeue");
                Optional<Page> page = PageQueueService.dequeue(task.getQueueName());

                if (page.isPresent()) {
                    logger.info("embulk-input-page: get page");
                    pageReader.setPage(page.get());
                    while (pageReader.nextRecord()) {
                        logger.info("embulk-input-page: add record!!!!");
                        pageReader.getSchema().visitColumns(visitor);
                        pageBuilder.addRecord();
                    }
                }
                else {
                    try {
                        Thread.sleep(5 * 1000);
                    }
                    catch (InterruptedException e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
            }
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

    public interface PluginTask
            extends Task
    {
        @Config("queue_name")
        String getQueueName();

        @Config("columns")
        SchemaConfig getColumns();

        @ConfigInject
        BufferAllocator getBufferAllocator();
    }
}
