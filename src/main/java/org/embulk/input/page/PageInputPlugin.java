package org.embulk.input.page;

import java.util.List;

import com.google.common.base.Optional;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.service.PageQueueService;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
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

public class PageInputPlugin
        implements InputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("columns")
        SchemaConfig getColumns();

        @ConfigInject
        BufferAllocator getBufferAllocator();
    }

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
        // イテレートして待ち受けて hasNext が false になったら処理終了する
        PluginTask task = taskSource.loadTask(PluginTask.class);

        try (
                final PageBuilder pageBuilder = new PageBuilder(task.getBufferAllocator(), schema, output);
                final PageReader pageReader = new PageReader(schema)
        ) {
            ColumnVisitor visitor = new ColumnVisitor()
            {
                @Override
                public void booleanColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                        return;
                    }
                    pageBuilder.setBoolean(column, pageReader.getBoolean(column));
                }

                @Override
                public void longColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                        return;
                    }
                    pageBuilder.setLong(column, pageReader.getLong(column));
                }

                @Override
                public void doubleColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                        return;
                    }
                    pageBuilder.setDouble(column, pageReader.getDouble(column));

                }

                @Override
                public void stringColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                        return;
                    }
                    pageBuilder.setString(column, pageReader.getString(column));
                }

                @Override
                public void timestampColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                        return;
                    }
                    pageBuilder.setTimestamp(column, pageReader.getTimestamp(column));
                }

                @Override
                public void jsonColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                        return;
                    }
                    pageBuilder.setJson(column, pageReader.getJson(column));
                }
            };


            logger.info("embulk-input-page: start dequeue");
            while (!PageQueueService.isClosed()) {
                logger.info("embulk-input-page: queue is not closed");
                Optional<Page> page;
                try {
                    logger.info("try to dequeue");
                    page = PageQueueService.dequeue();
                }
                catch (InterruptedException e) {
                    logger.warn("dequeue failed", e);
                    continue;
                }
                if (page.isPresent()) {
                    logger.info("embulk-input-page: get page");
                    pageReader.setPage(page.get());
                    while (pageReader.nextRecord()) {
                        pageReader.getSchema().visitColumns(visitor);
                        pageBuilder.addRecord();
                    }
                }
            }
        }

        return Exec.newTaskReport(); // TODO
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }
}
