package org.embulk.filter.copy;

import com.google.inject.Injector;
import org.embulk.EmbulkEmbed;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.exec.ExecutionResult;
import org.embulk.guice.LifeCycleInjector;
import org.embulk.service.EmbulkExecutorService;
import org.embulk.service.PageQueueService;
import org.embulk.spi.Buffer;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecSession;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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

        final EmbulkExecutorService embulkExecutorService = new EmbulkExecutorService(2, Exec.getInjector(), Exec.session());

        ConfigSource inputConfig = Exec.newConfigSource();
        inputConfig.set("type", "page");
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
            private final ColumnVisitorImpl visitor = new ColumnVisitorImpl(pageBuilder);

            @Override
            public void add(Page page)
            {
                try {
                    logger.info("enqueue!!!");
                    PageQueueService.enqueue(copyPage(page));
                }
                catch (InterruptedException e) {
                    logger.warn("enqueue failed", e);
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
                PageQueueService.close();
                embulkExecutorService.waitExecutionFinished();
                embulkExecutorService.shutdown();
            }

            class ColumnVisitorImpl
                    implements ColumnVisitor
            {
                private final PageBuilder pageBuilder;

                ColumnVisitorImpl(PageBuilder pageBuilder)
                {
                    this.pageBuilder = pageBuilder;
                }

                @Override
                public void booleanColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                    }
                    else {
                        pageBuilder.setBoolean(column, pageReader.getBoolean(column));
                    }
                }

                @Override
                public void longColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                    }
                    else {
                        pageBuilder.setLong(column, pageReader.getLong(column));
                    }
                }

                @Override
                public void doubleColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                    }
                    else {
                        pageBuilder.setDouble(column, pageReader.getDouble(column));
                    }
                }

                @Override
                public void stringColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                    }
                    else {
                        pageBuilder.setString(column, pageReader.getString(column));
                    }
                }

                @Override
                public void timestampColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                    }
                    else {
                        pageBuilder.setTimestamp(column, pageReader.getTimestamp(column));
                    }
                }

                @Override
                public void jsonColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                    }
                    else {
                        pageBuilder.setJson(column, pageReader.getJson(column));
                    }
                }
            }
        };
    }

    private Page copyPage(Page page)
    {
        byte[] raw = page.buffer().array();
        Buffer newBuf = Buffer.wrap(raw);
        return Page.wrap(newBuf);
    }
}
