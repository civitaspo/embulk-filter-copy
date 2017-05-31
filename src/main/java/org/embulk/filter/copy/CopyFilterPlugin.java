package org.embulk.filter.copy;

import com.google.common.collect.Maps;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.plugin.common.DefaultColumnVisitor;
import org.embulk.service.EmbulkExecutorService;
import org.embulk.service.PageCopyService;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.komamitsu.fluency.Fluency;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
            extends Task
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
        inputConfig.set("type", "influent");
        inputConfig.set("columns", inputSchema);

        ConfigSource anotherConfig = Exec.newConfigSource();
        anotherConfig.set("in", inputConfig);
        anotherConfig.set("filters", task.getConfig().getFilterConfig());
        anotherConfig.set("out", task.getConfig().getOutputConfig());

        final EmbulkExecutorService embulkExecutorService = new EmbulkExecutorService(1, Exec.getInjector());
        embulkExecutorService.executeAsync(anotherConfig);

        Schema outputSchema = inputSchema;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            embulkExecutorService.waitExecutionFinished();
            embulkExecutorService.shutdown();
        }));

        control.run(task.dump(), outputSchema);
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
            private final ColumnVisitor visitor = new DefaultColumnVisitor(pageReader, pageBuilder);
            private final Fluency fluency = getFluency();

            @Override
            public void add(Page page)
            {
                logger.info("enqueue!!!");
                Page newPage = PageCopyService.copy(page);
                pageReader.setPage(newPage);
                while (pageReader.nextRecord()) {
                    Map<String, Object> event = Maps.newHashMap();
                    outputSchema.visitColumns(new ColumnVisitor() {
                        @Override
                        public void booleanColumn(Column column)
                        {
                            unlessNull(column, () -> event.put(column.getName(), pageReader.getBoolean(column)));
                        }

                        @Override
                        public void longColumn(Column column)
                        {
                            unlessNull(column, () -> event.put(column.getName(), pageReader.getLong(column)));
                        }

                        @Override
                        public void doubleColumn(Column column)
                        {
                            unlessNull(column, () -> event.put(column.getName(), pageReader.getDouble(column)));
                        }

                        @Override
                        public void stringColumn(Column column)
                        {
                            unlessNull(column, () -> event.put(column.getName(), pageReader.getString(column)));
                        }

                        @Override
                        public void timestampColumn(Column column)
                        {
                            unlessNull(column, () -> event.put(column.getName(), pageReader.getTimestamp(column)));
                        }

                        @Override
                        public void jsonColumn(Column column)
                        {
                            unlessNull(column, () -> event.put(column.getName(), pageReader.getJson(column)));
                        }

                        private void unlessNull(Column column, Runnable runnable)
                        {
                            if (pageReader.isNull(column)) {
                                event.put(column.getName(), Optional.empty());
                                return;
                            }
                            runnable.run();
                        }
                    });
                    try {
                        fluency.emit("embulk", event);
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
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
                try {
                    fluency.flush();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                pageBuilder.finish();
            }

            @Override
            public void close()
            {
                try {
                    fluency.close();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                pageBuilder.close();
            }

            private Fluency getFluency()
            {
                try {
                    return Fluency.defaultFluency();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
