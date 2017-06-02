package org.embulk.input.copy;

import com.google.common.collect.ImmutableMap;
import influent.forward.ForwardCallback;
import influent.forward.ForwardServer;
import org.embulk.config.Config;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class CopyInputPlugin
        implements InputPlugin
{
    private final static Logger logger = Exec.getLogger(CopyInputPlugin.class);

    public interface PluginTask
            extends Task, TimestampParser.Task
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
        TimestampParser timestampParser = new TimestampParser(
                task.getJRuby(),
                task.getDefaultTimestampFormat(),
                task.getDefaultTimeZone());
        JsonParser jsonParser = new JsonParser();


        ImmutableMap.Builder<String, Column> builder = ImmutableMap.builder();
        schema.getColumns().forEach(cc -> builder.put(cc.getName(), cc));
        Map<String, Column> cMap = builder.build();

        try (PageBuilder pageBuilder = new PageBuilder(task.getBufferAllocator(), schema, output)) {
            logger.info("embulk-input-copy: start dequeue");
            // lifecycle がいる！！

            ForwardServer server = new ForwardServer.Builder(
                    ForwardCallback.ofSyncConsumer(eventStream -> eventStream.getEntries().forEach(eventEntry -> {
                        eventEntry.getRecord().entrySet().forEach(kv -> {

                            // TODO:ColumnVisitor
                            try {
                                Column c = cMap.get(kv.getKey().asRawValue().asString());

                                Value v = kv.getValue();

                                if (Types.BOOLEAN.equals(c.getType())) {
                                    if (v.isNilValue()) {
                                        pageBuilder.setNull(c);
                                    }
                                    else {
                                        pageBuilder.setBoolean(c, v.asBooleanValue().getBoolean());
                                    }
                                }
                                else if (Types.STRING.equals(c.getType())) {
                                    if (v.isNilValue()) {
                                        pageBuilder.setNull(c);
                                    }
                                    else {
                                        pageBuilder.setString(c, v.asStringValue().asString());
                                    }
                                }
                                else if (Types.LONG.equals(c.getType())) {
                                    if (v.isNilValue()) {
                                        pageBuilder.setNull(c);
                                    }
                                    else {
                                        pageBuilder.setLong(c, v.asIntegerValue().asLong());
                                    }
                                }
                                else if (Types.DOUBLE.equals(c.getType())) {
                                    if (v.isNilValue()) {
                                        pageBuilder.setNull(c);
                                    }
                                    else {
                                        pageBuilder.setDouble(c, v.asFloatValue().toDouble());
                                    }
                                }
                                else if (Types.TIMESTAMP.equals(c.getType())) {
                                    if (v.isNilValue()) {
                                        pageBuilder.setNull(c);
                                    }
                                    else {
                                        pageBuilder.setTimestamp(c, timestampParser.parse(v.asStringValue().asString()));
                                    }
                                }
                                else if (Types.JSON.equals(c.getType())) {
                                    if (v.isNilValue()) {
                                        pageBuilder.setNull(c);
                                    }
                                    else {
                                        pageBuilder.setJson(c, jsonParser.parse(v.asStringValue().toString()));
                                    }
                                }
                                else {
                                    throw new DataException("unknown data type.");
                                }

                            }
                            catch (Exception e) {
                                logger.warn(e.getMessage(), e);
                            }
                        });
                        logger.info("add a record!!!");
                        pageBuilder.addRecord();
                    }), Executors.newFixedThreadPool(1, r -> new Thread(r, "embulk-input-copy"))))
                    .localAddress(24224)
                    .build();

            server.start();

            AtomicBoolean isShutdown = new AtomicBoolean(false);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> isShutdown.set(true)));

            while (!isShutdown.get()) {
                logger.info("embulk-input-copy: running yet");
                try {
                    Thread.sleep(1000L);
                }
                catch (InterruptedException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
            logger.info("embulk-input-copy: input finished!");

            server.shutdown();
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
