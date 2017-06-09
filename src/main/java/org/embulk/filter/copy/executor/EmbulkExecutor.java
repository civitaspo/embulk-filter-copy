package org.embulk.filter.copy.executor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Joiner;
import com.google.inject.Injector;
import org.embulk.EmbulkEmbed;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.guice.LifeCycleInjector;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Locale;

public abstract class EmbulkExecutor
{
    static final Logger logger = Exec.getLogger(EmbulkExecutor.class);

    public interface ExecutorTask
            extends org.embulk.config.Task
    {
        @Config("\"type\"")
        @ConfigDefault("\"local_thread\"")
        ExecutorType getType();

        enum ExecutorType
        {
            LOCAL_THREAD;

            @JsonValue
            @Override
            public String toString()
            {
                return name().toLowerCase(Locale.ENGLISH);
            }

            @JsonCreator
            public static ExecutorType fromString(String value)
            {
                switch (value) {
                    case "local_thread":
                        return LOCAL_THREAD;
                    default:
                        String values = Joiner.on(", ").join(values()).toLowerCase(Locale.ENGLISH);
                        throw new ConfigException(String.format("Unknown executor type '%s'. Supported executor types are %s", value, values));
                }
            }
        }

    }

    public interface Task
        extends org.embulk.config.Task
    {
        @Config("executor")
        @ConfigDefault("{}")
        ExecutorTask getExecutorTask();
    }

    public static EmbulkExecutor buildExecutor(Task task)
    {
        switch (task.getExecutorTask().getType()) {
            case LOCAL_THREAD:
                return new LocalThreadExecutor(task.getExecutorTask());
            default:
                String values = Joiner.on(", ").join(ExecutorTask.ExecutorType.values()).toLowerCase(Locale.ENGLISH);
                throw new ConfigException(String.format("Unknown executor type '%s'. Supported executor types are %s", task.getExecutorTask().getType(), values));
        }
    }

    final ExecutorTask task;
    final Injector injector;

    EmbulkExecutor(ExecutorTask task)
    {
        this.task = task;
        this.injector = Exec.getInjector();
    }

    public abstract void setup();

    public abstract void executeAsync(ConfigSource config);

    public abstract void waitUntilExecutionFinished();

    public abstract void shutdown();

    EmbulkEmbed newEmbulkEmbed()
    {
        try {
            Constructor<EmbulkEmbed> constructor = EmbulkEmbed.class
                    .getDeclaredConstructor(ConfigSource.class, LifeCycleInjector.class);
            constructor.setAccessible(true);
            return constructor.newInstance(null, injector);
        }
        catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new ConfigException(e);
        }
    }
}
