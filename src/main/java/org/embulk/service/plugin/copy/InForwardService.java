package org.embulk.service.plugin.copy;

import influent.EventStream;
import influent.forward.ForwardCallback;
import influent.forward.ForwardServer;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;

import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class InForwardService
{
    public interface InForwardTask
            extends org.embulk.config.Task
    {
        @Config("port")
        @ConfigDefault("24224")
        int getPort();
    }

    public interface Task
            extends org.embulk.config.Task
    {
        @Config("in_forward")
        @ConfigDefault("{}")
        InForwardTask getInForwardTask();

        @Config("thread_name")
        @ConfigDefault("\"embulk-input-copy\"")
        String getThreadName();

        @Config("num_threads")
        @ConfigDefault("1")
        int getNumThreads();
    }

    public static class Builder
    {
        private Task task;
        private Consumer<EventStream> eventConsumer;

        public Builder()
        {
        }

        public Builder task(Task task)
        {
            this.task = task;
            return this;
        }

        public Builder forEachEventCallback(Consumer<EventStream> eventConsumer)
        {
            this.eventConsumer = eventConsumer;
            return this;
        }

        public InForwardService build()
        {
            return new InForwardService(task, eventConsumer);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    private final Task task;
    private final ForwardServer server;

    private InForwardService(Task task, Consumer<EventStream> eventConsumer)
    {
        this.task = task;
        this.server = buildServer(eventConsumer);

    }

    private ForwardServer buildServer(Consumer<EventStream> eventConsumer)
    {
        return new ForwardServer.Builder(
                ForwardCallback.ofSyncConsumer(
                        eventConsumer,
                        Executors.newFixedThreadPool(
                                task.getNumThreads(),
                                r -> new Thread(r, task.getThreadName())
                        )
                ))
                .localAddress(task.getInForwardTask().getPort())
                .build();
    }

    public void runUntilCallbackFinished(Runnable r)
    {
        server.start();
        r.run();
        server.shutdown();
    }
}
