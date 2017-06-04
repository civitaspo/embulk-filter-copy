package org.embulk.filter.copy.service;

import influent.EventStream;
import influent.Tag;
import influent.forward.ForwardCallback;
import influent.forward.ForwardServer;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class InForwardService
{
    private final static Logger logger = Exec.getLogger(InForwardService.class);

    public interface InForwardTask
            extends org.embulk.config.Task
    {
        @Config("port")
        @ConfigDefault("24224")
        int getPort();

        @Config("chunk_size_limit")
        @ConfigDefault("null")
        Optional<Long> getChunkSizeLimit();

        @Config("backlog")
        @ConfigDefault("null")
        Optional<Integer> getBacklog();

        @Config("send_buffer_size")
        @ConfigDefault("null")
        Optional<Integer> getSendBufferSize();

        @Config("receive_buffer_size")
        @ConfigDefault("null")
        Optional<Integer> getReceiveBufferSize();

        @Config("keep_alive_enabled")
        @ConfigDefault("null")
        Optional<Boolean> getKeepAliveEnabled();

        @Config("tcp_no_delay_enabled")
        @ConfigDefault("null")
        Optional<Boolean> getTcpNoDelayEnabled();
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

        @Config("shutdown_tag")
        @ConfigDefault("\"shutdown\"")
        String getShutdownTag();

        @Config("message_tag")
        @ConfigDefault("\"message\"")
        String getMessageTag();
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
    private final ExecutorService callbackThread;
    private final AtomicBoolean shouldShutdown = new AtomicBoolean(false);

    private InForwardService(Task task, Consumer<EventStream> eventConsumer)
    {
        this.task = task;
        this.callbackThread = Executors.newFixedThreadPool(
                task.getNumThreads(),
                r -> new Thread(r, task.getThreadName()));
        this.server = buildServer(task.getInForwardTask(), eventConsumer, callbackThread);
    }

    private ForwardServer buildServer(InForwardTask t, Consumer<EventStream> eventConsumer, Executor callbackThread)
    {
        ForwardServer.Builder builder = new ForwardServer.Builder(
                ForwardCallback.ofSyncConsumer(
                        wrapEventConsumer(eventConsumer),
                        callbackThread));

        builder.localAddress(t.getPort());

        if (t.getChunkSizeLimit().isPresent()) {
            builder.chunkSizeLimit(t.getChunkSizeLimit().get());
        }
        if (t.getBacklog().isPresent()) {
            builder.backlog(t.getBacklog().get());
        }
        if (t.getSendBufferSize().isPresent()) {
            builder.sendBufferSize(t.getSendBufferSize().get());
        }
        if (t.getReceiveBufferSize().isPresent()) {
            builder.receiveBufferSize(t.getReceiveBufferSize().get());
        }
        if (t.getKeepAliveEnabled().isPresent()) {
            builder.keepAliveEnabled(t.getKeepAliveEnabled().get());
        }
        if (t.getTcpNoDelayEnabled().isPresent()) {
            builder.tcpNoDelayEnabled(t.getTcpNoDelayEnabled().get());
        }

        return builder.build();
    }

    private Consumer<EventStream> wrapEventConsumer(Consumer<EventStream> eventConsumer)
    {
        return eventStream ->
        {
            if (isShutdownTag(eventStream.getTag())) {
                logger.info("Receive shutdown tag: {}", eventStream.getTag());
                shouldShutdown.set(true);
            }
            else if (isMessageTag(eventStream.getTag())) {
                eventConsumer.accept(eventStream);
            }
            else {
                throw new RuntimeException(String.format("Unknown Tag received: %s", eventStream.getTag().getName()));
            }
        };
    }

    private boolean isShutdownTag(Tag tag)
    {
        return tag.getName().contentEquals(task.getShutdownTag());
    }

    private boolean isMessageTag(Tag tag)
    {
        return tag.getName().contentEquals(task.getMessageTag());
    }

    public void runUntilShouldShutdown()
    {
        long startMillis = System.currentTimeMillis();
        logger.info("in_forward server start");
        server.start();

        while (!shouldShutdown.get()) {
            logger.info("in_forward server is running. (Elapsed: {}ms)", System.currentTimeMillis() - startMillis);

            try {
                Thread.sleep(1000L);
            }
            catch (InterruptedException e) {
                logger.warn(e.getMessage(), e);
            }
        }

        shutdown();
    }

    private void shutdown()
    {
        long startMillis = System.currentTimeMillis();
        logger.info("in_forward server start to shut down");
        logger.info("first, callback is shutting down. (Elapsed: {}ms)", System.currentTimeMillis() - startMillis);
        callbackThread.shutdown();
        logger.info("callback finish to shut down (Elapsed: {}ms)", System.currentTimeMillis() - startMillis);

        CompletableFuture<Void> shutdown = server.shutdown();

        while (!(shutdown.isCancelled() || shutdown.isCompletedExceptionally() || shutdown.isDone())) {
            logger.info("in_forward server is shutting down. (Elapsed: {}ms)", System.currentTimeMillis() - startMillis);
            try {
                Thread.sleep(1000L);
            }
            catch (InterruptedException e) {
                logger.warn(e.getMessage(), e);
            }
        }
        logger.info("in_forward server finish to shut down. (Elapsed: {}ms)", System.currentTimeMillis() - startMillis);
    }
}
