package org.embulk.filter.copy.forward;

import com.google.common.base.Optional;
import influent.EventStream;
import influent.Tag;
import influent.forward.ForwardCallback;
import influent.forward.ForwardServer;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.filter.copy.util.ElapsedTime;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

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

    // TODO: configure?
    private static final String THREAD_NAME = "InForwardService";
    private static final int NUM_THREADS = 1;

    private final Task task;
    private final ForwardServer server;
    private final ExecutorService callbackThread;
    private final AtomicBoolean shouldShutdown = new AtomicBoolean(false);

    private InForwardService(Task task, Consumer<EventStream> eventConsumer)
    {
        this.task = task;
        this.callbackThread = Executors.newFixedThreadPool(
                NUM_THREADS,
                r -> new Thread(r, THREAD_NAME));
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
        server.start();
        ElapsedTime.debugUntil(shouldShutdown::get, logger, "in_forward server", 1000L);
        shutdown();
    }

    private void shutdown()
    {
        ElapsedTime.debug(logger, "shutdown in_forward callback", callbackThread::shutdown);
        CompletableFuture<Void> shutdown = server.shutdown();
        ElapsedTime.debugUntil(() -> shutdown.isCancelled() || shutdown.isCompletedExceptionally() || shutdown.isDone(),
                logger, "shutdown in_forward server", 1000L);
    }
}
