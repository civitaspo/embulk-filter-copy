package org.embulk.service.plugin.copy;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;
import org.komamitsu.fluency.Fluency;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class OutForwardService
{
    public interface OutForwardTask
    {
        @Config("host")
        @ConfigDefault("\"localhost\"")
        String getHost();
        void setHost(String host);

        @Config("port")
        @ConfigDefault("24224")
        int getPort();
        void setPort(int port);
    }

    public interface Task
    {
        @Config("out_forward")
        @ConfigDefault("{}") // TODO
        OutForwardTask getOutForwardTask();

        @Config("tag")
        @ConfigDefault("embulk")
        String getTag();
    }

    private final Task task;
    private final Fluency client;

    public OutForwardService(Task task)
    {
        this.task = task;
        this.client = newFluency(task.getOutForwardTask());
    }

    private Fluency newFluency(OutForwardTask forwardTask)
    {
        try {
            return Fluency.defaultFluency(forwardTask.getHost(), forwardTask.getPort());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void emitMessage(Consumer<Map<String, Object>> consumer)
    {
        Map<String, Object> message = Maps.newHashMap();
        consumer.accept(message);
        try {
            client.emit(task.getTag(), message);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void finish()
    {
        try {
            client.flush();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close()
    {
        try {
            client.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
