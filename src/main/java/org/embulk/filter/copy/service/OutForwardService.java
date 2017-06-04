package org.embulk.filter.copy.service;

import com.google.common.collect.Maps;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.spi.Exec;
import org.komamitsu.fluency.Fluency;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;

public class OutForwardService
{
    private final static Logger logger = Exec.getLogger(OutForwardService.class);

    public interface OutForwardTask
            extends org.embulk.config.Task
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
            extends org.embulk.config.Task
    {
        @Config("out_forward")
        @ConfigDefault("{}") // TODO
        OutForwardTask getOutForwardTask();

        @Config("message_tag")
        @ConfigDefault("\"message\"")
        String getMessageTag();

        @Config("shutdown_tag")
        @ConfigDefault("\"shutdown\"")
        String getShutdownTag();
    }

    public static void sendShutdownMessage(Task task)
    {
        logger.info("out_forward: send shutdown message.");
        OutForwardService outForward = new OutForwardService(task);
        outForward.emit(task.getShutdownTag(), Maps.newHashMap());
        outForward.finish();
        outForward.close();
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

    public void emit(String tag, Map<String, Object> message)
    {
        try {
            client.emit(tag, message);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void emit(Map<String, Object> message)
    {
        emit(task.getMessageTag(), message);
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
