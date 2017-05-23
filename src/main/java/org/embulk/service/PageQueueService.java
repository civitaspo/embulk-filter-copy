package org.embulk.service;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Queue;

public class PageQueueService
{
    private static final Logger logger = Exec.getLogger(PageQueueService.class);
    private static final Map<String, Queue<Page>> multiTaskQueue = Maps.newConcurrentMap();
    private static final Map<String, QueueStatus> queueStatusMap = Maps.newConcurrentMap();

    private PageQueueService()
    {
    }

    private static Queue<Page> newQueue()
    {
        return Queues.newArrayBlockingQueue(1000);
    }

    public static void openNewTaskQueue(String taskName)
    {
        multiTaskQueue.put(taskName, newQueue());
        queueStatusMap.put(taskName, QueueStatus.OPEN);
    }

    public static void closeTaskQueue(String taskName)
    {
        // TODO wait close
        if (!isEmptyQueue(taskQueue(taskName))) {
            logger.warn("Task: {} has some Pages.", taskName);
        }
        taskQueue(taskName).clear();
        queueStatusMap.put(taskName, QueueStatus.CLOSE);
    }

    public static boolean isTaskQueueClosed(String taskName)
    {
        return queueStatusMap.get(taskName).equals(QueueStatus.CLOSE);
    }

    private static Queue<Page> taskQueue(String taskName)
    {
        Queue<Page> q = multiTaskQueue.get(taskName);
        if (q == null) {
            throw new NullPointerException(String.format("Task: %s is not registered.", taskName));
        }
        return q;
    }

    private static boolean isEmptyQueue(Queue q)
    {
        return q.size() == 0;
    }

    public static boolean enqueue(String taskName, Page page)
    {
        try {
            return taskQueue(taskName).add(page);
        }
        catch (Exception e) {
            logger.warn(e.getMessage(), e);
            return false;
        }
    }

    public static Optional<Page> dequeue(String taskName)
    {
        try {
            return Optional.of(taskQueue(taskName).remove());
        }
        catch (Exception e) {
            logger.warn(e.getMessage(), e);
            return Optional.absent();
        }
    }

    enum QueueStatus
    {
        OPEN, CLOSE
    }
}
