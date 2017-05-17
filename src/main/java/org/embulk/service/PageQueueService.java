package org.embulk.service;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class PageQueueService
{
    private static final Logger logger = Exec.getLogger(PageQueueService.class);
    private static final List<Page> q = Collections.synchronizedList(Lists.<Page>newArrayList());
    private static boolean isClosed = false;

    public static synchronized boolean isClosed()
    {
        return isClosed;
    }

    public static synchronized void enqueue(Page page)
            throws InterruptedException
    {
        logger.info("enqueue: {}", q);
        if (isClosed()) {
            throw new IllegalStateException("Queue is already closed.");
        }
        q.add(page);
        logger.info("enqueued: {}", q);
    }

    public static synchronized Optional<Page> dequeue()
            throws InterruptedException
    {
        logger.info("{}", q);
        if (isClosed()) {
            logger.warn("PageQueueService: already closed");
            return Optional.absent();
        }
        if (q.isEmpty()) {
            Thread.sleep(10000L);
            return Optional.absent();
        }

        Iterator<Page> iterator = q.iterator();
        Page next = iterator.next();
        iterator.remove();

        Optional<Page> nullable = Optional.fromNullable(next);
        if (nullable.isPresent()) {
            logger.info("PageQueueService: not null page!");
        }
        else {
            logger.info("PageQueueService: null page!");
        }
        return nullable;
    }

    public static synchronized void close()
    {
        logger.info("PageQueueService: close!");
        isClosed = true;
    }
}
