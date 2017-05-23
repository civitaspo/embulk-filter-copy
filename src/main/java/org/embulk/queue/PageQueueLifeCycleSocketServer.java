package org.embulk.queue;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.util.concurrent.Executors;

public class PageQueueLifeCycleSocketServer
{
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 0; // Choose un-used port. See. http://moznion.hatenadiary.com/entry/2014/11/29/222221
    private static final int DEFAULT_SOCKET_TIMEOUT = 10 * 1000; // ms
    private static final Logger logger = Exec.getLogger(PageQueueSocketServer.class);

    private final String host;
    private final int port;
    private final String socketServerHost;
    private final int socketServerPort;
    private final ListeningExecutorService es;

    PageQueueLifeCycleSocketServer(
            String host,
            int port,
            String socketServerHost,
            int socketServerPort)
    {
        this.host = host;
        this.port = port;
        this.socketServerHost = socketServerHost;
        this.socketServerPort = socketServerPort;
        this.es = MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(1));
    }
}
