package org.embulk.queue;

import com.google.common.base.Optional;
import com.google.common.collect.Queues;
import com.google.common.io.ByteStreams;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class PageQueueSocketServer
{
    private static final int DEFAULT_SOCKET_TIMEOUT = 10 * 1000; // 10 sec
    private static final Logger logger = Exec.getLogger(PageQueueSocketServer.class);

    private final String host;
    private final int port;
    private final Queue<Page> queue;
    private final ListeningExecutorService es;

    private AtomicBoolean serverRunning = new AtomicBoolean(false);
    private AtomicBoolean threadFinalized = new AtomicBoolean(false);
    private ServerSocket serverSocket;

    public PageQueueSocketServer(String host, int port, Queue<Page> queue)
    {
        this.host = host;
        this.port = port;
        this.queue = queue;
        this.es = MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(1));
    }

    public PageQueueSocketServer(String host, int port)
    {
        this(host, port, Queues.<Page>newArrayBlockingQueue(1)); // TODO: queue size
    }

    public String getHost()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }

    public InetAddress getInetAddress()
    {
        if (getHost().contentEquals("localhost")) {
            try {
                return InetAddress.getLocalHost();
            }
            catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
        return InetAddresses.forString(getHost());
    }

    public boolean isRunning()
    {
        return serverRunning.get();
    }

    public void start()
    {
        configure();
        serverRunning.set(true);
        runServer();
    }

    public void stop()
    {
        serverRunning.set(false);
        while (!threadFinalized.get()) {
            logger.info("Stopping ...");
            try {
                Thread.sleep(5 * 1000); // 5 sec
            }
            catch (InterruptedException e) {
                logger.warn(e.getMessage(), e);
            }
        }
        es.shutdown();
    }

    public Optional<Page> dequeuePage()
    {
        try {
            return Optional.of(queue.remove());
        }
        catch (Exception e) {
            logger.warn(e.getMessage(), e);
            return Optional.absent();
        }
    }

    private void configure()
    {
        if (threadFinalized.get()) {
            throw new IllegalStateException("Server thread is already finalized.");
        }

        try {
            // use default value if backlog < 1
            this.serverSocket = new ServerSocket(getPort(), 0, getInetAddress());
            serverSocket.setSoTimeout(DEFAULT_SOCKET_TIMEOUT); // TODO: configurable?
        }
        catch (IOException e) {
            throw new ConfigException(e);
        }
    }

    private void runServer()
    {
        ListenableFuture<TaskReport> future = es.submit(controlTask());
        Futures.addCallback(future, finalizeTask(), es);
    }

    private Callable<TaskReport> controlTask()
    {
        return new Callable<TaskReport>() {
            @Override
            public TaskReport call()
                    throws Exception
            {
                long numSuccess = 0L;
                long numFailure = 0L;

                while (serverRunning.get()) {
                    try {
                        Page page = acceptPage();
                        enqueuePage(page);
                        logger.info("queue: {}", queue);
                    }
                    catch (IOException e) {
                        numFailure++;
                        logger.warn("Accept Failed.", e);
                        continue;
                    }
                    catch (Exception e) {
                        numFailure++;
                        logger.error("Unknown Error.", e);
                        continue;
                    }
                    numSuccess++;
                }
                return Exec.newTaskReport()
                        .set("num_success", numSuccess)
                        .set("num_failure", numFailure);
            }
        };
    }

    private FutureCallback<TaskReport> finalizeTask()
    {
        return new FutureCallback<TaskReport>() {
            @Override
            public void onSuccess(@Nullable TaskReport result)
            {
                threadFinalized.set(true);
                logger.info("Server Report: {}", result);
            }

            @Override
            public void onFailure(Throwable t)
            {
                threadFinalized.set(true);
                throw new RuntimeException(t);
            }
        };
    }

    private Page acceptPage()
            throws IOException
    {
        try (Socket socket = serverSocket.accept()) {
            if (socket.isConnected()) {
                logger.info("server connected client");
            }
            else {
                logger.warn("server connect failed.");
            }

            try (InputStream in = socket.getInputStream()) {
                byte[] bytes = ByteStreams.toByteArray(in);
                Buffer buffer = Buffer.copyOf(bytes);
                return Page.wrap(buffer);
            }
        }
    }

    private void enqueuePage(Page page)
    {
        try {
//            for (String s : page.getStringReferences()) {
//                logger.warn("in:page:{}", s);
//            }
            queue.add(page);
        }
        catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }
}
