package org.embulk.queue;

import com.google.common.net.InetAddresses;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class PageEnqueueClient
{
    private static final Logger logger = Exec.getLogger(PageEnqueueClient.class);
    private final InetAddress host;
    private final int port;

    public PageEnqueueClient(String host, int port)
    {
        this.host = convertToInetAddress(host);
        this.port = port;
    }

    public InetAddress convertToInetAddress(String host)
    {
        if (host.contentEquals("localhost")) {
            try {
                return InetAddress.getLocalHost();
            }
            catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
        return InetAddresses.forString(host);
    }

    public boolean enqueue(Page page)
    {
        try (Socket socket = new Socket(host, port)) {
            if (socket.isConnected()) {
                logger.info("client connected server");
            }
            else {
                logger.warn("client connect failed.");
            }

            try (OutputStream outputStream = socket.getOutputStream()) {
                logger.info("enqueue to sock: {}", page);
                outputStream.write(page.buffer().array());
                logger.info("enqueue finish {}", page);
            }
        }
        catch (IOException e) {
            logger.warn("Failed to send.", e);
            return false;
        }
        return true;
    }
}
