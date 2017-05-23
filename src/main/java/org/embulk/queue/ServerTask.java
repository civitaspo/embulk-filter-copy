package org.embulk.queue;

import org.embulk.config.Config;
import org.embulk.config.Task;

public interface ServerTask
    extends Task
{
    @Config("host")
    String getHost();

    @Config("port")
    int getPort();
}
