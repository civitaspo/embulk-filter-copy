package org.embulk.filter.copy.forward;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

public interface ForwardBaseTask
    extends Task
{
    @Config("shutdown_tag")
    @ConfigDefault("\"shutdown\"")
    String getShutdownTag();

    @Config("message_tag")
    @ConfigDefault("\"message\"")
    String getMessageTag();
}
