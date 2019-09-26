package org.embulk.filter.copy;

import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static com.google.common.base.Optional.absent;
import static org.embulk.filter.copy.CopyFilterPlugin.EmbulkConfig;
import static org.embulk.filter.copy.CopyFilterPlugin.PluginTask;
import static org.embulk.filter.copy.forward.InForwardService.InForwardTask;
import static org.embulk.filter.copy.forward.OutForwardService.OutForwardTask;
import static org.junit.Assert.assertEquals;

public class TestCopyFilterPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private Schema schema;
    private CopyFilterPlugin plugin;

    private static Schema schema(Object... nameAndTypes)
    {
        Schema.Builder builder = Schema.builder();
        for (int i = 0; i < nameAndTypes.length; i += 2) {
            builder.add((String) nameAndTypes[i], (Type) nameAndTypes[i + 1]);
        }
        return builder.build();
    }

    private static ConfigSource fromYaml(String yaml)
    {
        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        return loader.fromYamlString(yaml);
    }

    @Before
    public void createResources()
    {
        schema = schema("_c0", Types.STRING, "_c1", Types.STRING); // default schema
    }

    @Test
    public void testPluginTaskDefault()
    {
        String yaml = String.join("\n",
                "type: copy",
                "config:",
                "  out:",
                "    type: stdout"
        );

        ConfigSource config = fromYaml(yaml);
        PluginTask task = config.loadConfig(PluginTask.class);

        // EmbulkConfig
        EmbulkConfig embulkConfig = task.getConfig();
        assertEquals(Exec.newConfigSource(), embulkConfig.getExecConfig());
        assertEquals(Lists.<ConfigSource>newArrayList(), embulkConfig.getFilterConfig());

        // ForwardBaseTask
        assertEquals("message", task.getMessageTag());
        assertEquals("shutdown", task.getShutdownTag());

        // InForwardService
        InForwardTask inForward = task.getInForwardTask();
        assertEquals(24224, inForward.getPort());
        assertEquals(absent(), inForward.getChunkSizeLimit());
        assertEquals(absent(), inForward.getBacklog());
        assertEquals(absent(), inForward.getSendBufferSize());
        assertEquals(absent(), inForward.getReceiveBufferSize());
        assertEquals(absent(), inForward.getKeepAliveEnabled());
        assertEquals(absent(), inForward.getTcpNoDelayEnabled());

        // OutForwardService
        OutForwardTask outForward = task.getOutForwardTask();
        assertEquals("localhost", outForward.getHost());
        assertEquals(24224, outForward.getPort());
        assertEquals(absent(), outForward.getMaxBufferSize());
        assertEquals(absent(), outForward.getBufferChunkInitialSize());
        assertEquals(absent(), outForward.getBufferChunkRetentionSize());
        assertEquals(absent(), outForward.getFlushIntervalMillis());
        assertEquals(absent(), outForward.getSenderMaxRetryCount());
        assertEquals(absent(), outForward.getAckResponseMode());
        assertEquals(absent(), outForward.getFileBackupDir());
        assertEquals(absent(), outForward.getWaitUntilBufferFlushed());
        assertEquals(absent(), outForward.getWaitUntilFlusherTerminated());
    }

}
