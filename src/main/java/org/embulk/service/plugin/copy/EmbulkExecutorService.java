package org.embulk.service.plugin.copy;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Injector;
import org.embulk.EmbulkEmbed;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.exec.ExecutionResult;
import org.embulk.guice.LifeCycleInjector;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

public class EmbulkExecutorService
{
    private final static Logger logger = Exec.getLogger(EmbulkExecutorService.class);
    private final Injector injector;
    private final ListeningExecutorService es;
    private ListenableFuture<ExecutionResult> future;

    public EmbulkExecutorService(Injector injector)
    {
        this.injector = injector;
        this.es = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1, r -> new Thread(r, "embulk-executor-service")));
    }

    public void executeAsync(final ConfigSource config)
    {
        logger.info("execute with this config: {}", config);
        future = es.submit(embulkRun(config));
        Futures.addCallback(future, resultFutureCallback());
    }

    public void shutdown()
    {
        logger.info("embulk embed shutdown start");
        if (!es.isShutdown()) {
            logger.info("embulk embed shutdown...");
            es.shutdown();
        }
        logger.info("embulk embed shutdown finished");
    }

    public void waitExecutionFinished()
    {
        while (!(future.isDone() || future.isCancelled())) {
            logger.info("all exec are not finished yet.");
            try {
                Thread.sleep(3000L); // 3 seconds
            }
            catch (InterruptedException e) {
                logger.warn("Sleep failed", e);
            }
        }
    }

    private Callable<ExecutionResult> embulkRun(ConfigSource config)
    {
        return () -> newEmbulkEmbed(injector).run(config);
    }

    private EmbulkEmbed newEmbulkEmbed(Injector injector)
    {
        try {
            Constructor<EmbulkEmbed> constructor = EmbulkEmbed.class
                    .getDeclaredConstructor(ConfigSource.class, LifeCycleInjector.class);
            constructor.setAccessible(true);
            return constructor.newInstance(Exec.newConfigSource(), injector);
        }
        catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new ConfigException(e);
        }
    }

    private FutureCallback<ExecutionResult> resultFutureCallback()
    {
        return new FutureCallback<ExecutionResult>()
        {
            @Override
            public void onSuccess(@Nullable ExecutionResult result)
            {
                for (Throwable throwable : result.getIgnoredExceptions()) {
                    logger.warn("Ignored error ", throwable);
                }
                logger.info("Config diff: {}", result.getConfigDiff());
                logger.debug("ExecutionResult: {}", result);
            }

            @Override
            public void onFailure(Throwable t)
            {
                throw new RuntimeException(t);
            }
        };
    }
}
