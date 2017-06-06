package org.embulk.filter.copy.service;

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
import org.embulk.filter.copy.util.ElapsedTime;
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
    private final static String THREAD_NAME = "embulk executor service";
    private static final int NUM_THREADS = 1;
    private final static Logger logger = Exec.getLogger(EmbulkExecutorService.class);
    private final Injector injector;
    private final ListeningExecutorService es;
    private ListenableFuture<ExecutionResult> future;

    public EmbulkExecutorService(Injector injector)
    {
        this.injector = injector;
        this.es = MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(
                        NUM_THREADS,
                        r -> new Thread(r, THREAD_NAME)
                ));
    }

    public void executeAsync(ConfigSource config)
    {
        logger.debug("execute with this config: {}", config);
        if (future != null) {
            throw new IllegalStateException("executeAsync is already called.");
        }
        future = es.submit(embulkRun(config));
        Futures.addCallback(future, resultFutureCallback());
    }

    public void shutdown()
    {
        ElapsedTime.info(
                logger,
                "embulk executor service shutdown",
                es::shutdown);
    }

    public void waitExecutionFinished()
    {
        if (future == null) {
            throw new NullPointerException();
        }

        ElapsedTime.debugUntil(() -> future.isDone() || future.isCancelled(),
                logger, "embulk executor", 3000L);
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
