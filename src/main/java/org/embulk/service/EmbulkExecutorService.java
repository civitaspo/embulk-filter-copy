package org.embulk.service;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Injector;
import org.embulk.EmbulkEmbed;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.exec.BulkLoader;
import org.embulk.exec.ExecutionResult;
import org.embulk.exec.PartialExecutionException;
import org.embulk.guice.LifeCycleInjector;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecSession;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

public class EmbulkExecutorService
{
    private final static Logger logger = Exec.getLogger(EmbulkExecutorService.class);
    private final EmbulkEmbed embed;
    private final ListeningExecutorService es;
    private final List<Future> q = Lists.newArrayList();

    public EmbulkExecutorService(int numThreads, Injector injector)
    {
        this.embed = newEmbulkEmbed(injector);
        this.es = MoreExecutors.listeningDecorator(newThreadPool(numThreads));
    }

    public void executeAsync(final ConfigSource config)
    {
        logger.info("execute with this config: {}", config);
        ListenableFuture<ExecutionResult> future = es.submit(new Callable<ExecutionResult>()
        {
            @Override
            public ExecutionResult call()
                    throws Exception
            {
                return embed.run(config);
            }
        });
        Futures.addCallback(future, new FutureCallback<ExecutionResult>() {
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
        });

        q.add(future);
    }

    public void shutdown()
    {
        if (!es.isShutdown()) {
            es.shutdown();
        }
    }

    public void waitExecutionFinished()
    {
        while (!areAllExecutionsFinished()) {
            logger.info("all exec are not finished yet.");
            try {
                Thread.sleep(3000L); // 3 seconds
            }
            catch (InterruptedException e) {
                logger.warn("Sleep failed", e);
            }
        }
    }

    private boolean areAllExecutionsFinished()
    {
        for (Future future : q) {
            if (!(future.isDone() || future.isCancelled())) {
                return false;
            }
        }
        return true;
    }

    private ExecutorService newThreadPool(int numThreads)
    {
        return Executors.newFixedThreadPool(numThreads, r -> new Thread(r, "embulk-executor-service"));
    }

    private ConfigSource emptyConfigSource()
    {
        return Exec.newConfigSource();
    }

    private EmbulkEmbed newEmbulkEmbed(Injector injector)
    {
        try {
            Constructor<EmbulkEmbed> constructor = EmbulkEmbed.class
                    .getDeclaredConstructor(ConfigSource.class, LifeCycleInjector.class);
            constructor.setAccessible(true);
            return constructor.newInstance(emptyConfigSource(), injector);
        }
        catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new ConfigException(e);
        }
    }
}
