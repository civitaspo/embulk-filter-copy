package org.embulk.filter.copy.util;

import org.slf4j.Logger;

import java.util.function.Supplier;

public class ElapsedTime
{
    private ElapsedTime()
    {
    }

    private final static String TEMPLATE = " ({} ms)";
    private enum LogLevel {TRACE, DEBUG, INFO, WARN, ERROR}
    private enum Status {START, RUNNING, FINISHED}

    private static void log(Logger logger, LogLevel level, String message, long elapsed)
    {
        String msgWithTmpl = message + TEMPLATE;
        switch (level)
        {
            case TRACE:
                logger.trace(msgWithTmpl, elapsed);
                break;
            case DEBUG:
                logger.debug(msgWithTmpl, elapsed);
                break;
            case INFO:
                logger.info(msgWithTmpl, elapsed);
                break;
            case WARN:
                logger.warn(msgWithTmpl, elapsed);
                break;
            case ERROR:
                logger.error(msgWithTmpl, elapsed);
                break;
        }
    }

    private static <T>T run(Logger logger, LogLevel level, String message, Supplier<T> proc)
    {
        long start = System.currentTimeMillis();
        try {
            return proc.get();
        }
        finally {
            log(logger, level, message, System.currentTimeMillis() - start);
        }
    }

    private static void run(Logger logger, LogLevel level, String message, Runnable proc)
    {
        long start = System.currentTimeMillis();
        try {
            proc.run();
        }
        finally {
            log(logger, level, message, System.currentTimeMillis() - start);
        }
    }

    public static <T>T trace(Logger logger, String message, Supplier<T> proc)
    {
        return run(logger, LogLevel.TRACE, message, proc);
    }

    public static <T>T debug(Logger logger, String message, Supplier<T> proc)
    {
        return run(logger, LogLevel.DEBUG, message, proc);
    }

    public static <T>T info(Logger logger, String message, Supplier<T> proc)
    {
        return run(logger, LogLevel.INFO, message, proc);
    }

    public static <T>T warn(Logger logger, String message, Supplier<T> proc)
    {
        return run(logger, LogLevel.WARN, message, proc);
    }

    public static <T>T error(Logger logger, String message, Supplier<T> proc)
    {
        return run(logger, LogLevel.ERROR, message, proc);
    }

    public static void trace(Logger logger, String message, Runnable proc)
    {
        run(logger, LogLevel.TRACE, message, proc);
    }

    public static void debug(Logger logger, String message, Runnable proc)
    {
        run(logger, LogLevel.DEBUG, message, proc);
    }

    public static void info(Logger logger, String message, Runnable proc)
    {
        run(logger, LogLevel.INFO, message, proc);
    }

    public static void warn(Logger logger, String message, Runnable proc)
    {
        run(logger, LogLevel.WARN, message, proc);
    }

    public static void error(Logger logger, String message, Runnable proc)
    {
        run(logger, LogLevel.ERROR, message, proc);
    }

    private static void logUntil(Supplier<Boolean> condition,
            Logger logger, LogLevel level, String message, long sleep)
    {
        long start = System.currentTimeMillis();
        log(logger, level, String.format("(%s) %s", Status.START, message),
                System.currentTimeMillis() - start);
        while (true) {
            if (condition.get()) {
                log(logger, level, String.format("(%s) %s", Status.FINISHED, message),
                        System.currentTimeMillis() - start);
                break;
            }
            try {
                Thread.sleep(sleep);
            }
            catch (InterruptedException e) {
                logger.warn(e.getMessage(), e);
            }

            log(logger, level, String.format("(%s) %s", Status.RUNNING, message),
                    System.currentTimeMillis() - start);
        }
    }

    public static void traceUntil(Supplier<Boolean> condition,
            Logger logger, String message, long sleep)
    {
        logUntil(condition, logger, LogLevel.TRACE, message, sleep);
    }

    public static void debugUntil(Supplier<Boolean> condition,
            Logger logger, String message, long sleep)
    {
        logUntil(condition, logger, LogLevel.DEBUG, message, sleep);
    }

    public static void infoUntil(Supplier<Boolean> condition,
            Logger logger, String message, long sleep)
    {
        logUntil(condition, logger, LogLevel.INFO, message, sleep);
    }

    public static void warnUntil(Supplier<Boolean> condition,
            Logger logger, String message, long sleep)
    {
        logUntil(condition, logger, LogLevel.WARN, message, sleep);
    }

    public static void errorUntil(Supplier<Boolean> condition,
            Logger logger, String message, long sleep)
    {
        logUntil(condition, logger, LogLevel.ERROR, message, sleep);
    }
}
