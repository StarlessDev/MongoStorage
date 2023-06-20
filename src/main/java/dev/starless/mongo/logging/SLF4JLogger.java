package dev.starless.mongo.logging;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

public class SLF4JLogger implements ILogger {

    private final Logger logger;

    public SLF4JLogger(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void info(@NotNull String message, Object... objects) {
        logger.info(String.format(message, objects));
    }

    @Override
    public void warn(@NotNull String message, Object... objects) {
        logger.warn(String.format(message, objects));
    }

    @Override
    public void error(@NotNull String message, Object... objects) {
        logger.error(String.format(message, objects));
    }
}
