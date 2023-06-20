package dev.starless.mongo.logging;

import org.jetbrains.annotations.NotNull;

import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaLogger implements ILogger {

    private final Logger logger;

    public JavaLogger(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void info(@NotNull String message, Object... objects) {
        logger.info(String.format(message, objects));
    }

    @Override
    public void warn(@NotNull String message, Object... objects) {
        logger.log(Level.WARNING, String.format(message, objects));
    }

    @Override
    public void error(@NotNull String message, Object... objects) {
        logger.log(Level.SEVERE, String.format(message, objects));
    }
}
