package it.ayyjava.storage.logging;

import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaLogger implements ILogger {

    private final Logger logger;

    public JavaLogger(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void info(String message, Object... objects) {
        logger.info(String.format(message, objects));
    }

    @Override
    public void warn(String message, Object... objects) {
        logger.log(Level.WARNING, String.format(message, objects));
    }

    @Override
    public void error(String message, Object... objects) {
        logger.log(Level.SEVERE, String.format(message, objects));
    }
}
