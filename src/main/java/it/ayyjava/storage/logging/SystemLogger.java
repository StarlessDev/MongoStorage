package it.ayyjava.storage.logging;

import org.jetbrains.annotations.NotNull;

public class SystemLogger implements ILogger {

    @Override
    public void info(@NotNull String message, Object... objects) {
        System.out.printf(message + "%n", objects);
    }

    @Override
    public void warn(@NotNull String message, Object... objects) {
        System.out.printf(message + "%n", objects);
    }

    @Override
    public void error(@NotNull String message, Object... objects) {
        System.err.printf(message + "%n", objects);
    }
}
