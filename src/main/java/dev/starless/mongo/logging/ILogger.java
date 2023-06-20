package dev.starless.mongo.logging;

import org.jetbrains.annotations.NotNull;

public interface ILogger {

    void info(@NotNull String message, Object... objects);

    void warn(@NotNull String message, Object... objects);

    void error(@NotNull String message, Object... objects);
}
