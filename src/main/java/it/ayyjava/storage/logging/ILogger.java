package it.ayyjava.storage.logging;

public interface ILogger {

    void info(String message, Object... objects);

    void warn(String message, Object... objects);

    void error(String message, Object... objects);
}
