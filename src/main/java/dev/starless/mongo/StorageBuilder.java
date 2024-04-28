package dev.starless.mongo;

import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import dev.starless.mongo.adapters.DurationAdapter;
import dev.starless.mongo.adapters.InstantAdapter;
import dev.starless.mongo.adapters.OffsetDateTimeAdapter;
import dev.starless.mongo.api.MongoStorage;
import dev.starless.mongo.logging.ILogger;
import dev.starless.mongo.logging.JavaLogger;
import dev.starless.mongo.logging.SLF4JLogger;
import dev.starless.mongo.schema.MigrationSchema;

import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class StorageBuilder {

    private final String url;
    private final GsonBuilder gson;
    private final List<MigrationSchema> schemas;
    private ILogger logger;

    private StorageBuilder(String url) {
        this.url = url;
        this.gson = new GsonBuilder()
                .setPrettyPrinting()
                .serializeNulls()
                // Register some useful TypeAdapters out of the box
                .registerTypeAdapter(Duration.class, new DurationAdapter())
                .registerTypeAdapter(Instant.class, new InstantAdapter())
                .registerTypeAdapter(OffsetDateTime.class, new OffsetDateTimeAdapter());
        this.schemas = new ArrayList<>();
        this.logger = new JavaLogger(Logger.getLogger("MongoStorage"));
    }

    /**
     * Initializes a builder.
     *
     * @param url MongoDB connection string
     * @return new {@link StorageBuilder} instance
     */
    public static StorageBuilder create(String url) {
        return new StorageBuilder(url);
    }

    /**
     * Register a {@link com.google.gson.TypeAdapter} implementation to
     * (de)serialize custom classes.
     *
     * @param type        Type of the class
     * @param typeAdapter This object must implement at least one of the {@link TypeAdapter}, {@link com.google.gson.InstanceCreator},
     *                    {@link com.google.gson.JsonSerializer}, and a {@link com.google.gson.JsonDeserializer} interfaces.
     * @return this {@link StorageBuilder} instance
     */
    public StorageBuilder typeAdapter(Type type, Object typeAdapter) {
        gson.registerTypeAdapter(type, typeAdapter);
        return this;
    }

    /**
     * Register new instructions to migrate objects from old
     * to new schemas, for example when a variable gets renamed.
     *
     * @param schema {@link MigrationSchema} object
     * @return this {@link StorageBuilder} instance
     */
    public StorageBuilder migrationSchema(MigrationSchema schema) {
        schemas.add(schema);
        return this;
    }

    /**
     * Force the library to use a certain logger
     * instead of the default {@link Logger}.
     *
     * @param obj Logger instance to use
     * @return this {@link StorageBuilder} instance
     */
    public StorageBuilder logger(Object obj) {
        if (obj instanceof java.util.logging.Logger loggerInstance) {
            this.logger = new JavaLogger(loggerInstance);
        } else if (obj instanceof org.slf4j.Logger loggerInstance) {
            this.logger = new SLF4JLogger(loggerInstance);
        } else {
            throw new IllegalArgumentException("This type of logger is not supported!");
        }
        return this;
    }

    /**
     * Sometimes MongoDB logging can be annoying, so
     * here's an option to decide what level of logging you desire.
     *
     * @param level {@link Level} for the MongoDB logger
     * @return this {@link StorageBuilder} instance
     */
    public StorageBuilder mongoLoggerLevel(Level level) {
         Logger.getLogger("org.mongodb.driver").setLevel(level);
         return this;
    }

    /**
     * @return A new {@link dev.starless.mongo.api.MongoStorage} implementation
     */
    public MongoStorage build() {
        return new StorageImpl(url, logger, gson.create(), schemas);
    }
}
