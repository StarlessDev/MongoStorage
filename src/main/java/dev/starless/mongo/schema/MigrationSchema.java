package dev.starless.mongo.schema;

import dev.starless.mongo.api.annotations.MongoObject;
import dev.starless.mongo.schema.suppliers.ValueSupplier;
import dev.starless.mongo.schema.suppliers.impl.ConstantSupplier;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class MigrationSchema {

    private final String database;
    private final String collection;

    private final Set<Entry> entries;

    public MigrationSchema(Class<?> clazz) {
        MongoObject annotation = clazz.getAnnotation(MongoObject.class);
        Objects.requireNonNull(annotation);

        this.database = annotation.database();
        this.collection = annotation.collection();

        this.entries = new HashSet<>();
    }

    public MigrationSchema entry(String currentName, Object fixedValue) {
        return entry(currentName, new ConstantSupplier(null, fixedValue));
    }

    public MigrationSchema entry(String currentName, String deprecatedKey, Object fixedValue) {
        return entry(currentName, new ConstantSupplier(deprecatedKey, fixedValue));
    }

    public MigrationSchema entry(String currentName, ValueSupplier defaultSupplier) {
        entries.add(new Entry(currentName, defaultSupplier));
        return this;
    }

    public String database() {
        return database;
    }

    public String collection() {
        return collection;
    }

    public Set<Entry> entries() {
        return entries;
    }
}
