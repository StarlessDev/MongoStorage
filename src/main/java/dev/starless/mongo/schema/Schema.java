package dev.starless.mongo.schema;

import dev.starless.mongo.annotations.MongoObject;

import java.lang.reflect.Field;
import java.util.*;

public class Schema {

    private final String database;
    private final String collection;

    private final Class<?> clazz;
    private final Set<Entry> entries;

    public Schema(Class<?> clazz) {
        MongoObject annotation = clazz.getAnnotation(MongoObject.class);
        Objects.requireNonNull(annotation);

        this.database = annotation.database();
        this.collection = annotation.collection();

        this.clazz = clazz;
        this.entries = new HashSet<>();
    }

    public Schema entry(String currentName, Object defaultValue) {
        return entry(currentName, null, document -> defaultValue);
    }

    public Schema entry(String currentName, String legacyName, Object defaultValue) {
        return entry(currentName, legacyName, document -> defaultValue);
    }

    public Schema entry(String currentName, ValueSupplier defaultSupplier) {
        return entry(currentName, null, defaultSupplier);
    }

    public Schema entry(String currentName, String legacyName, ValueSupplier defaultSupplier) {
        entries.add(new Entry(currentName, legacyName, defaultSupplier));
        return this;
    }

    // Controlla se tutti i fields della classe
    // hanno una corrispettiva entry nello schema
    public boolean validate() {
        List<String> entriesFieldNames = entries.stream().map(Entry::fieldName).toList();
        return Arrays.stream(clazz.getFields())
                .map(Field::getName)
                .allMatch(entriesFieldNames::contains);
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

    public record Entry(String fieldName, String legacyName, ValueSupplier defaultSupplier) {

        public boolean hasLegacyName() {
            return legacyName != null;
        }
    }
}
