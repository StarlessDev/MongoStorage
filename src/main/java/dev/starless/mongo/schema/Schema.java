package dev.starless.mongo.schema;

import dev.starless.mongo.api.annotations.MongoObject;
import dev.starless.mongo.schema.suppliers.ValueSupplier;
import dev.starless.mongo.schema.suppliers.impl.ConstantSupplier;

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

    public Schema entry(String currentName, Object fixedValue) {
        return entry(currentName, new ConstantSupplier(null, fixedValue));
    }

    public Schema entry(String currentName, String deprecatedKey, Object fixedValue) {
        return entry(currentName, new ConstantSupplier(deprecatedKey, fixedValue));
    }

    public Schema entry(String currentName, ValueSupplier defaultSupplier) {
        entries.add(new Entry(currentName, defaultSupplier));
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
}
