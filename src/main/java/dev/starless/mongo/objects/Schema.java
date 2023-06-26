package dev.starless.mongo.objects;

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

        Field[] clazzFields = clazz.getFields();
        this.database = annotation.database();
        this.collection = annotation.collection();

        this.clazz = clazz;
        this.entries = new HashSet<>(clazzFields.length);

        for (Field field : clazzFields) {
            entries.add(new Entry(field.getName(), null));
        }
    }

    public Schema entry(String fieldName, Object defaultValue) {
        entries.add(new Entry(fieldName, defaultValue));
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

    public record Entry(String fieldName, Object defaultValue) {
    }
}
