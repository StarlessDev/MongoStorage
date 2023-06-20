package dev.starless.mongo.objects;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class Schema<T> {

    private final String database;
    private final String collection;

    private final Class<T> clazz;
    private final Set<Entry> entries;

    public Schema(String database, String collection, Class<T> clazz) {
        this.database = database;
        this.collection = collection;

        this.clazz = clazz;
        this.entries = new HashSet<>(clazz.getFields().length);
    }

    public Schema<T> entry(String fieldName, Object defaultValue) {
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
