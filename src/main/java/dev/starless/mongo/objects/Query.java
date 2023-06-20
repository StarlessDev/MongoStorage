package dev.starless.mongo.objects;

import java.util.HashMap;
import java.util.Map;

public class Query {

    private final Map<String, String> values;

    protected Query() {
        values = new HashMap<>();
    }

    public String get(String key) {
        return values.getOrDefault(key, null);
    }

    public Map<String, String> getValues() {
        return values;
    }
}
