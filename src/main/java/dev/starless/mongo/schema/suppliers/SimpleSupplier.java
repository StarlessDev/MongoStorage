package dev.starless.mongo.schema.suppliers;

import dev.starless.mongo.schema.ValueSupplier;
import org.bson.Document;

public class SimpleSupplier implements ValueSupplier {

    private final String legacyKey;
    private final Object defaultValue;

    public SimpleSupplier(String legacyKey) {
        this.legacyKey = legacyKey;
        this.defaultValue = null;
    }

    public SimpleSupplier(String legacyKey, String defaultValue) {
        this.legacyKey = legacyKey;
        this.defaultValue = defaultValue;
    }

    @Override
    public Object supply(Document document) {
        Object value = document.get(legacyKey);
        return value != null ? value : defaultValue;
    }
}
