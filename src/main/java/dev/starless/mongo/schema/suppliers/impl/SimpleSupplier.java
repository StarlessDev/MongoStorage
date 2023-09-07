package dev.starless.mongo.schema.suppliers.impl;

import dev.starless.mongo.schema.suppliers.FixedKeySupplier;
import org.bson.Document;

// This supplier will return the object associated with the previousKey
// and if it is not present the defaultValue Object will be returned
public class SimpleSupplier extends FixedKeySupplier {

    private final Object defaultValue;

    public SimpleSupplier(String previousKey, Object defaultValue) {
        super(previousKey);

        this.defaultValue = defaultValue;
    }

    @Override
    public Object supply(Document document) {
        String key = deprecatedKey();
        if (key == null) return null;

        Object value = document.get(key);
        return value != null ? value : defaultValue;
    }
}
