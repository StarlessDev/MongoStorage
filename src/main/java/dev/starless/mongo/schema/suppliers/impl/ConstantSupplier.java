package dev.starless.mongo.schema.suppliers.impl;

import dev.starless.mongo.schema.suppliers.FixedKeySupplier;
import org.bson.Document;

// This supplier will always return only the fixedValue Object
public class ConstantSupplier extends FixedKeySupplier {

    private final Object fixedValue;

    public ConstantSupplier(String previousKey, Object fixedValue) {
        super(previousKey);

        this.fixedValue = fixedValue;
    }

    @Override
    public Object supply(Document document) {
        return fixedValue;
    }
}
