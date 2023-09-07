package dev.starless.mongo.schema;

import dev.starless.mongo.schema.suppliers.ValueSupplier;

public record Entry(String fieldName, ValueSupplier defaultSupplier) {

    public boolean hasDeprecatedName() {
        return defaultSupplier.deprecatedKey() != null;
    }
}