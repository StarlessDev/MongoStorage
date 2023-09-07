package dev.starless.mongo.schema.suppliers;

public abstract class FixedKeySupplier implements ValueSupplier {

    private final String previousKey;

    public FixedKeySupplier(String previousKey) {
        this.previousKey = previousKey;
    }

    @Override
    public String deprecatedKey() {
        return previousKey;
    }
}
