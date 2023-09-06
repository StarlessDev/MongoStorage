package dev.starless.mongo.schema;

import org.bson.Document;

public interface ValueSupplier {

    Object supply(Document document);
}
