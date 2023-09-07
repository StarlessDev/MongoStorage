package dev.starless.mongo.schema.suppliers;

import org.bson.Document;

public interface ValueSupplier {

    // This is the function that will compute from the document
    // the object that will be associated to a field named with the new name
    Object supply(Document document);

    // During validation, if a document contains a field with this name
    // it will be deleted after the execution of the migration function
    String deprecatedKey();
}
