package dev.starless.mongo.api;

import com.mongodb.client.FindIterable;
import org.bson.Document;

/**
 * This interfaced allows to perform operations directly on the FindIterable object
 * returned by Mongo in find operations
 */
public interface IterableProcessor {

    /**
     * This processor instance does not modify the FindIterable object
     */
    IterableProcessor passthrough = iterable -> iterable;

    FindIterable<Document> process(FindIterable<Document> iterable);
}
