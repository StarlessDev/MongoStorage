package dev.starless.mongo.api;

import com.mongodb.client.FindIterable;
import org.bson.Document;

/**
 * Interface che permette di effettuare operazioni direttamente
 * sull'oggetto FindIterable che ritorna Mongo nelle operazioni di find
 */
public interface IterableProcessor {

    /**
     * Questo processor ritorna il parametro senza modificarlo
     */
    IterableProcessor passthrough = iterable -> iterable;

    FindIterable<Document> process(FindIterable<Document> iterable);
}
