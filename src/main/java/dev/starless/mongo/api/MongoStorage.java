package dev.starless.mongo.api;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import dev.starless.mongo.StorageImpl;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Optional;

/**
 * This class handles interactions between MongoDB
 * and java objects annotated with {@link dev.starless.mongo.api.annotations.MongoObject}.
 */
public interface MongoStorage {

    /**
     * Connects to MongoDB and performs the requested
     * operations on the objects'.
     */
    void init();

    /**
     * Closes the connection to MongoDB.
     */
    void close();

    /**
     * Convenience method which calls {@link StorageImpl#find(Class, IterableProcessor, Bson)}
     * without filtering or doing operations on the output.
     *
     * @param type Type to look for
     * @return a mutable list which contains only objects of the requested type
     */
    default <T> List<T> find(@NotNull Class<? extends T> type) {
        return find(type, Filters.empty());
    }

    /**
     * Convenience method which calls {@link StorageImpl#find(Class, IterableProcessor, Bson)}
     * accepting every type of object T matching a certain criteria.
     *
     * @param type   Type to look for
     * @param filter Filter to apply
     * @return a mutable list which contains only objects of the requested type
     */
    default <T> List<T> find(@NotNull Class<? extends T> type, @NotNull Bson filter) {
        return find(type, IterableProcessor.passthrough, filter);
    }

    /**
     * Convenience method which calls {@link StorageImpl#find(Class, IterableProcessor, Bson)}
     * accepting every type of object T and then performs some operations on the {@link com.mongodb.client.FindIterable<org.bson.Document>}.
     *
     * @param type Type to look for
     * @param processor {@link IterableProcessor} to apply to MongoDB results
     * @return a mutable list which contains only objects of the requested type
     */
    default <T> List<T> find(@NotNull Class<? extends T> type, @NotNull IterableProcessor processor) {
        return find(type, processor, Filters.empty());
    }

    /**
     * Retrieves a list of objects from MongoDB.
     *
     * @param type       Type of the object to retrieve
     * @param filter     Filter to apply
     * @param processor  Operations to perform directly on the {@link com.mongodb.client.FindIterable<org.bson.Document>} before the serialization
     * @return a mutable list which contains only objects of the requested type
     */
    <T> List<T> find(@NotNull Class<? extends T> type,
                     @NotNull IterableProcessor processor,
                     @NotNull Bson filter);


    /**
     * Convenience method which calls {@link StorageImpl#findFirst(Class, IterableProcessor, Bson)}.
     *
     * @param type Type to look for
     * @return the first object in the collection
     */
    default <T> Optional<T> findFirst(@NotNull Class<? extends T> type) {
        return findFirst(type, Filters.empty());
    }

    /**
     * Convenience method which calls {@link StorageImpl#findFirst(Class, IterableProcessor, Bson)}
     * passing only a Filter as a parameter.
     *
     * @param type   Type to look for
     * @param filter Filter to apply
     * @return the first object found matching the criteria
     */
    default <T> Optional<T> findFirst(@NotNull Class<? extends T> type, @NotNull Bson filter) {
        return findFirst(type, IterableProcessor.passthrough, filter);
    }

    /**
     * Convenience method which calls {@link StorageImpl#findFirst(Class, IterableProcessor, Bson)}
     * passing only a {@link IterableProcessor} to apply to MongoDB results.
     *
     * @param type Type to look for
     * @param processor {@link IterableProcessor} instance
     * @return the first object found matching the criteria
     */
    default <T> Optional<T> findFirst(@NotNull Class<? extends T> type, @NotNull IterableProcessor processor) {
        return findFirst(type, processor, Filters.empty());
    }

    /**
     * Retrieves a <b>single</b> object from MongoDB.
     *
     * @param type       Type of the object to retrieve
     * @param filter     Filter to apply
     * @param processor  Operations to perform directly on the {@link com.mongodb.client.FindIterable<org.bson.Document>} before the serialization
     * @return Optional containing an object of type T, otherwise empty if nothing is found
     */
    <T> Optional<T> findFirst(@NotNull Class<? extends T> type,
                              @NotNull IterableProcessor processor,
                              @NotNull Bson filter);

    /**
     * Save an object to the database:
     * can be used to insert or update a document.
     *
     * @param obj    Object to save
     * @param update When set to false, the object will not be saved if another object with the same key is found.
     *               Otherwise, the previous saved object is overwritten.
     *
     * @return true if the MongoDB collection was changed
     */
    boolean store(@NotNull Object obj, boolean update);

    /**
     * Remove an object from the database.
     *
     * @param obj Object to be removed
     * @return the number of objects removed by this call.
     */
    int remove(@NotNull Object obj);

    /**
     * This method returns the collection handling a particular type of object.
     *
     * @param type Type of {@link dev.starless.mongo.api.annotations.MongoObject} annotated object
     * @return The {@link MongoCollection<Document>} storing this type of object
     */
    MongoCollection<Document> getObjectCollection(Class<?> type);

    /**
     * @return The underlying {@link MongoClient} used by this {@link MongoStorage} instance
     */
    MongoClient getClient();
}
