package dev.starless.mongo;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import dev.starless.mongo.api.IterableProcessor;
import dev.starless.mongo.api.MongoStorage;
import dev.starless.mongo.api.annotations.MongoKey;
import dev.starless.mongo.api.annotations.MongoObject;
import dev.starless.mongo.logging.ILogger;
import dev.starless.mongo.schema.MigrationSchema;
import dev.starless.mongo.schema.suppliers.ValueSupplier;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class StorageImpl implements MongoStorage {

    private final ILogger logger;
    private final Gson gson;

    private final String connectionString;
    private MongoClient client;
    private boolean initialized;

    private final Map<String, MongoDatabase> cachedDatabases;
    private final Map<String, Map<String, Class<?>>> cachedKeys;
    private final List<MigrationSchema> schemas;

    StorageImpl(String connectionString, ILogger logger, Gson gson, List<MigrationSchema> schemas) {
        this.logger = logger;
        this.gson = gson;

        this.connectionString = connectionString;
        this.initialized = false;

        this.cachedDatabases = new ConcurrentHashMap<>();
        this.cachedKeys = new ConcurrentHashMap<>();
        this.schemas = schemas;
    }

    @Override
    public void init() {
        if (client != null) close();

        client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(3000, TimeUnit.MILLISECONDS)
                        .readTimeout(3000, TimeUnit.MILLISECONDS))
                .build());

        try (MongoCursor<String> it = client.listDatabaseNames().iterator()) {
            logger.info("Connected to a MongoDB cluster with %d databases.", it.available());
        }

        // Check for schema changes
        schemas.forEach(schema -> {
            MongoDatabase database = getDatabase(schema.database());
            MongoCollection<Document> collection = database.getCollection(schema.collection());

            schema.entries().forEach(entry -> {
                // This list contains the name of fields which will be eliminated later
                Set<String> deprecatedFields = new HashSet<>();

                // For every field, check if any document is missing said field
                collection.find(Filters.exists(entry.fieldName(), false)).forEach(document -> {
                    ValueSupplier defaultSupplier = entry.defaultSupplier();
                    // If the field is missing, let's add the computed value
                    // asked by the user
                    Object defaultValue = defaultSupplier.supply(document);
                    // Insert the name of the old to be deleted field
                    if (entry.hasDeprecatedName()) {
                        deprecatedFields.add(defaultSupplier.deprecatedKey());
                    }

                    collection.findOneAndUpdate(document.toBsonDocument(), Updates.set(entry.fieldName(), defaultValue));
                });

                // Delete every deprecated field
                if (!deprecatedFields.isEmpty()) {
                    BasicDBObject update = new BasicDBObject();
                    BasicDBObject unset = new BasicDBObject();
                    deprecatedFields.forEach(id -> unset.put(id, ""));

                    update.put("$unset", unset);
                    collection.updateMany(Filters.empty(), update);
                }
            });
        });
        logger.info("Validated all documents according to schemas.");

        initialized = true;
    }

    @Override
    public void close() {
        if (client == null) return;

        // Close MongoClient
        client.close();
        client = null;

        // Clear cached resources
        cachedDatabases.clear();
        cachedKeys.clear();

        initialized = false;
    }

    @Override
    public <T> List<T> find(@NotNull Class<? extends T> type,
                            @NotNull IterableProcessor processor,
                            @NotNull Bson filter) {
        if (!initialized) {
            logger.error("Please run MongoStorage#init before querying the database!");
            return Collections.emptyList();
        }

        // List containing the found objects
        List<T> data = new ArrayList<>();

        processRequest(type, (collection, keyInfo) -> {
            // Look for a Document matching the filter
            // and apply the processor's to the output
            FindIterable<Document> iterableDocuments = processor.process(collection.find(filter));

            // Serialize the documents and add them to the list
            iterableDocuments.forEach(document -> {
                try {
                    /*
                    We weed to create an auxiliary object to define the
                    type of the object returned by Gson, otherwise we
                    cannot add it to the list.
                     */
                    T obj = gson.fromJson(document.toJson(), type);

                    // (Finally) add the object
                    data.add(obj);
                } catch (JsonSyntaxException | ClassCastException ignored) {
                    // We just ignore the exceptions 😎
                }
            });
        });
        return data;
    }

    @Override
    public boolean store(@NotNull Object obj, boolean update) {
        if (!initialized) {
            logger.error("Please run MongoStorage#init before querying the database!");
            return false;
        }

        AtomicBoolean bool = new AtomicBoolean(true);
        processRequest(obj.getClass(), (collection, keyInfo) -> {
            String json = gson.toJson(obj); // Convert the java object to Json
            Document doc = Document.parse(json); // Create a new Document from that Json string

            // If we need to update
            if (update) {
                // We use FindOneAndReplaceOptions to return the document that
                // was replaced due to this call
                boolean found = collection.findOneAndReplace(
                        matchFilterFromKeys(obj, keyInfo), // Crea un filtro che cerca lo stesso oggetto
                        doc, // il documento da inserire
                        new FindOneAndReplaceOptions().returnDocument(ReturnDocument.BEFORE)) != null;

                // If something is found, then the
                // document was already replaced,
                // and we do not need to do anything.
                if (found) return;

                // If nothing is found, then nothing was replaced,
                // and we need to insert the document normally
            } else {
                Document retrievedDoc = collection.find(matchFilterFromKeys(doc, keyInfo)).first();
                // If another equal object is found, abort the mission
                if (retrievedDoc != null) {
                    bool.set(false);
                    return;
                }
            }

            // Insert the document normally
            collection.insertOne(doc);
        });
        return bool.get();
    }

    @Override
    public <T> Optional<T> findFirst(@NotNull Class<? extends T> type,
                                     @NotNull IterableProcessor processor,
                                     @NotNull Bson filter) {
        if (!initialized) {
            logger.error("Please run MongoStorage#init before querying the database!");
            return Optional.empty();
        }

        // This object is used to bring out the Document as JSON outside the lambda
        AtomicReference<String> string = new AtomicReference<>(null);
        processRequest(type, ((collection, keyInfo) -> {
            // Look for a Document matching the filter
            // and apply the processor's to the output
            Document doc = processor.process(collection.find(filter)).first(); // Grab the first result
            if (doc != null) { // If something is found
                string.set(doc.toJson());
            }
        }));

        T result = null; // We return an empty optional if nothing is found
        String value = string.get();
        if (value != null) {
            try {
                // Instantiate a new object using Gson
                result = gson.fromJson(value, type);
            } catch (JsonSyntaxException e) {
                logger.error("An error occurred while running findFirst on %s class. (Type mismatch)", type.getSimpleName());
            }
        }
        return Optional.ofNullable(result);
    }

    @Override
    public int remove(@NotNull Object obj) {
        if (!initialized) {
            logger.error("Please run MongoStorage#init before querying the database!");
            return 0;
        }

        AtomicLong integer = new AtomicLong(0);
        processRequest(obj.getClass(), ((collection, keyInfo) -> {
            // Delete the objects and get the number of objects deleted by MongoDB
            integer.set(collection.deleteMany(matchFilterFromKeys(obj, keyInfo)).getDeletedCount());
        }));

        return integer.intValue();
    }

    @Override
    public MongoCollection<Document> getObjectCollection(Class<?> type) {
        MongoObject annotation = type.getAnnotation(MongoObject.class);
        if (annotation == null) {
            logger.warn("The class %s has no @MongoObject annotation!", type.getSimpleName());
            return null;
        } else if (annotation.database().isBlank() || annotation.collection().isBlank()) {
            logger.warn("The class %s has blank @MongoObject annotated field(s)!", type.getSimpleName());
            return null;
        }

        return getDatabase(annotation.database()).getCollection(annotation.collection());
    }

    private MongoDatabase getDatabase(String name) {
        MongoDatabase database;
        if (cachedDatabases.containsKey(name)) {
            database = cachedDatabases.get(name);
        } else {
            database = client.getDatabase(name);
            cachedDatabases.put(name, database);
        }

        return database;
    }

    /*
    This method enables us to perform operations directly on
    the collections containing object with type T
     */
    private void processRequest(Class<?> type, RequestConsumer consumer) {
        if (client == null) return;

        // Retrieve the primary key used for this type of object
        Map<String, Class<?>> keys = getKeys(type);

        // Small check for invalid keys
        if (keys.isEmpty()) {
            logger.warn("There are no keys for this object");
            return;
        }

        // Now let's execute the code passed via parameter
        consumer.accept(getObjectCollection(type), keys);
    }

    /**
     * Creates a filter from the key data previously found with {@link StorageImpl#getKeys(Class)}
     * It converts the Object to a {@link Document} to call {@link StorageImpl#matchFilterFromKeys(Document, Map)}.
     *
     * @param obj  Object to look for
     * @param keys Map containing the key information
     */
    private Bson matchFilterFromKeys(Object obj, Map<String, Class<?>> keys) {
        return matchFilterFromKeys(Document.parse(gson.toJson(obj)), keys);
    }

    /**
     * Creates a filter which can be passed to MongoDB
     * to look for a document equal to the one passed as a parameter.
     *
     * @param document {@link Document} representing a java object
     * @param keys     Map containing the key data
     * @return The filter as a {@link Bson} object
     */
    private Bson matchFilterFromKeys(Document document, Map<String, Class<?>> keys) {
        Set<Bson> filters = new HashSet<>();
        keys.forEach((key, keyType) -> filters.add(Filters.eq(key, document.get(key))));
        return Filters.and(filters);
    }

    /**
     * Finds all the names and types of the fields used
     * in the object's key.
     *
     * @param type The type of generic object
     * @return An Map containing said data
     */
    private Map<String, Class<?>> getKeys(Class<?> type) {
        Map<String, Class<?>> cache = cachedKeys.getOrDefault(type.getName(), null);
        if (cache != null) return cache;

        Map<String, Class<?>> keys = new HashMap<>();
        List<Field> fields = new ArrayList<>();
        searchFields(type, fields);

        fields.stream()
                .filter(field -> field.getAnnotation(MongoKey.class) != null)
                .forEach(field -> keys.put(field.getName(), field.getClass()));

        cachedKeys.put(type.getName(), keys);

        return keys;
    }

    // Finds all fields of a class recursively
    private void searchFields(Class<?> objectType, List<Field> fields) {
        Collections.addAll(fields, objectType.getDeclaredFields());

        Class<?> superClass = objectType.getSuperclass();
        if (superClass != null) searchFields(superClass, fields);
    }

    @Override
    public MongoClient getClient() {
        return client;
    }
}
