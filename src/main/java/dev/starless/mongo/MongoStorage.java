package dev.starless.mongo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import dev.starless.mongo.adapters.DurationAdapter;
import dev.starless.mongo.adapters.InstantAdapter;
import dev.starless.mongo.adapters.OffsetDateTimeAdapter;
import dev.starless.mongo.api.annotations.MongoKey;
import dev.starless.mongo.api.annotations.MongoObject;
import dev.starless.mongo.logging.ILogger;
import dev.starless.mongo.logging.JavaLogger;
import dev.starless.mongo.logging.SLF4JLogger;
import dev.starless.mongo.logging.SystemLogger;
import dev.starless.mongo.schema.Schema;
import dev.starless.mongo.schema.suppliers.ValueSupplier;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class MongoStorage {

    private final ILogger logger;
    private MongoClient client;
    private boolean initialized;

    private final String connectionString;
    private final Map<String, MongoDatabase> cachedDatabases;
    private final Map<String, Map<String, Class<?>>> cachedKeys;
    private final List<Schema> schemas;

    private Gson gson = null;
    private final GsonBuilder gsonBuilder = new GsonBuilder()
            .setPrettyPrinting()
            .serializeNulls()
            .registerTypeAdapter(Duration.class, new DurationAdapter())
            .registerTypeAdapter(Instant.class, new InstantAdapter())
            .registerTypeAdapter(OffsetDateTime.class, new OffsetDateTimeAdapter());

    public MongoStorage(String connectionString) {
        this(new SystemLogger(), connectionString);
    }

    public MongoStorage(java.util.logging.Logger logger, String connectionString) {
        this(new JavaLogger(logger), connectionString);
    }

    public MongoStorage(org.slf4j.Logger logger, String connectionString) {
        this(new SLF4JLogger(logger), connectionString);
    }

    private MongoStorage(ILogger logger, String connectionString) {
        this.logger = logger;
        this.connectionString = connectionString;
        this.initialized = false;

        this.cachedDatabases = new ConcurrentHashMap<>();
        this.cachedKeys = new ConcurrentHashMap<>();
        this.schemas = new ArrayList<>();
    }

    /**
     * Inizializza il MongoClient e la instance di Gson
     */
    public void init() {
        if (client != null) close();

        gson = gsonBuilder.create();
        client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(3000, TimeUnit.MILLISECONDS)
                        .readTimeout(3000, TimeUnit.MILLISECONDS))
                .build());

        try (MongoCursor<String> it = client.listDatabaseNames().iterator()) {
            logger.info("Connected to a MongoDB cluster with %d databases.", it.available());
        }

        // Check per i cambiamenti di schema
        schemas.forEach(schema -> {
            if (!schema.validate()) {
                logger.warn("The schema for the collection %s is missing some field(s)", schema.collection());
                return;
            }

            MongoDatabase database = getDatabase(schema.database());
            MongoCollection<Document> collection = database.getCollection(schema.collection());

            schema.entries().forEach(entry -> {
                // Lista che contiene i nomi dei campi da eliminare successivamente
                Set<String> deprecatedFields = new HashSet<>();

                // Per ogni entry cerchiamo nel database
                // dei documenti con questo field mancante
                collection.find(Filters.exists(entry.fieldName(), false)).forEach(document -> {
                    ValueSupplier defaultSupplier = entry.defaultSupplier();
                    // Se effettivamente manca, aggiungiamo noi il valore default
                    Object defaultValue = defaultSupplier.supply(document);
                    // Inserisci nella lista il vecchio nome
                    if (entry.hasDeprecatedName()) {
                        deprecatedFields.add(defaultSupplier.deprecatedKey());
                    }

                    collection.findOneAndUpdate(document.toBsonDocument(), Updates.set(entry.fieldName(), defaultValue));
                });

                // Cancella tutti i vecchi campi
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

    /**
     * Chiude il MongoClient
     */
    public void close() {
        if (client == null) return;

        client.close();
        client = null;

        initialized = false;
    }

    /**
     * Permette di registrare un TypeAdapter custom per serializzare
     * classi non supportate da Gson
     *
     * @param type        Tipo della classe
     * @param typeAdapter Classe che deve implementare una di queste classi TypeAdapter, InstanceCreator, JsonSerializer o JsonDeserialize
     * @return questa classe per concatenare le chiamate a questa funzione
     */
    public MongoStorage registerTypeAdapter(Type type, Object typeAdapter) {
        if (initialized) {
            logger.error("MongoStorage#registerTypeAdapter needs to be run before initializing the MongoClient!");
        } else {
            gsonBuilder.registerTypeAdapter(type, typeAdapter);
        }

        return this;
    }

    public MongoStorage registerSchema(Schema schema) {
        if (initialized) {
            logger.error("MongoStorage#registerSchema needs to be run before initializing the MongoClient!");
        } else {
            schemas.add(schema);
        }

        return this;
    }

    /**
     * Trova tutti gli oggetti che corrispondono
     * ai filtri passati tramite parametro
     * (ignora filtri e projections, quindi
     * preleverà tutti i documenti dal database)
     *
     * @param type Tipo della classe da cercare
     * @return List<T> che contiene solo oggetti con il tipo type
     */
    public <T> List<T> find(Class<?> type) {
        return find(type, Filters.empty(), null);
    }

    /**
     * Trova tutti gli oggetti che corrispondono
     * ai filtri passati tramite parametro
     * (ignorando le projections)
     *
     * @param type   Tipo della classe da cercare
     * @param filter Filtri
     * @return List<T> che contiene solo oggetti con il tipo type
     */
    public <T> List<T> find(Class<?> type, Bson filter) {
        return find(type, filter, null);
    }

    /**
     * Trova tutti gli oggetti che corrispondono
     * ai filtri passati tramite parametro
     *
     * @param type       Tipo della classe da cercare
     * @param filter     Filtri
     * @param projection Altri filtri lmao
     * @return List<T> che contiene solo oggetti con il tipo type
     */
    public <T> List<T> find(@NotNull Class<?> type, @NotNull Bson filter, @Nullable Bson projection) {
        if (!initialized) {
            logger.error("Please run MongoStorage#init before querying the database!");
            return Collections.emptyList();
        }

        // Inizializza la lista
        List<T> data = new ArrayList<>();

        // Inizializza la connessione e prende le informazioni dell'oggetto
        processRequest(type, (collection, keyInfo) ->
                // Trova tutti i risultati
                collection.find(filter).projection(projection).forEach(document -> {
                    try {
                        @SuppressWarnings("unchecked") T obj = (T) gson.fromJson(document.toJson(), type); // Cast
                        data.add(obj); // e aggiungi il risultato alla lista
                    } catch (JsonSyntaxException | ClassCastException ignored) {
                        // Non credo che serva (almeno a me) gestire l'eccezzione
                    }
                }));
        return data;
    }

    /**
     * Salva nel database un oggetto e aggiorna
     * i dati se necessario
     *
     * @param o      Oggetto da salvare nel database
     * @param update aggiorna il documento se è già presente uno simile
     */
    public boolean store(@NotNull Object o, boolean update) {
        if (!initialized) {
            logger.error("Please run MongoStorage#init before querying the database!");
            return false;
        }

        AtomicBoolean bool = new AtomicBoolean(true);
        // Inizializza la connessione e prende le informazioni dell'oggetto
        processRequest(o.getClass(), (collection, keyInfo) -> {
            String json = gson.toJson(o); // Trasforma l'oggetto in formato JSON
            Document doc = Document.parse(json); // Crea un documento a partire dal JSON

            if (update) {
                // FindOneAndReplaceOptions è settata per ritornare il documento
                // prima dell'operazione di rimpiazzo
                boolean found = collection.findOneAndReplace(
                        matchFilterFromKeys(o, keyInfo), // Crea un filtro che cerca lo stesso oggetto
                        doc, // il documento da inserire
                        new FindOneAndReplaceOptions().returnDocument(ReturnDocument.BEFORE)) != null;

                // Se è già stato rimpiazzato, va bene così
                if (found) return;
            } else {
                Document retrievedDoc = collection.find(matchFilterFromKeys(doc, keyInfo)).first();
                if (retrievedDoc != null) { // Se c'è un documento già inserito, non fare niente
                    bool.set(false);
                    return;
                }
            }

            collection.insertOne(doc); // Altrimenti, inseriscilo normalmente
        });
        return bool.get();
    }

    /**
     * Recupera un oggetto da un documento
     * presente in MongoDB
     * (ignorando i filtri e le projections, quindi
     * preleverà tutti i documenti)
     *
     * @param type Tipo dell'oggetto
     * @return Optional che contiene un generico (se è stato trovato)
     */
    public <T> Optional<T> findFirst(@NotNull Class<?> type) {
        return findFirst(type, Filters.empty(), null);
    }

    /**
     * Recupera un oggetto da un documento
     * presente in MongoDB
     * (ignorando le projections)
     *
     * @param type   Tipo dell'oggetto
     * @param filter Criterio di ricerca da usare
     * @return Optional che contiene un generico (se è stato trovato)
     */
    public <T> Optional<T> findFirst(@NotNull Class<?> type, @NotNull Bson filter) {
        return findFirst(type, filter, null);
    }

    /**
     * Recupera un oggetto da un documento
     * presente in MongoDB
     *
     * @param type       Tipo dell'oggetto
     * @param filter     Criterio di ricerca da usare
     * @param projection Altri criteri di ricerca
     * @return Optional che contiene un generico (se è stato trovato)
     */
    public <T> Optional<T> findFirst(@NotNull Class<?> type, @NotNull Bson filter, @Nullable Bson projection) {
        if (!initialized) {
            logger.error("Please run MongoStorage#init before querying the database!");
            return Optional.empty();
        }

        // Oggetto usato per estrarre un oggetto dalla lambda
        AtomicReference<String> string = new AtomicReference<>(null);
        // Inizializza la connessione e prende le informazioni dell'oggetto
        processRequest(type, ((collection, keyInfo) -> {
            // Cerca un documento con filtri e proiezioni passati
            // alla funzione come argomenti
            Document doc = collection.find(filter).projection(projection).first(); // prendi il primo risultato
            if (doc != null) { // Se esiste
                string.set(doc.toJson());
            }
        }));

        T result = null; // Valore iniziale
        String value = string.get();
        if (value != null) { // Se il wrapper ha un valore non nullo
            try {
                // crea un nuovo oggetto usando gson
                //noinspection unchecked
                result = (T) gson.fromJson(value, type);
            } catch (JsonSyntaxException e) {
                logger.error("An error occurred while running findFirst on %s class. (Type mismatch)", type.getSimpleName());
            }
        }
        return Optional.ofNullable(result);
    }

    /**
     * Rimuovi un oggetto dal database
     *
     * @param o Object da rimuovere
     * @return numero degli oggetti rimossi
     */
    public int remove(@NotNull Object o) {
        if (!initialized) {
            logger.error("Please run MongoStorage#init before querying the database!");
            return 0;
        }

        AtomicLong integer = new AtomicLong(0);
        processRequest(o.getClass(), ((collection, keyInfo) ->
                integer.set(collection.deleteMany(matchFilterFromKeys(o, keyInfo)).getDeletedCount())));

        return integer.intValue();
    }

    // Metodo interno usato per ridurre la ridondanza del codice al minimo
    private void processRequest(Class<?> type, RequestConsumer consumer) {
        if (client == null) return;

        // Otteniamo le informazioni dell' annotazione
        // e facciamo dei check basici
        MongoObject annotation = type.getAnnotation(MongoObject.class);
        if (annotation == null) {
            logger.warn("The class %s has on @MongoObject annotation!", type.getSimpleName());
            return;
        } else if (annotation.database().isBlank() || annotation.collection().isBlank()) {
            logger.warn("The class %s has blank @MongoObject field(s)!", type.getSimpleName());
            return;
        }

        // Ora dobbiamo ottenere la variabile che sarà
        // la chiave unica che contraddistinguerà il documento
        Map<String, Class<?>> keys = getKeys(type);
        // Facciamo dei check basici
        if (keys.isEmpty()) {
            logger.warn("There are no keys for this object");
            return;
        }

        // Ottieni il database e la collection
        // con i dati dell'annotazione
        MongoDatabase database = getDatabase(annotation.database());
        MongoCollection<Document> collection = database.getCollection(annotation.collection());

        consumer.accept(collection, keys); // ora eseguiamo il codice passato via parametro
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

    /**
     * Ritorna un filtro che cerca nel database un oggetto
     * uguale a quello passato via argomento
     *
     * @param document Documento dell' oggetto
     * @param keys     utilizzate nella ricerca
     * @return Filtro descritto sopra
     */
    private Bson matchFilterFromKeys(Document document, Map<String, Class<?>> keys) {
        Set<Bson> filters = new HashSet<>();
        keys.forEach((key, keyType) -> filters.add(Filters.eq(key, document.get(key))));
        return Filters.and(filters);
    }

    /**
     * Ritorna un filtro che cerca nel database un oggetto
     * uguale a quello passato via argomento.
     * Converte prima l' oggetto in un Document
     *
     * @param obj  Oggetto da cercare
     * @param keys utilizzate nella ricerca
     * @return Filtro descritto sopra
     */
    private Bson matchFilterFromKeys(Object obj, Map<String, Class<?>> keys) {
        return matchFilterFromKeys(Document.parse(gson.toJson(obj)), keys);
    }

    /**
     * Ritorna un hashmap che contiene i types delle
     * variabili usate come chiavi
     *
     * @param objectType Type dell' oggetto da analizzare
     * @return (vedi titolo)
     */
    private Map<String, Class<?>> getKeys(Class<?> objectType) {
        Map<String, Class<?>> cache = cachedKeys.getOrDefault(objectType.getName(), null);
        if (cache != null) return cache;

        Map<String, Class<?>> keys = new HashMap<>();
        List<Field> fields = new ArrayList<>();
        searchFields(objectType, fields);

        fields.stream()
                .filter(field -> field.getAnnotation(MongoKey.class) != null)
                .forEach(field -> keys.put(field.getName(), field.getClass()));

        cachedKeys.put(objectType.getName(), keys);

        return keys;
    }

    private void searchFields(Class<?> objectType, List<Field> fields) {
        Collections.addAll(fields, objectType.getDeclaredFields());

        Class<?> superClass = objectType.getSuperclass();
        if (superClass != null) searchFields(superClass, fields);
    }

    public MongoClient getClient() {
        return client;
    }
}
