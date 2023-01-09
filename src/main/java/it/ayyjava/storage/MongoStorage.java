package it.ayyjava.storage;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import it.ayyjava.storage.adapters.DurationAdapter;
import it.ayyjava.storage.adapters.InstantAdapter;
import it.ayyjava.storage.adapters.OffsetDateTimeAdapter;
import it.ayyjava.storage.annotations.MongoKey;
import it.ayyjava.storage.annotations.MongoObject;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class MongoStorage {

    private final Logger logger;
    private final String connectionString;
    private final Map<String, Map<String, Class<?>>> cachedKeys;

    private final Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .serializeNulls()
            .registerTypeAdapter(Duration.class, new DurationAdapter())
            .registerTypeAdapter(Instant.class, new InstantAdapter())
            .registerTypeAdapter(OffsetDateTime.class, new OffsetDateTimeAdapter())
            .create();

    public MongoStorage(Logger logger, String connectionString) {
        this.logger = logger;
        this.connectionString = connectionString;
        this.cachedKeys = new HashMap<>();

        // Verifica che la connessione sia avvenuta
        // con successo
        verifyConnection();
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
                logger.error("Could not retrieve result. Types mismatch!");
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
        AtomicLong integer = new AtomicLong(0);
        processRequest(o.getClass(), ((collection, keyInfo) ->
                integer.set(collection.deleteMany(matchFilterFromKeys(o, keyInfo)).getDeletedCount())));

        return integer.intValue();
    }

    // Metodo interno usato per ridurre la ridondanza del codice al minimo
    private void processRequest(Class<?> type, RequestConsumer consumer) {
        // Otteniamo le informazioni dell' annotazione
        // e facciamo dei check basici
        MongoObject annotation = type.getAnnotation(MongoObject.class);
        if (annotation == null) {
            logger.warn("This object does not have the MongoObject annotation!");
            return;
        } else if (annotation.database().isBlank() || annotation.collection().isBlank()) {
            logger.warn("This object has one field blank!");
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

        // Apri la connessione
        try (MongoClient client = connection()) {
            // Ottieni il database e la collection
            // con i dati dell'annotazione
            MongoDatabase database = client.getDatabase(annotation.database());
            MongoCollection<Document> collection = database.getCollection(annotation.collection());

            consumer.accept(collection, keys); // ora eseguiamo il codice passato via parametro
        } catch (IllegalArgumentException e) {
            logger.error("DB or Collection name are invalid!");
        }
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

    /**
     * Controlla che la connessione al database sia
     * effettuata con successo
     */
    private void verifyConnection() {
        try (MongoClient client = connection()) {
            logger.info(String.format("Connected to a cluster with %s databases%n", client.listDatabaseNames().iterator().available()));
        } catch (Exception e) {
            logger.error(String.format("Failed to connect to cluster, due to: %s", e.getMessage()));
        }
    }

    /**
     * Crea una connessione al database usando la stringa
     *
     * @return MongoClient --> connessione al database
     */
    private MongoClient connection() {
        return MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(3000, TimeUnit.MILLISECONDS)
                        .readTimeout(3000, TimeUnit.MILLISECONDS))
                .build());
    }

    public Gson gson() {
        return gson;
    }
}
