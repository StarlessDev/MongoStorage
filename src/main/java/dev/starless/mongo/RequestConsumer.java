package dev.starless.mongo;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.Map;

public interface RequestConsumer {

    void accept(MongoCollection<Document> collection, Map<String, Class<?>> keys);
}
