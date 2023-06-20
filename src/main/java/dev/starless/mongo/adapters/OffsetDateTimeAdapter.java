package dev.starless.mongo.adapters;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.time.OffsetDateTime;

public class OffsetDateTimeAdapter implements JsonSerializer<OffsetDateTime>, JsonDeserializer<OffsetDateTime> {

    @Override
    public OffsetDateTime deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        return OffsetDateTime.parse(json.getAsString());
    }

    @Override
    public JsonElement serialize(OffsetDateTime src, Type typeOfSrc, JsonSerializationContext context) {
        return new JsonPrimitive(src.toString());
    }
}
