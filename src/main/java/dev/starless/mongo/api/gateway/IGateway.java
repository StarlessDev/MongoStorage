package dev.starless.mongo.api.gateway;

import dev.starless.mongo.api.Query;

import java.util.List;
import java.util.Optional;

public interface IGateway<T> {

    boolean insert(T value);

    void update(T value);

    int remove(T value);

    Optional<T> load(Query query);

    List<T> lazyLoad(Query query);
}
