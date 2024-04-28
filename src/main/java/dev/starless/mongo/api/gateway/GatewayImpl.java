package dev.starless.mongo.api.gateway;

import dev.starless.mongo.StorageImpl;
import dev.starless.mongo.api.querying.Query;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public abstract class GatewayImpl<T> implements IGateway<T> {

    private final StorageImpl storage;

    public GatewayImpl(StorageImpl storage) {
        this.storage = storage;
    }

    @Override
    public boolean insert(T value) {
        return storage.store(value, false);
    }

    @Override
    public void update(T value) {
        storage.store(value, true);
    }

    @Override
    public int remove(T value) {
        return storage.remove(value);
    }

    @Override
    public Optional<T> loadOne(Query query) {
        return Optional.empty();
    }

    @Override
    public List<T> loadMany(Query query) {
        return Collections.emptyList();
    }

    protected StorageImpl storage() {
        return storage;
    }
}
