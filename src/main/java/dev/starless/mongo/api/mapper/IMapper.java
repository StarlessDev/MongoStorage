package dev.starless.mongo.api.mapper;

import dev.starless.mongo.api.querying.Query;

import java.util.List;
import java.util.Optional;

public interface IMapper<T> {

    boolean insert(T value);

    void update(T value);

    int delete(T value);

    Optional<T> search(Query query);

    List<T> bulkSearch(Query query);
}
