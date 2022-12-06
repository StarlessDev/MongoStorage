package it.ayyjava.storage.structures;

public class QueryBuilder {

    public static QueryBuilder init() {
        return new QueryBuilder();
    }

    private final Query query;

    private QueryBuilder() {
        query = new Query();
    }

    public QueryBuilder add(String key, String value) {
        query.getValues().put(key, value);
        return this;
    }

    public Query create() {
        return query;
    }
}
