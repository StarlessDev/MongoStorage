package it.ayyjava.storage.structures;

public class QueryBuilder {

    public static QueryBuilder init() {
        return new QueryBuilder();
    }

    public static Query empty() {
        return new Query();
    }

    private final Query query;

    private QueryBuilder() {
        query = new Query();
    }

    public QueryBuilder add(String key, String value) {
        query.getValues().put(key, value);
        return this;
    }

    public QueryBuilder add(String key, Number value) {
        query.getValues().put(key, String.valueOf(value));
        return this;
    }

    public Query create() {
        return query;
    }
}
