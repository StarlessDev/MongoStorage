package it.ayyjava.storage;

public class LambdaWrapper<T> {

    private T value;

    public LambdaWrapper() {
        this.value = null;
    }

    public boolean hasValue() {
        return value != null;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }
}
