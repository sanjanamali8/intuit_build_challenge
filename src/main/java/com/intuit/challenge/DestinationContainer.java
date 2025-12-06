package com.intuit.challenge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Thread-safe destination container that stores consumed items.
 */
public class DestinationContainer<T> {
    private final List<T> stored = new ArrayList<>();

    public synchronized void store(T item) {
        stored.add(item);
    }

    public synchronized List<T> snapshot() {
        return Collections.unmodifiableList(new ArrayList<>(stored));
    }
}