package com.intuit.challenge;

import java.util.List;
import java.util.Optional;

/**
 * Simple in-memory source container that exposes items sequentially.
 */
public class SourceContainer<T> {
    private final List<T> items;
    private int index = 0;

    public SourceContainer(List<T> items) {
        this.items = List.copyOf(items);
    }

    /**
     * Retrieves the next item if available, otherwise returns an empty Optional.
     */
    public synchronized Optional<T> nextItem() {
        if (index >= items.size()) {
            return Optional.empty();
        }
        T value = items.get(index);
        index++;
        return Optional.of(value);
    }
}