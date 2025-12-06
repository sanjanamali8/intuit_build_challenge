package com.intuit.challenge;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;

/**
 * Thread-safe bounded buffer implementation using intrinsic locks and
 * wait/notify.
 * 
 * @param <T> type of items stored
 */
public class BoundedBuffer<T> {
    private final Queue<T> queue = new ArrayDeque<>();
    private final int capacity;
    private boolean closed = false;

    public BoundedBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        this.capacity = capacity;
    }

    public synchronized void put(T value) throws InterruptedException {
        // Wait while full AND not closed
        while (!closed && queue.size() == capacity) {
            wait();
        }

        // After waking up, re-check closed before enqueuing
        if (closed) {
            // behave as "cancelled" put
            throw new InterruptedException("Buffer is closed");
        }

        queue.add(value);
        notifyAll(); // wake up any waiting takers
    }

    public synchronized Optional<T> take() throws InterruptedException {
        // Wait while empty AND not closed
        while (!closed && queue.isEmpty()) {
            wait();
        }

        // If empty and closed, no more items will ever come
        if (queue.isEmpty()) {
            return Optional.empty();
        }

        T value = queue.remove();
        notifyAll(); // wake up any waiting producers
        return Optional.of(value);
    }

    public synchronized void close() {
        closed = true;
        // wake everyone so waiting puts/takes can exit
        notifyAll();
    }

    public synchronized int size() {
        if (closed) {
            // From the outside, a closed buffer is considered empty
            return 0;
        }
        return queue.size();
    }

    public synchronized boolean isClosed() {
        return closed;
    }
}