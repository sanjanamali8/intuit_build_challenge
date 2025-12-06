package com.intuit.challenge;

import java.util.Optional;

/**
 * Producer thread that reads from a {@link SourceContainer} and places items
 * into a {@link BoundedBuffer}.
 */
public class Producer<T> implements Runnable {
    private final SourceContainer<T> source;
    private final BoundedBuffer<T> buffer;

    public Producer(SourceContainer<T> source, BoundedBuffer<T> buffer) {
        this.source = source;
        this.buffer = buffer;
    }

    @Override
    public void run() {
        try {
            while (true) {
                Optional<T> next = source.nextItem();
                if (next.isEmpty()) {
                    buffer.close();
                    break;
                }
                buffer.put(next.get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}