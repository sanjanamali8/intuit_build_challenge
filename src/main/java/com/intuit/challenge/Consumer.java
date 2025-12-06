package com.intuit.challenge;

import java.util.Optional;

/**
 * Consumer thread that pulls items from a {@link BoundedBuffer} and stores them
 * in a {@link DestinationContainer}.
 */
public class Consumer<T> implements Runnable {
    private final BoundedBuffer<T> buffer;
    private final DestinationContainer<T> destination;

    public Consumer(BoundedBuffer<T> buffer, DestinationContainer<T> destination) {
        this.buffer = buffer;
        this.destination = destination;
    }

    @Override
    public void run() {
        try {
            while (true) {
                Optional<T> next = buffer.take();
                if (next.isEmpty()) {
                    break;
                }
                destination.store(next.get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}