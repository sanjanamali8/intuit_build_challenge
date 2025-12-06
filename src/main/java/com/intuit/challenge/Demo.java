package com.intuit.challenge;

import java.util.Optional;

public class Demo {
    public static void main(String[] args) throws Exception {
        BoundedBuffer<String> buffer = new BoundedBuffer<>(2);

        // Producer thread
        Thread producer = new Thread(() -> {
            try {
                for (int i = 1; i <= 5; i++) {
                    System.out.println("PUT: " + i);
                    buffer.put("msg-" + i);
                    Thread.sleep(300);
                }
                buffer.close();
            } catch (InterruptedException e) {
                System.out.println("Producer interrupted");
            }
        });

        // Consumer thread
        Thread consumer = new Thread(() -> {
            try {
                while (true) {
                    Optional<String> item = buffer.take();
                    if (item.isEmpty()) {
                        System.out.println("Buffer closed & empty — consumer exiting.");
                        return;
                    }
                    System.out.println("TAKE: " + item.get());
                    Thread.sleep(500);
                }
            } catch (InterruptedException e) {
                System.out.println("Consumer interrupted");
            }
        });

        producer.start();
        consumer.start();

        producer.join();
        consumer.join();
    }
}