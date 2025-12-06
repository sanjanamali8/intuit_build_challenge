import org.junit.jupiter.api.Test;

import com.intuit.challenge.BoundedBuffer;
import com.intuit.challenge.Consumer;
import com.intuit.challenge.DestinationContainer;
import com.intuit.challenge.Producer;
import com.intuit.challenge.SourceContainer;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class ProducerConsumerTest {

    @Test
    void producerAndConsumerTransferAllItems() throws Exception {
        SourceContainer<String> source = new SourceContainer<>(List.of("one", "two", "three"));
        BoundedBuffer<String> buffer = new BoundedBuffer<>(2);
        DestinationContainer<String> destination = new DestinationContainer<>();

        Thread producerThread = new Thread(new Producer<>(source, buffer));
        Thread consumerThread = new Thread(new Consumer<>(buffer, destination));

        producerThread.start();
        consumerThread.start();
        producerThread.join();
        consumerThread.join();

        assertEquals(List.of("one", "two", "three"), destination.snapshot());
        assertTrue(buffer.isClosed());
        assertEquals(0, buffer.size());
    }

    @Test
    void consumerWaitsUntilProducerHasData() throws Exception {
        BoundedBuffer<String> buffer = new BoundedBuffer<>(1);
        DestinationContainer<String> destination = new DestinationContainer<>();

        Thread consumerThread = new Thread(new Consumer<>(buffer, destination));
        consumerThread.start();

        Thread.sleep(150); // allow consumer to block on empty buffer
        buffer.put("ready");
        buffer.close();

        consumerThread.join(500);
        assertFalse(consumerThread.isAlive(), "Consumer should exit after receiving item and close signal");
        assertEquals(List.of("ready"), destination.snapshot());
    }

    @Test
    void bufferBlocksWhenFull() throws Exception {
        BoundedBuffer<String> buffer = new BoundedBuffer<>(1);
        CountDownLatch firstPutDone = new CountDownLatch(1);
        CountDownLatch secondPutStarted = new CountDownLatch(1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> producer = executor.submit(() -> {
                try {
                    buffer.put("first");
                    firstPutDone.countDown();
                    buffer.put("second");
                } catch (InterruptedException ignored) {
                }
            });

            assertTrue(firstPutDone.await(500, java.util.concurrent.TimeUnit.MILLISECONDS));
            // Start another observer to ensure producer is blocked
            Future<Boolean> blockedCheck = executor.submit(() -> {
                secondPutStarted.countDown();
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ignored) {
                }
                return buffer.size() == 1;
            });

            assertTrue(secondPutStarted.await(500, java.util.concurrent.TimeUnit.MILLISECONDS));
            assertTrue(blockedCheck.get(), "Buffer should still be full before take");

            Optional<String> taken = buffer.take();
            assertEquals("first", taken.orElseThrow());
            buffer.close();
            producer.get();
            // closing cancels the second pending put, so the buffer should be empty
            assertEquals(0, buffer.size());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void consumerStopsWhenBufferClosedWithoutData() throws Exception {
        BoundedBuffer<String> buffer = new BoundedBuffer<>(1);
        DestinationContainer<String> destination = new DestinationContainer<>();

        Thread consumerThread = new Thread(new Consumer<>(buffer, destination));
        consumerThread.start();

        Thread.sleep(100);
        buffer.close();

        consumerThread.join(500);
        assertFalse(consumerThread.isAlive());
        assertEquals(List.of(), destination.snapshot());
    }
}