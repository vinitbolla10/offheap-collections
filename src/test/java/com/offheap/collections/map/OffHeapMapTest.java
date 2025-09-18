package com.offheap.collections.map;

import com.offheap.serialization.JavaSerializer;
import com.offheap.serialization.KryoSerializer;
import com.offheap.storage.DirectMemoryStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class OffHeapMapTest {

    private OffHeapMap<String, Integer> map;

    @BeforeEach
    void setUp() {
        map = new OffHeapMap<>(new DirectMemoryStorage(), new JavaSerializer<>(), new JavaSerializer<>(), 10, true);
    }

    @AfterEach
    void tearDown() {
        map.close();
    }

    @Test
    void testBasicPutGetPositive() {
        map.put("Key1", 1);
        assertEquals(1, map.size());
        assertEquals(1, map.get("Key1"));
    }

    @Test
    void testRemove() {
        map.put("Key1", 1);
        assertEquals(1, map.remove("Key1"));
        assertNull(map.get("Key1"));
        assertEquals(0, map.size());
    }

    @Test
    void testDuplicatePut() {
        map.put("Key", 1);
        map.put("Key", 2);
        assertEquals(2, map.get("Key"));
        assertEquals(1, map.size());
    }

    @Test
    void testWithKryoSerializer() {
        OffHeapMap<String, Integer> kryoMap = new OffHeapMap<>(
                new DirectMemoryStorage(),
                new KryoSerializer<>(),  // no class argument needed
                new KryoSerializer<>(),  // no class argument needed
                10,
                true // thread-safe
        );
        kryoMap.put("Test", 42);
        assertEquals(42, kryoMap.get("Test"));
        kryoMap.close();
    }

    @Test
    void testNegativeNullKey() {
        assertNull(map.get("NonExistent"));
    }

    @Test
    void testConcurrentPutAndGet() throws InterruptedException {
        OffHeapMap<String, Integer> threadSafeMap =
                new OffHeapMap<>(new DirectMemoryStorage(), new JavaSerializer<>(), new JavaSerializer<>(), 100, true);

        int threads = 4;
        int perThread = 25;
        Thread[] threadPool = new Thread[threads];

        for (int t = 0; t < threads; t++) {
            final int threadNum = t;
            threadPool[t] = new Thread(() -> {
                for (int i = 0; i < perThread; i++) {
                    String key = "Key-" + (threadNum * perThread + i);
                    threadSafeMap.put(key, threadNum * perThread + i);
                }
            });
            threadPool[t].start();
        }

        // Wait for all threads to finish
        for (Thread t : threadPool) t.join();

        assertEquals(threads * perThread, threadSafeMap.size());
        threadSafeMap.close();
    }

}
