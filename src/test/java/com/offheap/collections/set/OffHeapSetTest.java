package com.offheap.collections.set;

import com.offheap.serialization.JavaSerializer;
import com.offheap.serialization.KryoSerializer;
import com.offheap.storage.DirectMemoryStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

class OffHeapSetTest {
    private OffHeapSet<String> set;

    @BeforeEach
    void setUp() {
        // Non-thread-safe by default
        set = new OffHeapSet<>(new DirectMemoryStorage(), new JavaSerializer<>(), 10, false);
    }

    @AfterEach
    void tearDown() {
        set.close();
    }

    @Test
    void testBasicAddContains() {
        set.add("A");
        assertTrue(set.contains("A"));
        assertEquals(1, set.size());
    }

    @Test
    void testDuplicateAdd() {
        set.add("A");
        set.add("A");
        assertEquals(1, set.size());
    }

    @Test
    void testRemove() {
        set.add("A");
        assertTrue(set.remove("A"));
        assertEquals(0, set.size());
    }

    @Test
    void testWithKryoThreadSafe() {
        OffHeapSet<String> kryoSet = new OffHeapSet<>(new DirectMemoryStorage(), new KryoSerializer<String>(), 10, true);
        kryoSet.add("Test");
        assertTrue(kryoSet.contains("Test"));
        kryoSet.close();
    }

    @Test
    void testNegativeRemoveNonExistent() {
        assertFalse(set.remove("NonExistent"));
    }


    @Test
    void testConcurrentAdd() throws InterruptedException {
        OffHeapSet<String> threadSafeSet = new OffHeapSet<>(new DirectMemoryStorage(), new JavaSerializer<>(), 100, true);
        int threads = 4;
        int perThread = 25;
        CountDownLatch latch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            final int threadNum = t;
            new Thread(() -> {
                for (int i = 0; i < perThread; i++) {
                    threadSafeSet.add("T-" + (threadNum * perThread + i));
                }
                latch.countDown();
            }).start();
        }

        latch.await();
        assertEquals(threads * perThread, threadSafeSet.size());
        threadSafeSet.close();
    }
}
