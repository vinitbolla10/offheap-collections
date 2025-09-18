package com.offheap.collections;

import com.offheap.collections.list.OffHeapList;
import com.offheap.collections.map.OffHeapMap;
import com.offheap.collections.set.OffHeapSet;
import com.offheap.serialization.Serializer;
import com.offheap.storage.DirectMemoryStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class CollectionsTest {

    private DirectMemoryStorage mapStorage;
    private DirectMemoryStorage listStorage;
    private DirectMemoryStorage setStorage;

    private OffHeapMap<Integer, String> map;
    private OffHeapList<Integer> list;
    private OffHeapSet<Integer> set;

    private Serializer<Integer> intSerializer;
    private Serializer<String> stringSerializer;

    @BeforeEach
    void setUp() {
        // ----------------- Serializers -----------------
        intSerializer = new Serializer<>() {
            @Override
            public byte[] serialize(Integer obj) {
                return new byte[]{
                        (byte) ((obj >> 24) & 0xFF),
                        (byte) ((obj >> 16) & 0xFF),
                        (byte) ((obj >> 8) & 0xFF),
                        (byte) (obj & 0xFF)
                };
            }

            @Override
            public Integer deserialize(byte[] bytes) {
                return ((bytes[0] & 0xFF) << 24)
                        | ((bytes[1] & 0xFF) << 16)
                        | ((bytes[2] & 0xFF) << 8)
                        | (bytes[3] & 0xFF);
            }

            @Override
            public long estimatedSize(Integer obj) {
                return 4; // Integer always 4 bytes
            }
        };

        stringSerializer = new Serializer<>() {
            @Override
            public byte[] serialize(String obj) {
                return obj.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public String deserialize(byte[] bytes) {
                return new String(bytes, StandardCharsets.UTF_8);
            }

            @Override
            public long estimatedSize(String obj) {
                return obj.getBytes(StandardCharsets.UTF_8).length;
            }
        };

        // ----------------- OffHeap Storage -----------------
        mapStorage = new DirectMemoryStorage();
        listStorage = new DirectMemoryStorage();
        setStorage = new DirectMemoryStorage();

        mapStorage.allocate(1024 * 10);
        listStorage.allocate(1024 * 10);
        setStorage.allocate(1024 * 10);

        // ----------------- OffHeap Collections -----------------
        map = new OffHeapMap<>(mapStorage, intSerializer, stringSerializer, 16, true);
        list = new OffHeapList<>(listStorage, intSerializer, 16, true);
        set = new OffHeapSet<>(setStorage, intSerializer, 16, true);
    }

    @AfterEach
    void tearDown() {
        map.close();
        list.close();
        set.close();
        mapStorage.close();
        listStorage.close();
        setStorage.close();
    }

    // ======================= MAP TESTS =======================
    @Test
    void testMapPutGetRemove() {
        assertNull(map.put(1, "one"));
        assertEquals("one", map.get(1));
        assertEquals("one", map.remove(1));
        assertNull(map.get(1));
    }

    @Test
    void testMapOverwrite() {
        map.put(1, "one");
        assertEquals("one", map.put(1, "uno"));
        assertEquals("uno", map.get(1));
    }

    @Test
    void testMapIterationAndSize() {
        map.put(1, "one");
        map.put(2, "two");
        Set<Integer> keys = new HashSet<>();
        map.entrySet().forEach(entry -> keys.add(entry.getKey()));
        assertTrue(keys.contains(1));
        assertTrue(keys.contains(2));
        assertEquals(2, map.size());
    }

    // ======================= LIST TESTS =======================
    @Test
    void testListAddGetRemove() {
        list.add(10);
        list.add(20);
        list.add(30);
        assertEquals(10, list.get(0));
        assertEquals(20, list.get(1));
        assertEquals(30, list.get(2));
        assertEquals(20, list.remove(1));
        assertEquals(30, list.get(1));
        assertEquals(2, list.size());
    }

    @Test
    void testListIteration() {
        IntStream.range(0, 5).forEach(list::add);
        List<Integer> collected = new ArrayList<>();
        list.stream().forEach(collected::add);
        assertEquals(Arrays.asList(0, 1, 2, 3, 4), collected);
    }

    @Test
    void testListConcurrentAdd() throws InterruptedException {
        int threads = 4;
        int perThread = 25;
        CountDownLatch latch = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            final int threadNum = t;
            new Thread(() -> {
                for (int i = 0; i < perThread; i++) {
                    list.add(threadNum * perThread + i);
                }
                latch.countDown();
            }).start();
        }
        latch.await();
        assertEquals(threads * perThread, list.size());
    }

    // ======================= SET TESTS =======================
    @Test
    void testSetAddContainsRemove() {
        assertTrue(set.add(100));
        assertFalse(set.add(100), "Duplicate element should return false");
        assertTrue(set.contains(100));
        assertTrue(set.remove(100));
        assertFalse(set.contains(100));
    }

    @Test
    void testSetIterationAndSize() {
        set.add(1);
        set.add(2);
        set.add(3);
        Set<Integer> collected = new HashSet<>();
        set.stream().forEach(collected::add);
        assertEquals(Set.of(1, 2, 3), collected);
        assertEquals(3, set.size());
    }

    // ======================= MEMORY TEST =======================
    @Test
    void testMemoryUsageAndResize() {
        long before = mapStorage.getMemoryUsage();
        mapStorage.resize(before * 2);
        long after = mapStorage.getMemoryUsage();
        assertTrue(after > before);
    }
}
