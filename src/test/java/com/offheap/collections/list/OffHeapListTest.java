package com.offheap.collections.list;

import com.offheap.serialization.KryoSerializer;
import com.offheap.storage.DirectMemoryStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class OffHeapListTest {
    private OffHeapList<String> list;

    @BeforeEach
    void setUp() {
        // Thread-safe list using KryoSerializer
        list = new OffHeapList<>(new DirectMemoryStorage(), new KryoSerializer<String>(), 10, true);
    }

    @AfterEach
    void tearDown() {
        list.close();
    }

    @Test
    void testBasicAddGetPositive() {
        list.add("A");
        list.add("B");
        assertEquals(2, list.size());
        assertEquals("A", list.get(0));
    }

    @Test
    void testInsertAndRemove() {
        list.add("A");
        list.add("C");
        list.add(1, "B");
        assertEquals("B", list.get(1));
        list.remove(1);
        assertEquals("C", list.get(1));
    }

    @Test
    void testStream() {
        list.add("hello");
        list.add("world");
        List<String> upper = list.stream().map(String::toUpperCase).collect(Collectors.toList());
        assertEquals(List.of("HELLO", "WORLD"), upper);
    }

    @Test
    void testNegativeIndex() {
        assertThrows(IndexOutOfBoundsException.class, () -> list.get(-1));
    }

    @Test
    void testEmptyList() {
        assertEquals(0, list.size());
        assertThrows(IndexOutOfBoundsException.class, () -> list.get(0));
    }

    @Test
    void testConcurrentAdd() throws InterruptedException {
        // Using the same thread-safe list
        Thread t1 = new Thread(() -> {
            for (int i = 0; i < 50; i++) list.add("T1");
        });
        Thread t2 = new Thread(() -> {
            for (int i = 0; i < 50; i++) list.add("T2");
        });
        t1.start(); t2.start();
        t1.join(); t2.join();
        assertEquals(100, list.size());
    }
}
