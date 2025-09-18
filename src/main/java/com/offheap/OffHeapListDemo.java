package com.offheap;

import com.offheap.collections.list.OffHeapList;
import com.offheap.serialization.JavaSerializer;
import com.offheap.storage.DirectMemoryStorage;
import com.offheap.storage.OffHeapStorage;

public class OffHeapListDemo {
    public static void main(String[] args) {
        OffHeapStorage storage = new DirectMemoryStorage();
        try (OffHeapList<String> list = new OffHeapList<>(storage, new JavaSerializer<>(), 10, false)) {
            list.add("Hello");
            list.add("Off-Heap");
            System.out.println("Size: " + list.size());
            System.out.println("Get(0): " + list.get(0));
            list.stream().forEach(System.out::println);
            System.out.println("Avg Latency: " + list.averageLatencyMs() + " ms");
            System.out.println("Memory Usage: " + storage.getMemoryUsage() + " bytes");
        }
    }
}