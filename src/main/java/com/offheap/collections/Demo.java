package com.offheap.collections;

import com.offheap.collections.list.OffHeapList;
import com.offheap.collections.map.OffHeapMap;
import com.offheap.collections.set.OffHeapSet;
import com.offheap.serialization.JavaSerializer;
import com.offheap.storage.DirectMemoryStorage;

public class Demo {
    public static void main(String[] args) {
        // List Demo
        try (OffHeapList<String> list = new OffHeapList<>(new DirectMemoryStorage(), new JavaSerializer<>(), 10, false)) {
            list.add("Apple");
            list.add("Banana");
            list.add(1, "Orange");
            System.out.println("List: " + list);
            System.out.println("List size: " + list.size());
            System.out.println("List get(1): " + list.get(1));
            list.stream().forEach(s -> System.out.println("List stream: " + s));
            System.out.println("List latency: " + list.averageLatencyMs() + " ms");
            System.out.println("List memory: " + list.getMemoryUsage() + " bytes");
        }

        // Map Demo
        try (OffHeapMap<String, Integer> map = new OffHeapMap<>(new DirectMemoryStorage(), new JavaSerializer<>(), new JavaSerializer<>(), 10, false)) {
            map.put("One", 1);
            map.put("Two", 2);
            System.out.println("Map: " + map);
            System.out.println("Map get(Two): " + map.get("Two"));
            map.entrySet().forEach(e -> System.out.println("Map entry: " + e.getKey() + "=" + e.getValue()));
            System.out.println("Map latency: " + map.averageLatencyMs() + " ms");
        }

        // Set Demo
        try (OffHeapSet<String> set = new OffHeapSet<>(new DirectMemoryStorage(), new JavaSerializer<>(), 10, false)) {
            set.add("Cat");
            set.add("Dog");
            set.add("Cat"); // Duplicate
            System.out.println("Set: " + set);
            System.out.println("Set contains(Dog): " + set.contains("Dog"));
            set.forEach(s -> System.out.println("Set element: " + s));
            System.out.println("Set latency: " + set.averageLatencyMs() + " ms");
        }
    }
}