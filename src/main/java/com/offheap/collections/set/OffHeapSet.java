package com.offheap.collections.set;

import com.offheap.collections.map.OffHeapMap;
import com.offheap.serialization.Serializer;
import com.offheap.serialization.JavaSerializer; // Add this import
import com.offheap.storage.OffHeapStorage;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

/**
 * An off-heap Set implementation wrapping OffHeapMap.
 */
public class OffHeapSet<E> extends AbstractSet<E> implements AutoCloseable {
    private final OffHeapMap<E, Boolean> map;

    public OffHeapSet(OffHeapStorage storage, Serializer<E> serializer, int initialCapacity, boolean threadSafe) {
        this.map = new OffHeapMap<>(storage, serializer, new JavaSerializer<>(), initialCapacity, threadSafe);
    }

    @Override
    public boolean add(E e) {
        return map.put(e, Boolean.TRUE) == null;
    }

    @Override
    public boolean contains(Object o) {
        return map.containsKey(o);
    }

    @Override
    public boolean remove(Object o) {
        return map.remove(o) != null;
    }

    @Override
    public Iterator<E> iterator() {
        return map.keySet().iterator();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public void close() {
        map.close();
    }

    public double averageLatencyMs() {
        return map.averageLatencyMs();
    }
}