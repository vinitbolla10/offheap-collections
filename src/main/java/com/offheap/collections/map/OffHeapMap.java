package com.offheap.collections.map;

import com.offheap.serialization.Serializer;
import com.offheap.storage.OffHeapStorage;

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class OffHeapMap<K, V> extends AbstractMap<K, V> implements AutoCloseable {
    private final OffHeapStorage storage;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private long[] buckets;
    private int size = 0;
    private long dataOffset = 0;
    private int capacity;
    private final Cleaner cleaner = Cleaner.create();
    private final Cleaner.Cleanable cleanable;
    private final boolean threadSafe;
    private long operationCount = 0;
    private long totalLatencyNanos = 0;
    private static final double LOAD_FACTOR = 0.75;

    public OffHeapMap(OffHeapStorage storage, Serializer<K> keySerializer, Serializer<V> valueSerializer,
                      int initialCapacity, boolean threadSafe) {
        this.storage = storage;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.capacity = initialCapacity;
        this.buckets = new long[initialCapacity];
        Arrays.fill(buckets, -1L);
        this.threadSafe = threadSafe;
        storage.allocate(1024L * initialCapacity);
        this.cleanable = cleaner.register(this, this::cleanup);
    }

    private void cleanup() {
        try {
            storage.close();
        } catch (Exception ignored) {
        }
    }

    private <T> T sync(java.util.function.Supplier<T> action) {
        if (threadSafe) {
            synchronized (this) {
                return action.get();
            }
        }
        return action.get();
    }

    private void ensureStorage(long additionalBytes) {
        long needed = dataOffset + additionalBytes;
        if (needed > storage.getMemoryUsage()) {
            long newSize = storage.getMemoryUsage() * 2;
            if (newSize < needed) newSize = needed;
            storage.resize(newSize);
        }
    }

    private void resizeBuckets() {
        int newCapacity = capacity * 2;
        long[] newBuckets = new long[newCapacity];
        Arrays.fill(newBuckets, -1L);
        ByteBuffer buffer = storage.asByteBuffer();

        for (long offset : buckets) {
            while (offset != -1L) {
                buffer.position((int) offset);
                int hash = buffer.getInt();
                long nextOffset = buffer.getLong();
                int keyLength = buffer.getInt();
                byte[] keyData = new byte[keyLength];
                buffer.get(keyData);
                int valueLength = buffer.getInt();
                byte[] valueData = new byte[valueLength];
                buffer.get(valueData);

                int newIndex = Math.abs(hash % newCapacity);
                long newEntryOffset = dataOffset;
                ensureStorage(20L + keyLength + valueLength);
                buffer.position((int) dataOffset)
                        .putInt(hash)
                        .putLong(newBuckets[newIndex])
                        .putInt(keyLength)
                        .put(keyData)
                        .putInt(valueLength)
                        .put(valueData);
                newBuckets[newIndex] = newEntryOffset;
                dataOffset += 20 + keyLength + valueLength;
                offset = nextOffset;
            }
        }
        buckets = newBuckets;
        capacity = newCapacity;
    }

    @Override
    public V put(K key, V value) {
        return sync(() -> {
            long start = System.nanoTime();
            if (size >= capacity * LOAD_FACTOR) {
                resizeBuckets();
            }
            byte[] keyData = keySerializer.serialize(key);
            byte[] valueData = valueSerializer.serialize(value);
            int hash = key.hashCode();
            int index = Math.abs(hash % capacity);

            ByteBuffer buffer = storage.asByteBuffer();
            long currentOffset = buckets[index];
            long prevOffset = -1L;
            while (currentOffset != -1L) {
                buffer.position((int) currentOffset);
                int storedHash = buffer.getInt();
                long nextOffset = buffer.getLong();
                int keyLength = buffer.getInt();
                byte[] storedKey = new byte[keyLength];
                buffer.get(storedKey);
                if (storedHash == hash && keySerializer.deserialize(storedKey).equals(key)) {
                    int valueLength = buffer.getInt();
                    byte[] oldValueData = new byte[valueLength];
                    buffer.get(oldValueData);
                    V oldValue = valueSerializer.deserialize(oldValueData);

                    if (valueData.length != valueLength) {
                        remove(key);
                        put(key, value);
                        totalLatencyNanos += System.nanoTime() - start;
                        operationCount++;
                        return oldValue;
                    }

                    buffer.position((int) (currentOffset + 20 + keyLength)).put(valueData);
                    totalLatencyNanos += System.nanoTime() - start;
                    operationCount++;
                    return oldValue;
                }
                prevOffset = currentOffset;
                currentOffset = nextOffset;
            }

            // New entry
            ensureStorage(20L + keyData.length + valueData.length);
            long newEntryOffset = dataOffset;
            buffer.position((int) dataOffset)
                    .putInt(hash)
                    .putLong(-1L)
                    .putInt(keyData.length)
                    .put(keyData)
                    .putInt(valueData.length)
                    .put(valueData);
            if (prevOffset == -1L) {
                buffer.position((int) (dataOffset + 4)).putLong(buckets[index]);
                buckets[index] = newEntryOffset;
            } else {
                buffer.position((int) (prevOffset + 4)).putLong(newEntryOffset);
            }
            dataOffset += 20 + keyData.length + valueData.length;
            size++;
            totalLatencyNanos += System.nanoTime() - start;
            operationCount++;
            return null;
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        return sync(() -> {
            long start = System.nanoTime();
            int hash = key.hashCode();
            int index = Math.abs(hash % capacity);
            ByteBuffer buffer = storage.asByteBuffer();
            long offset = buckets[index];

            while (offset != -1L) {
                buffer.position((int) offset);
                int storedHash = buffer.getInt();
                long nextOffset = buffer.getLong();
                int keyLength = buffer.getInt();
                byte[] storedKey = new byte[keyLength];
                buffer.get(storedKey);

                if (storedHash == hash && keySerializer.deserialize(storedKey).equals(key)) {
                    int valueLength = buffer.getInt();
                    byte[] valueData = new byte[valueLength];
                    buffer.get(valueData);
                    V result = valueSerializer.deserialize(valueData);
                    totalLatencyNanos += System.nanoTime() - start;
                    operationCount++;
                    return result;
                }
                offset = nextOffset;
            }

            totalLatencyNanos += System.nanoTime() - start;
            operationCount++;
            return null; // must return null if not found
        });
    }

    /**
     * Convenience method to return Optional instead of null.
     */
    public Optional<V> getOptional(Object key) {
        return Optional.ofNullable(get(key));
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        return sync(() -> {
            long start = System.nanoTime();
            int hash = key.hashCode();
            int index = Math.abs(hash % capacity);
            ByteBuffer buffer = storage.asByteBuffer();
            long offset = buckets[index];
            long prevOffset = -1L;

            while (offset != -1L) {
                buffer.position((int) offset);
                int storedHash = buffer.getInt();
                long nextOffset = buffer.getLong();
                int keyLength = buffer.getInt();
                byte[] storedKey = new byte[keyLength];
                buffer.get(storedKey);

                if (storedHash == hash && keySerializer.deserialize(storedKey).equals(key)) {
                    int valueLength = buffer.getInt();
                    byte[] valueData = new byte[valueLength];
                    buffer.get(valueData);
                    V result = valueSerializer.deserialize(valueData);

                    if (prevOffset == -1L) {
                        buckets[index] = nextOffset;
                    } else {
                        buffer.position((int) (prevOffset + 4)).putLong(nextOffset);
                    }

                    size--;
                    totalLatencyNanos += System.nanoTime() - start;
                    operationCount++;
                    return result;
                }
                prevOffset = offset;
                offset = nextOffset;
            }

            totalLatencyNanos += System.nanoTime() - start;
            operationCount++;
            return null;
        });
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new AbstractSet<>() {
            @Override
            public Iterator<Entry<K, V>> iterator() {
                return new Iterator<>() {
                    private int bucketIndex = 0;
                    private long offset = buckets[0];
                    private Entry<K, V> nextEntry;

                    {
                        advance();
                    }

                    private void advance() {
                        while (bucketIndex < capacity && offset == -1L) {
                            bucketIndex++;
                            offset = bucketIndex < capacity ? buckets[bucketIndex] : -1L;
                        }
                        if (offset != -1L) {
                            ByteBuffer buffer = storage.asByteBuffer();
                            buffer.position((int) offset);
                            buffer.getInt(); // Skip hash
                            long next = buffer.getLong();
                            int keyLength = buffer.getInt();
                            byte[] keyData = new byte[keyLength];
                            buffer.get(keyData);
                            int valueLength = buffer.getInt();
                            byte[] valueData = new byte[valueLength];
                            buffer.get(valueData);
                            try {
                                nextEntry = new SimpleEntry<>(keySerializer.deserialize(keyData),
                                        valueSerializer.deserialize(valueData));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            offset = next;
                        } else {
                            nextEntry = null;
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return nextEntry != null;
                    }

                    @Override
                    public Entry<K, V> next() {
                        if (!hasNext()) throw new NoSuchElementException();
                        Entry<K, V> current = nextEntry;
                        advance();
                        return current;
                    }
                };
            }

            @Override
            public int size() {
                return size;
            }
        };
    }

    @Override
    public int size() {
        return size;
    }

    public Stream<Entry<K, V>> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(entrySet().iterator(), Spliterator.DISTINCT), false);
    }

    @Override
    public void close() {
        cleanable.clean();
    }

    public double averageLatencyMs() {
        return operationCount > 0 ? (totalLatencyNanos / (double) operationCount) / 1_000_000 : 0;
    }
}
