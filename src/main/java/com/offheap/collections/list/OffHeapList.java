package com.offheap.collections.list;

import com.offheap.serialization.Serializer;
import com.offheap.storage.OffHeapStorage;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class OffHeapList<E> extends AbstractList<E> implements AutoCloseable {
    private final OffHeapStorage storage;
    private final Serializer<E> serializer;
    private long[] offsets; // On-heap offsets for variable-size elements
    private int size = 0;
    private long dataOffset = 0;
    private int capacity;
    private final Cleaner cleaner = Cleaner.create();
    private final Cleaner.Cleanable cleanable;
    private final boolean threadSafe;
    private long operationCount = 0;
    private long totalLatencyNanos = 0;

    public OffHeapList(OffHeapStorage storage, Serializer<E> serializer, int initialCapacity, boolean threadSafe) {
        this.storage = storage;
        this.serializer = serializer;
        this.capacity = initialCapacity;
        this.offsets = new long[initialCapacity];
        this.threadSafe = threadSafe;
        storage.allocate(1024L * initialCapacity); // Estimate 1KB per element
        this.cleanable = cleaner.register(this, this::cleanup);
    }

    private void cleanup() {
        try {
            storage.close();
        } catch (Exception e) {
            // Log error in production
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

    private void ensureCapacity(int minCapacity) {
        if (minCapacity > capacity) {
            int newCapacity = capacity * 2;
            if (newCapacity < minCapacity) newCapacity = minCapacity;
            long[] newOffsets = new long[newCapacity];
            System.arraycopy(offsets, 0, newOffsets, 0, size);
            offsets = newOffsets;
            capacity = newCapacity;
        }
    }

    private void ensureStorage(long additionalBytes) {
        long needed = dataOffset + additionalBytes;
        if (needed > storage.getMemoryUsage()) {
            long newSize = storage.getMemoryUsage() * 2;
            if (newSize < needed) newSize = needed;
            storage.resize(newSize);
        }
    }

    @Override
    public void add(int index, E element) {
        sync(() -> {
            long start = System.nanoTime();
            Objects.checkIndex(index, size + 1);
            ensureCapacity(size + 1);
            byte[] data = serializer.serialize(element);
            ensureStorage(data.length);

            ByteBuffer buffer = storage.asByteBuffer();
            long insertOffset = (index == size) ? dataOffset : offsets[index];

            if (index < size) {
                long tailStart = offsets[index];
                long tailSize = dataOffset - tailStart;
                buffer.position((int) tailStart);
                byte[] tail = new byte[(int) tailSize];
                buffer.get(tail);
                buffer.position((int) (tailStart + data.length));
                buffer.put(tail);

                System.arraycopy(offsets, index, offsets, index + 1, size - index);

                for (int i = index + 1; i <= size; i++) {
                    offsets[i] += data.length;
                }
            }

            buffer.position((int) insertOffset).put(data);
            offsets[index] = insertOffset;
            dataOffset += data.length;
            size++;
            totalLatencyNanos += System.nanoTime() - start;
            operationCount++;
            return null;
        });
    }

    @Override
    public E remove(int index) {
        return sync(() -> {
            long start = System.nanoTime();
            E old = get(index);
            long startOffset = offsets[index];
            long endOffset = (index + 1 < size) ? offsets[index + 1] : dataOffset;
            long removeSize = endOffset - startOffset;

            ByteBuffer buffer = storage.asByteBuffer();
            if (index + 1 < size) {
                long tailStart = endOffset;
                long tailSize = dataOffset - tailStart;
                buffer.position((int) tailStart);
                byte[] tail = new byte[(int) tailSize];
                buffer.get(tail);
                buffer.position((int) startOffset);
                buffer.put(tail);
            }

            System.arraycopy(offsets, index + 1, offsets, index, size - index - 1);

            for (int i = index; i < size - 1; i++) {
                offsets[i] -= removeSize;
            }

            dataOffset -= removeSize;
            size--;
            totalLatencyNanos += System.nanoTime() - start;
            operationCount++;
            return old;
        });
    }
    @Override
    public boolean add(E e) {
        add(size, e);
        return true;
    }

    @Override
    public E get(int index) {
        return sync(() -> {
            long start = System.nanoTime();
            Objects.checkIndex(index, size);
            long startOffset = offsets[index];
            long endOffset = (index + 1 < size) ? offsets[index + 1] : dataOffset;
            int length = (int) (endOffset - startOffset);
            byte[] data = new byte[length];
            storage.asByteBuffer().position((int) startOffset).get(data);
            E result = serializer.deserialize(data);
            totalLatencyNanos += System.nanoTime() - start;
            operationCount++;
            return result;
        });
    }

    @Override
    public E set(int index, E element) {
        return sync(() -> {
            long start = System.nanoTime();
            E old = get(index);
            remove(index);
            add(index, element);
            totalLatencyNanos += System.nanoTime() - start;
            operationCount++;
            return old;
        });
    }

//    @Override
//    public E remove1(int index) {
//        return sync(() -> {
//            long start = System.nanoTime();
//            E old = get(index);
//            long startOffset = offsets[index];
//            long endOffset = (index + 1 < size) ? offsets[index + 1] : dataOffset;
//            long removeSize = endOffset - startOffset;
//
//            ByteBuffer buffer = storage.asByteBuffer();
//            if (index + 1 < size) {
//                long tailStart = endOffset;
//                long tailSize = dataOffset - tailStart;
//                buffer.position((int) tailStart);
//                byte[] tail = new byte[(int) tailSize];
//                buffer.get(tail);
//                buffer.position((int) startOffset);
//                buffer.put(tail);
//            }
//
//            System.arraycopy(offsets, index + 1, offsets, index, size - index - 1);
//            size--;
//            dataOffset -= removeSize;
//            totalLatencyNanos += System.nanoTime() - start;
//            operationCount++;
//            return old;
//        });
//    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Iterator<E> iterator() {
        return new Iterator<>() {
            private int current = 0;

            @Override
            public boolean hasNext() {
                return current < size;
            }

            @Override
            public E next() {
                if (!hasNext()) throw new NoSuchElementException();
                return get(current++);
            }
        };
    }

    @Override
    public Stream<E> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.DISTINCT | Spliterator.ORDERED), false);
    }

    @Override
    public void close() {
        cleanable.clean();
    }

    public double averageLatencyMs() {
        return operationCount > 0 ? (totalLatencyNanos / (double) operationCount) / 1_000_000 : 0;
    }

    public long getMemoryUsage() {
        return storage.getMemoryUsage();
    }
}