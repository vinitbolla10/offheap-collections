package com.offheap.storage;

import java.nio.ByteBuffer;

public interface OffHeapStorage extends AutoCloseable {
    void allocate(long bytes);
    void resize(long newBytes);
    ByteBuffer asByteBuffer();
    long getMemoryUsage();
}