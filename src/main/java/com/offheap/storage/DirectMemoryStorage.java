package com.offheap.storage;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;

public class DirectMemoryStorage implements OffHeapStorage {
    private Arena arena;
    private MemorySegment segment;
    private long allocatedBytes = 0;

    @Override
    public void allocate(long bytes) {
        if (segment != null) {
            arena.close();
        }
        // ✅ Use shared arena for multi-threaded access
        arena = Arena.ofShared();
        segment = arena.allocate(bytes, ValueLayout.JAVA_BYTE.byteAlignment());
        allocatedBytes = bytes;
    }

    @Override
    public void resize(long newBytes) {
        // ✅ Use shared arena here as well
        Arena newArena = Arena.ofShared();
        MemorySegment newSegment = newArena.allocate(newBytes, ValueLayout.JAVA_BYTE.byteAlignment());
        if (segment != null && allocatedBytes > 0) {
            ByteBuffer srcBuf = segment.asByteBuffer();
            srcBuf.position(0).limit((int) Math.min(allocatedBytes, newBytes));
            ByteBuffer dstBuf = newSegment.asByteBuffer();
            dstBuf.put(srcBuf);
        }
        if (arena != null) {
            arena.close();
        }
        arena = newArena;
        segment = newSegment;
        allocatedBytes = newBytes;
    }

    @Override
    public ByteBuffer asByteBuffer() {
        return segment.asByteBuffer();
    }

    @Override
    public long getMemoryUsage() {
        return allocatedBytes;
    }

    @Override
    public void close() {
        if (arena != null) {
            arena.close();
            arena = null;
            segment = null;
        }
    }
}
