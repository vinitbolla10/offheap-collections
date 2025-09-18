package com.offheap.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class MemoryMappedStorage implements OffHeapStorage {
    private final Path path;
    private FileChannel channel;
    private MappedByteBuffer buffer;
    private long allocatedBytes;

    public MemoryMappedStorage(Path path) {
        this.path = path;
    }

    @Override
    public void allocate(long bytes) {
        try {
            channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, bytes);
            allocatedBytes = bytes;
        } catch (IOException e) {
            throw new RuntimeException("Allocation failed", e);
        }
    }

    @Override
    public void resize(long newBytes) {
        try {
            channel.truncate(newBytes);
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, newBytes);
            allocatedBytes = newBytes;
        } catch (IOException e) {
            throw new RuntimeException("Resize failed", e);
        }
    }

    @Override
    public ByteBuffer asByteBuffer() {
        return buffer;
    }

    @Override
    public long getMemoryUsage() {
        return allocatedBytes;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}