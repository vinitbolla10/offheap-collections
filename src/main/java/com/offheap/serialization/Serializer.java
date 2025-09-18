package com.offheap.serialization;

public interface Serializer<T> {
    byte[] serialize(T obj);
    T deserialize(byte[] data);
    long estimatedSize(T obj); // compute actual size of object in bytes
}
