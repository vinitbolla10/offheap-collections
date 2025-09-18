package com.offheap.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class KryoSerializer<T> implements Serializer<T> {

    private final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(Kryo::new);

    @Override
    public byte[] serialize(T obj) {
        Kryo kryo = kryoThreadLocal.get();
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             Output output = new Output(bos)) {
            kryo.writeClassAndObject(output, obj);
            output.flush();
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(byte[] data) {
        Kryo kryo = kryoThreadLocal.get();
        try (Input input = new Input(new ByteArrayInputStream(data))) {
            return (T) kryo.readClassAndObject(input);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long estimatedSize(T obj) {
        // Rough estimate using serialized size
        return serialize(obj).length;
    }
}
