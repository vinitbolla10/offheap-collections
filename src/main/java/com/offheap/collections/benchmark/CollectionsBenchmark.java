package com.offheap.collections.benchmark;

import com.offheap.collections.list.OffHeapList;
import com.offheap.collections.map.OffHeapMap;
import com.offheap.collections.set.OffHeapSet;
import com.offheap.serialization.JavaSerializer;
import com.offheap.storage.DirectMemoryStorage;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class CollectionsBenchmark {
    private OffHeapList<String> offHeapList;
    private List<String> arrayList;
    private OffHeapMap<String, Integer> offHeapMap;
    private Map<String, Integer> hashMap;
    private OffHeapSet<String> offHeapSet;
    private Set<String> hashSet;
    private final int size = 1000;

    @Setup(Level.Trial)
    public void setup() {
        offHeapList = new OffHeapList<>(new DirectMemoryStorage(), new JavaSerializer<>(), size, false);
        arrayList = new ArrayList<>(size);
        offHeapMap = new OffHeapMap<>(new DirectMemoryStorage(), new JavaSerializer<>(), new JavaSerializer<>(), size, false);
        hashMap = new HashMap<>(size);
        offHeapSet = new OffHeapSet<>(new DirectMemoryStorage(), new JavaSerializer<>(), size, false);
        hashSet = new HashSet<>(size);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        offHeapList.close();
        offHeapMap.close();
        offHeapSet.close();
    }

    @Benchmark
    public void benchmarkListAdd(Blackhole bh) {
        for (int i = 0; i < size; i++) {
            offHeapList.add("Item" + i);
        }
        bh.consume(offHeapList);
    }

    @Benchmark
    public void benchmarkArrayListAdd(Blackhole bh) {
        for (int i = 0; i < size; i++) {
            arrayList.add("Item" + i);
        }
        bh.consume(arrayList);
    }

    @Benchmark
    public void benchmarkMapPut(Blackhole bh) {
        for (int i = 0; i < size; i++) {
            offHeapMap.put("Key" + i, i);
        }
        bh.consume(offHeapMap);
    }

    @Benchmark
    public void benchmarkHashMapPut(Blackhole bh) {
        for (int i = 0; i < size; i++) {
            hashMap.put("Key" + i, i);
        }
        bh.consume(hashMap);
    }

    @Benchmark
    public void benchmarkSetAdd(Blackhole bh) {
        for (int i = 0; i < size; i++) {
            offHeapSet.add("Item" + i);
        }
        bh.consume(offHeapSet);
    }

    @Benchmark
    public void benchmarkHashSetAdd(Blackhole bh) {
        for (int i = 0; i < size; i++) {
            hashSet.add("Item" + i);
        }
        bh.consume(hashSet);
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(CollectionsBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}