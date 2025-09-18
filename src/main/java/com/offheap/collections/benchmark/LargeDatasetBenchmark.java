package com.offheap.collections.benchmark;

import com.offheap.collections.list.OffHeapList;
import com.offheap.collections.map.OffHeapMap;
import com.offheap.collections.set.OffHeapSet;
import com.offheap.serialization.JavaSerializer;
import com.offheap.storage.DirectMemoryStorage;
import com.offheap.storage.MemoryMappedStorage;
import com.offheap.storage.OffHeapStorage;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 3, time = 2)
@Fork(1)
public class LargeDatasetBenchmark {

    @Param({"Direct", "Mapped"})
    private String storageType;

    private OffHeapList<String> offHeapList;
    private List<String> arrayList;
    private OffHeapMap<String, Integer> offHeapMap;
    private Map<String, Integer> hashMap;
    private OffHeapSet<String> offHeapSet;
    private Set<String> hashSet;
    private final int size = 1_000_000; // 1 million elements

    private OffHeapStorage createStorage() throws Exception {
        if ("Direct".equals(storageType)) {
            return new DirectMemoryStorage();
        } else {
            return new MemoryMappedStorage(Path.of("offheap_large.data"));
        }
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        OffHeapStorage storageList = createStorage();
        OffHeapStorage storageMap = createStorage();
        OffHeapStorage storageSet = createStorage();

        offHeapList = new OffHeapList<>(storageList, new JavaSerializer<>(), size, false);
        arrayList = new ArrayList<>(size);

        offHeapMap = new OffHeapMap<>(storageMap, new JavaSerializer<>(), new JavaSerializer<>(), size, false);
        hashMap = new HashMap<>(size);

        offHeapSet = new OffHeapSet<>(storageSet, new JavaSerializer<>(), size, false);
        hashSet = new HashSet<>(size);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        offHeapList.close();
        offHeapMap.close();
        offHeapSet.close();
    }

    @Benchmark
    public void benchmarkOffHeapListAdd(Blackhole bh) {
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
    public void benchmarkOffHeapMapPut(Blackhole bh) {
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
    public void benchmarkOffHeapSetAdd(Blackhole bh) {
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
                .include(LargeDatasetBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}
