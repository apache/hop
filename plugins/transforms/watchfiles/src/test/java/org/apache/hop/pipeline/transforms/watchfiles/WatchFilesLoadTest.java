/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.watchfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

/** Opt-in capacity and soak test. Uses a real Local engine, bounded rowsets and a slow sink. */
@EnabledIfSystemProperty(named = "watchfiles.load.seconds", matches = "[1-9][0-9]*")
class WatchFilesLoadTest {
  @TempDir Path temporary;

  @Test
  @Timeout(value = 4, unit = TimeUnit.DAYS)
  void sustainedNativeAndPollingWithConcurrentWritersSlowConsumerAndRestarts() throws Exception {
    HopEnvironment.init();
    for (Class<?> type : List.of(WatchFilesMeta.class, SlowSinkMeta.class)) {
      PluginRegistry.getInstance()
          .registerPluginClass(
              type.getClassLoader(),
              new ArrayList<>(),
              null,
              type.getName(),
              TransformPluginType.class,
              Transform.class,
              true);
    }
    long seconds = Long.parseLong(System.getProperty("watchfiles.load.seconds"));
    int baseline = Integer.parseInt(System.getProperty("watchfiles.load.baseline", "5000"));
    int batchSize = Integer.parseInt(System.getProperty("watchfiles.load.batch", "100"));
    long deliveryTimeout =
        Long.parseLong(System.getProperty("watchfiles.load.delivery.timeout.seconds", "60"));
    Path report =
        Path.of(System.getProperty("watchfiles.load.report", "target/watchfiles-load.jsonl"));
    Files.createDirectories(report.toAbsolutePath().getParent());
    ObjectMapper json = new ObjectMapper();
    AtomicInteger duplicates = new AtomicInteger();
    AtomicInteger unexpected = new AtomicInteger();
    AtomicInteger consumed = new AtomicInteger();
    AtomicReference<Batch> current = new AtomicReference<>();
    Path input = Files.createDirectory(temporary.resolve("input"));
    Path state = Files.createDirectory(temporary.resolve("state"));
    for (int i = 0; i < baseline; i++)
      Files.writeString(input.resolve("baseline-" + i + ".csv"), "baseline");
    var writers = Executors.newFixedThreadPool(3);
    long start = System.nanoTime();
    long deadline = start + TimeUnit.SECONDS.toNanos(seconds);
    int cycles = 0;
    long maximumHeap = 0;
    long warmDescriptors = -1;
    long warmThreads = -1;
    try (BufferedWriter output = Files.newBufferedWriter(report)) {
      output.write(
          json.writeValueAsString(
              Map.of(
                  "event",
                  "started",
                  "at",
                  Instant.now().toString(),
                  "requestedSeconds",
                  seconds,
                  "baseline",
                  baseline,
                  "batchSize",
                  batchSize,
                  "rowsetCapacity",
                  25,
                  "sinkDelayMillis",
                  2,
                  "java",
                  System.getProperty("java.version"),
                  "os",
                  System.getProperty("os.name"))));
      output.newLine();
      output.flush();
      do {
        String strategy = cycles % 2 == 0 ? "NATIVE" : "POLLING";
        WatchFilesMeta watch = new WatchFilesMeta();
        watch.setDirectory(input.toString());
        watch.setStateDirectory(state.toString());
        watch.setWatchId("load");
        watch.setStrategy(strategy);
        watch.setInitialScan("IGNORE_EXISTING");
        watch.setDeleted(true);
        watch.setIncludeWildcard(".*\\.csv");
        watch.setMinimumAge("0");
        watch.setStabilityChecks("2");
        watch.setStabilityInterval("10");
        watch.setPollingInterval("100");
        watch.setReconciliationInterval("1000");
        watch.setCheckpointInterval("250");
        watch.setMaximumEntries(Integer.toString(baseline + batchSize + 100));
        PipelineMeta metadata = new PipelineMeta();
        metadata.setName("Watch Files load");
        metadata.setMetadataProvider(new MemoryMetadataProvider());
        TransformMeta source = new TransformMeta("Source", watch);
        TransformMeta sink = new TransformMeta("Slow sink", new SlowSinkMeta());
        metadata.addTransform(source);
        metadata.addTransform(sink);
        metadata.addPipelineHop(new PipelineHopMeta(source, sink));
        LocalPipelineEngine pipeline = new LocalPipelineEngine(metadata);
        pipeline.setLogLevel(LogLevel.ERROR);
        ((org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration)
                pipeline.getPipelineRunConfiguration().getEngineRunConfiguration())
            .setRowSetSize("25");
        long prepare = System.nanoTime();
        pipeline.prepareExecution();
        assertEquals(25, pipeline.getRowSetSize());
        long prepareMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - prepare);
        pipeline
            .findRunThread("Slow sink")
            .addRowListener(
                new RowAdapter() {
                  @Override
                  public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
                    Batch batch = current.get();
                    if (batch == null
                        || !batch.names.contains(row[1].toString())
                        || !batch.event.equals(row[4])) {
                      unexpected.incrementAndGet();
                      return;
                    }
                    if (!batch.received.add(row[1].toString())) duplicates.incrementAndGet();
                    Long written = batch.started.get(row[1].toString());
                    if (written != null)
                      batch.latencies.add(
                          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - written));
                    consumed.incrementAndGet();
                    batch.done.countDown();
                  }
                });
        pipeline.startThreads();
        List<Long> latencies = new ArrayList<>();
        WatchFilesDiagnostics.Snapshot watchDiagnostics = null;
        long stopMillis;
        try {
          String diagnosticsKey = state.toRealPath().resolve("load").toString();
          long readyDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
          WatchFilesDiagnostics.Snapshot ready = null;
          while (System.nanoTime() < readyDeadline && pipeline.getErrors() == 0) {
            ready = WatchFilesDiagnostics.active(diagnosticsKey);
            if (ready != null && ready.observed() == baseline && ready.scans() > 0) break;
            Thread.sleep(50);
          }
          assertTrue(
              ready != null && ready.observed() == baseline && ready.scans() > 0,
              "Initial inventory must finish before producers publish files: " + ready);
          output.write(
              json.writeValueAsString(
                  Map.of(
                      "event",
                      "ready",
                      "strategy",
                      strategy,
                      "at",
                      Instant.now().toString(),
                      "watchDiagnostics",
                      ready)));
          output.newLine();
          output.flush();
          for (int pass = 0; pass < 10; pass++) {
            Batch created = new Batch("CREATED", batchSize);
            current.set(created);
            List<java.util.concurrent.Future<?>> jobs = new ArrayList<>();
            for (int worker = 0; worker < 3; worker++) {
              final int partition = worker;
              jobs.add(
                  writers.submit(
                      () -> {
                        try {
                          for (int index = partition; index < batchSize; index += 3) {
                            String name = "batch-" + index + ".csv";
                            created.names.add(name);
                            created.started.put(name, System.nanoTime());
                            Path file = input.resolve(name);
                            // Publish only by final rename, the documented reliable writer
                            // protocol.
                            Path staging = input.resolve(name + ".tmp");
                            Files.writeString(staging, "first");
                            Thread.sleep(5);
                            Files.writeString(staging, "complete-payload");
                            Files.move(staging, file, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                          }
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      }));
            }
            for (var job : jobs) job.get(30, TimeUnit.SECONDS);
            assertTrue(
                created.done.await(deliveryTimeout, TimeUnit.SECONDS),
                () ->
                    "Not all created files reached the slow sink: "
                        + created.received.size()
                        + "/"
                        + batchSize
                        + "; "
                        + WatchFilesDiagnostics.active(diagnosticsKey));
            assertEquals(batchSize, created.received.size());
            latencies.addAll(created.latencies);
            Batch deleted = new Batch("DELETED", batchSize);
            deleted.names.addAll(created.names);
            current.set(deleted);
            for (String name : deleted.names) {
              deleted.started.put(name, System.nanoTime());
              Files.delete(input.resolve(name));
            }
            assertTrue(
                deleted.done.await(deliveryTimeout, TimeUnit.SECONDS),
                () ->
                    "Not all deletes reached the slow sink: "
                        + deleted.received.size()
                        + "/"
                        + batchSize
                        + "; "
                        + WatchFilesDiagnostics.active(diagnosticsKey));
            assertEquals(batchSize, deleted.received.size());
            assertEquals(0, pipeline.getErrors());
            assertEquals(0, duplicates.get());
            assertEquals(0, unexpected.get(), "Unexpected or late output outside its batch");
            output.write(
                json.writeValueAsString(
                    Map.of(
                        "event",
                        "progress",
                        "at",
                        Instant.now().toString(),
                        "strategy",
                        strategy,
                        "pass",
                        pass + 1,
                        "consumed",
                        consumed.get(),
                        "duplicates",
                        duplicates.get(),
                        "unexpected",
                        unexpected.get(),
                        "watchDiagnostics",
                        WatchFilesDiagnostics.active(diagnosticsKey))));
            output.newLine();
            output.flush();
            if (System.nanoTime() >= deadline && cycles > 0) break;
          }
          watchDiagnostics =
              WatchFilesDiagnostics.active(state.toRealPath().resolve("load").toString());
        } finally {
          long stopping = System.nanoTime();
          pipeline.stopAll();
          pipeline.waitUntilFinished();
          stopMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stopping);
          assertEquals(0, pipeline.getErrors());
          assertEquals(0, unexpected.get(), "Unexpected output during shutdown");
        }
        cycles++;
        try (JsonFileStateStore checkpoint =
            new JsonFileStateStore(state, "load", null, null, baseline + batchSize + 100)) {
          assertEquals(baseline, checkpoint.load().size());
        }
        long heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        maximumHeap = Math.max(maximumHeap, heap);
        long descriptors = descriptors();
        long threads = ManagementFactory.getThreadMXBean().getThreadCount();
        if (cycles == 2) {
          warmDescriptors = descriptors;
          warmThreads = threads;
        }
        if (cycles > 2) {
          if (descriptors >= 0)
            assertTrue(descriptors <= warmDescriptors + 8, "File descriptor growth after restart");
          assertTrue(threads <= warmThreads + 8, "Thread growth after restart");
        }
        latencies.sort(Long::compareTo);
        Map<String, Object> sample = new java.util.LinkedHashMap<>();
        sample.put("event", "cycle");
        sample.put("at", Instant.now().toString());
        sample.put("strategy", strategy);
        sample.put("cycles", cycles);
        sample.put("consumed", consumed.get());
        sample.put("duplicates", duplicates.get());
        sample.put("unexpected", unexpected.get());
        sample.put("elapsedSeconds", TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start));
        sample.put("prepareMillis", prepareMillis);
        sample.put("stopMillis", stopMillis);
        sample.put("p50Millis", percentile(latencies, .50));
        sample.put("p95Millis", percentile(latencies, .95));
        sample.put("heapBytes", heap);
        sample.put("maximumSampledHeapBytes", maximumHeap);
        sample.put("openDescriptors", descriptors);
        sample.put("threads", threads);
        sample.put("watchDiagnostics", watchDiagnostics);
        sample.put(
            "processCpuNanos",
            ProcessHandle.current()
                .info()
                .totalCpuDuration()
                .orElse(java.time.Duration.ZERO)
                .toNanos());
        output.write(json.writeValueAsString(sample));
        output.newLine();
        output.flush();
      } while (System.nanoTime() < deadline || cycles < 2);
      output.write(
          json.writeValueAsString(
              Map.of(
                  "event",
                  "completed",
                  "at",
                  Instant.now().toString(),
                  "elapsedSeconds",
                  TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start),
                  "cycles",
                  cycles,
                  "consumed",
                  consumed.get(),
                  "duplicates",
                  duplicates.get(),
                  "unexpected",
                  unexpected.get())));
      output.newLine();
      output.flush();
    } finally {
      writers.shutdownNow();
      assertTrue(writers.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static long percentile(List<Long> values, double fraction) {
    return values.isEmpty()
        ? 0
        : values.get(Math.min(values.size() - 1, (int) (values.size() * fraction)));
  }

  private static long descriptors() throws Exception {
    Path path = Path.of("/proc/self/fd");
    if (!Files.isDirectory(path)) return -1;
    try (var entries = Files.list(path)) {
      return entries.count();
    }
  }

  static class Batch {
    final String event;
    final CountDownLatch done;
    final Set<String> names = ConcurrentHashMap.newKeySet();
    final Set<String> received = ConcurrentHashMap.newKeySet();
    final Map<String, Long> started = new ConcurrentHashMap<>();
    final List<Long> latencies = java.util.Collections.synchronizedList(new ArrayList<>());

    Batch(String event, int count) {
      this.event = event;
      done = new CountDownLatch(count);
    }
  }

  @Transform(
      id = "WatchFilesLoadSink",
      name = "Watch Files load test sink",
      description = "Test only")
  public static class SlowSinkMeta extends BaseTransformMeta<SlowSink, SlowSinkData> {}

  public static class SlowSinkData extends BaseTransformData {}

  public static class SlowSink extends BaseTransform<SlowSinkMeta, SlowSinkData> {
    public SlowSink(
        TransformMeta meta,
        SlowSinkMeta options,
        SlowSinkData data,
        int copy,
        PipelineMeta pipelineMeta,
        Pipeline pipeline) {
      super(meta, options, data, copy, pipelineMeta, pipeline);
    }

    @Override
    public boolean processRow() throws org.apache.hop.core.exception.HopException {
      Object[] row = getRow();
      if (row == null) {
        setOutputDone();
        return false;
      }
      try {
        Thread.sleep(2);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      putRow(getInputRowMeta(), row);
      return true;
    }
  }
}
