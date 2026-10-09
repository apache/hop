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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
@Timeout(30)
class WatchFilesIntegrationTest {
  @Test
  void portableSpecialNamesSurviveNativeAndPollingChangesAndRestart() throws Exception {
    assertSpecialNames(
        "portable", java.util.List.of("áéíóú-测试.csv", "space # [1] {2}+.csv", "literal%20.csv"));
  }

  @Test
  @EnabledOnOs(OS.LINUX)
  void unixSpecialNamesSurviveNativeAndPollingChangesAndRestart() throws Exception {
    assertSpecialNames(
        "unix?\\\n",
        java.util.List.of(
            "report?.csv",
            "a<b.csv",
            "a>b.csv",
            "a\"b.csv",
            "a\\b.csv",
            "a\nb.csv",
            "a\tb.csv",
            "a\u0001b.csv",
            "a|b^`c.csv"));
  }

  private void assertSpecialNames(String rootName, java.util.List<String> names) throws Exception {
    for (String strategy : java.util.List.of("NATIVE", "POLLING")) {
      Path input = Files.createDirectories(temporary.resolve(rootName + strategy));
      Path nested = Files.createDirectory(input.resolve(rootName + "nested"));
      LinkedBlockingQueue<Object[]> rows = new LinkedBlockingQueue<>();
      java.util.function.Consumer<WatchFilesMeta> configure =
          meta -> {
            meta.setWatchId("names-" + strategy);
            meta.setIncludeWildcard("");
            meta.setIncludeSubdirectories(true);
            meta.setDeleted(true);
            meta.setWaitUntilStable(false);
            // Native changes must work through hints, without waiting for periodic reconciliation.
            meta.setReconciliationInterval("60000");
          };
      LocalPipelineEngine pipeline =
          start("Names", strategy, "COMPARE_WITH_STATE", rows, input, configure);
      try {
        for (String name : names) Files.writeString(nested.resolve(name), "created");
        awaitNames(rows, names, "CREATED");
        for (String name : names) Files.writeString(nested.resolve(name), "modified content");
        awaitNames(rows, names, "MODIFIED");
        for (String name : names) Files.delete(nested.resolve(name));
        awaitNames(rows, names, "DELETED");
        assertTrue(pipeline.isRunning());
      } finally {
        stop(pipeline);
      }
      LocalPipelineEngine restarted =
          start("Names again", strategy, "COMPARE_WITH_STATE", rows, input, configure);
      try {
        assertNull(rows.poll(150, TimeUnit.MILLISECONDS));
        Files.writeString(nested.resolve(names.getFirst()), "after restart");
        awaitNames(rows, java.util.List.of(names.getFirst()), "CREATED");
        assertTrue(restarted.isRunning());
      } finally {
        stop(restarted);
      }
    }
  }

  private void awaitNames(
      LinkedBlockingQueue<Object[]> rows, java.util.List<String> expected, String event)
      throws Exception {
    java.util.Set<String> remaining = new java.util.HashSet<>(expected);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!remaining.isEmpty() && System.nanoTime() < deadline) {
      Object[] row = rows.poll(100, TimeUnit.MILLISECONDS);
      if (row == null) continue;
      assertEquals(event, row[4]);
      assertTrue(remaining.remove((String) row[1]), "Unexpected or duplicate filename");
    }
    assertTrue(remaining.isEmpty(), "Missing filename events: " + remaining);
  }

  @Test
  void runtimeAllowsQueuedDownstreamRowsToDrainAfterSourceFinishes() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            WatchFilesLoadTest.SlowSinkMeta.class.getClassLoader(),
            new ArrayList<>(),
            null,
            WatchFilesLoadTest.SlowSinkMeta.class.getName(),
            TransformPluginType.class,
            Transform.class,
            true);
    Path input = Files.createDirectory(temporary.resolve("input"));
    for (int i = 0; i < 5; i++) Files.writeString(input.resolve(i + ".csv"), "ready");
    WatchFilesMeta watch = new WatchFilesMeta();
    watch.setDirectory(input.toString());
    watch.setStateDirectory(temporary.resolve("state").toString());
    watch.setWatchId("drain");
    watch.setInitialScan("EMIT_EXISTING");
    watch.setWaitUntilStable(false);
    watch.setMinimumAge("0");
    watch.setMaximumRunTime("0.01");
    watch.setCheckpointInterval("60000");
    PipelineMeta metadata = new PipelineMeta();
    metadata.setName("Timed downstream drain");
    metadata.setMetadataProvider(new MemoryMetadataProvider());
    TransformMeta source = new TransformMeta("Source", watch);
    TransformMeta sink = new TransformMeta("Sink", new WatchFilesLoadTest.SlowSinkMeta());
    metadata.addTransform(source);
    metadata.addTransform(sink);
    metadata.addPipelineHop(new org.apache.hop.pipeline.PipelineHopMeta(source, sink));
    LocalPipelineEngine pipeline = new LocalPipelineEngine(metadata);
    pipeline.setLogLevel(LogLevel.ERROR);
    pipeline.prepareExecution();
    LinkedBlockingQueue<Object[]> consumed = new LinkedBlockingQueue<>();
    pipeline
        .findRunThread("Sink")
        .addRowListener(
            new RowAdapter() {
              @Override
              public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                  throws HopTransformException {
                try {
                  Thread.sleep(500);
                } catch (InterruptedException failure) {
                  Thread.currentThread().interrupt();
                  throw new HopTransformException(failure);
                }
                consumed.add(row);
              }
            });
    pipeline.startThreads();
    try {
      awaitNormalCompletion(pipeline);
      assertEquals(5, consumed.size(), "Normal EOF must drain every already delivered row");
      assertEquals(5, consumed.stream().map(row -> row[1]).distinct().count());
      String saved = Files.readString(temporary.resolve("state/drain.json"));
      for (int i = 0; i < 5; i++) assertTrue(saved.contains(i + ".csv"));
    } finally {
      if (pipeline.isRunning()) stop(pipeline);
    }
  }

  @Test
  void runtimeFinishesNormallySavesFinalStateAndResumesInBothStrategies() throws Exception {
    for (String strategy : new String[] {"NATIVE", "POLLING"}) {
      Path input = Files.createDirectory(temporary.resolve(strategy));
      Files.writeString(input.resolve("before.csv"), "before");
      LinkedBlockingQueue<Object[]> rows = new LinkedBlockingQueue<>();
      java.util.function.Consumer<WatchFilesMeta> timed =
          meta -> {
            meta.setWatchId("timed-" + strategy);
            meta.setMaximumRunTime("0.01");
            meta.setWaitUntilStable(false);
            meta.setCheckpointInterval("60000");
          };
      LocalPipelineEngine first = start("Timed", strategy, "EMIT_EXISTING", rows, input, timed);
      try {
        Object[] emitted = rows.poll(5, TimeUnit.SECONDS);
        assertNotNull(emitted);
        assertEquals("before.csv", emitted[1]);
        awaitNormalCompletion(first);
        assertNull(rows.poll());
        String saved = Files.readString(temporary.resolve("state/timed-" + strategy + ".json"));
        assertTrue(saved.contains("before.csv"));
      } finally {
        if (first.isRunning()) stop(first);
      }
      Files.writeString(input.resolve("offline.csv"), "offline");
      LocalPipelineEngine second =
          start("Renamed", strategy, "COMPARE_WITH_STATE", rows, input, timed);
      try {
        Object[] emitted = rows.poll(5, TimeUnit.SECONDS);
        assertNotNull(emitted);
        assertEquals("offline.csv", emitted[1]);
        awaitNormalCompletion(second);
        assertNull(rows.poll());
      } finally {
        if (second.isRunning()) stop(second);
      }
      LocalPipelineEngine unlimited =
          start(
              "Continuous",
              strategy,
              "COMPARE_WITH_STATE",
              rows,
              input,
              meta -> {
                timed.accept(meta);
                meta.setMaximumRunTime("");
              });
      try {
        assertNull(rows.poll(800, TimeUnit.MILLISECONDS));
        assertTrue(unlimited.isRunning());
      } finally {
        stop(unlimited);
      }
    }
  }

  private void awaitNormalCompletion(LocalPipelineEngine pipeline) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (pipeline.isRunning() && System.nanoTime() < deadline) Thread.sleep(20);
    assertTrue(!pipeline.isRunning(), "Timed source must finish without manual stop");
    pipeline.waitUntilFinished();
    assertEquals(0, pipeline.getErrors());
    assertTrue(!pipeline.isStopped(), "Timeout must signal EOF, not stop the pipeline");
  }

  @TempDir Path temporary;

  @BeforeAll
  static void initializeHop() throws Exception {
    HopEnvironment.init();
    PluginRegistry.getInstance()
        .registerPluginClass(
            WatchFilesMeta.class.getClassLoader(),
            new java.util.ArrayList<>(),
            null,
            WatchFilesMeta.class.getName(),
            TransformPluginType.class,
            Transform.class,
            true);
  }

  private LocalPipelineEngine start(
      String name, String strategy, String initial, LinkedBlockingQueue<Object[]> rows)
      throws Exception {
    return start(name, strategy, initial, rows, temporary.resolve("input"));
  }

  private LocalPipelineEngine start(
      String name, String strategy, String initial, LinkedBlockingQueue<Object[]> rows, Path input)
      throws Exception {
    return start(name, strategy, initial, rows, input, meta -> {});
  }

  private LocalPipelineEngine start(
      String name,
      String strategy,
      String initial,
      LinkedBlockingQueue<Object[]> rows,
      Path input,
      java.util.function.Consumer<WatchFilesMeta> configure)
      throws Exception {
    WatchFilesMeta meta = new WatchFilesMeta();
    meta.setDirectory(input.toString());
    meta.setWatchId("persistent-input");
    meta.setStateDirectory(temporary.resolve("state").toString());
    meta.setStrategy(strategy);
    meta.setInitialScan(initial);
    meta.setPollingInterval("25");
    meta.setMinimumAge("0");
    meta.setStabilityInterval("20");
    meta.setStabilityChecks("2");
    meta.setCheckpointInterval("25");
    meta.setIncludeWildcard(".*\\.csv");
    configure.accept(meta);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("Watch Files integration");
    pipelineMeta.addTransform(new TransformMeta(name, meta));
    pipelineMeta.setMetadataProvider(new MemoryMetadataProvider());
    LocalPipelineEngine pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.setLogLevel(LogLevel.ERROR);
    try {
      pipeline.prepareExecution();
    } catch (org.apache.hop.core.exception.HopException e) {
      throw new org.apache.hop.core.exception.HopException(
          org.apache.hop.core.logging.HopLogStore.getAppender()
              .getBuffer(pipeline.getLogChannelId(), false)
              .toString(),
          e);
    }
    pipeline
        .findRunThread(name)
        .addRowListener(
            new RowAdapter() {
              @Override
              public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                  throws HopTransformException {
                rows.add(row.clone());
              }
            });
    pipeline.startThreads();
    return pipeline;
  }

  private void stop(LocalPipelineEngine pipeline) {
    pipeline.stopAll();
    pipeline.waitUntilFinished();
    assertEquals(0, pipeline.getErrors());
  }

  @Test
  void wildcardsFilterRealNativeAndPollingRowsIncludingNestedFiles() throws Exception {
    Path input = Files.createDirectories(temporary.resolve("input"));
    Files.writeString(input.resolve("a-test.csv"), "a");
    Files.writeString(input.resolve("b-test[1].txt"), "b");
    Files.writeString(input.resolve("test-skip.csv"), "skip");
    Files.writeString(input.resolve("other.csv"), "other");
    Files.writeString(Files.createDirectories(input.resolve("nested")).resolve("c-test.log"), "c");
    for (String strategy : java.util.List.of("NATIVE", "POLLING")) {
      LinkedBlockingQueue<Object[]> rows = new LinkedBlockingQueue<>();
      LocalPipelineEngine pipeline =
          start(
              "Wildcards",
              strategy,
              "EMIT_EXISTING",
              rows,
              input,
              meta -> {
                meta.setWatchId("wildcards-" + strategy);
                meta.setPatternSyntax("WILDCARD");
                meta.setIncludeWildcard("*test*");
                meta.setExcludeWildcard("*skip*");
                meta.setIncludeSubdirectories(true);
              });
      try {
        java.util.Set<String> names = new java.util.HashSet<>();
        for (int i = 0; i < 3; i++) {
          Object[] row = rows.poll(5, TimeUnit.SECONDS);
          assertNotNull(row);
          assertEquals("CREATED", row[4]);
          assertTrue(names.add((String) row[1]));
        }
        assertEquals(java.util.Set.of("a-test.csv", "b-test[1].txt", "c-test.log"), names);
        assertNull(rows.poll(150, TimeUnit.MILLISECONDS));
      } finally {
        stop(pipeline);
      }
    }
  }

  @Test
  void bundledSampleLoadsAndRunsWithRealDownstreamLogTransform() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            WatchFilesMeta.class.getClassLoader(),
            new java.util.ArrayList<>(),
            null,
            org.apache.hop.pipeline.transforms.writetolog.WriteToLogMeta.class.getName(),
            TransformPluginType.class,
            Transform.class,
            true);
    Path input = Files.createDirectories(temporary.resolve("watch-files/input"));
    org.apache.hop.core.variables.Variables variables =
        new org.apache.hop.core.variables.Variables();
    variables.setVariable("PROJECT_HOME", temporary.toString());
    PipelineMeta metadata =
        new PipelineMeta(
            Path.of("src/main/samples/watch-files/watch-files.hpl").toAbsolutePath().toString(),
            new MemoryMetadataProvider(),
            variables);
    WatchFilesMeta watch =
        (WatchFilesMeta) metadata.findTransform("Watch directory").getTransform();
    assertEquals("AUTO", watch.getStrategy());
    assertTrue(watch.isDeleted());
    assertTrue(watch.isIncludeSubdirectories());
    watch.setMinimumAge("0");
    watch.setStabilityInterval("20");
    LocalPipelineEngine pipeline = new LocalPipelineEngine(metadata);
    pipeline.setVariable("PROJECT_HOME", temporary.toString());
    pipeline.setLogLevel(LogLevel.ERROR);
    pipeline.prepareExecution();
    LinkedBlockingQueue<Object[]> rows = new LinkedBlockingQueue<>();
    pipeline
        .findRunThread("Watch directory")
        .addRowListener(
            new RowAdapter() {
              @Override
              public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
                rows.add(row.clone());
              }
            });
    pipeline.startThreads();
    try {
      Files.writeString(input.resolve("sample.csv"), "example");
      Object[] row = rows.poll(5, TimeUnit.SECONDS);
      assertNotNull(row);
      assertEquals("sample.csv", row[1]);
      assertEquals("CREATED", row[4]);
    } finally {
      stop(pipeline);
    }
    assertTrue(Files.exists(temporary.resolve("watch-files/state/sample-watch-files.json")));
  }

  @Test
  void hopPipelineRestartRecoversOfflineFileAndTransformRenamePreservesWatchId() throws Exception {
    Path input = Files.createDirectory(temporary.resolve("input"));
    Files.writeString(input.resolve("A.csv"), "a");
    LinkedBlockingQueue<Object[]> firstRows = new LinkedBlockingQueue<>();
    LocalPipelineEngine first = start("Watch CSV", "POLLING", "IGNORE_EXISTING", firstRows);
    try {
      assertNull(firstRows.poll(150, TimeUnit.MILLISECONDS));
      Files.writeString(input.resolve("B.csv"), "b");
      Object[] b = firstRows.poll(5, TimeUnit.SECONDS);
      assertNotNull(b);
      assertEquals("B.csv", b[1]);
      assertEquals("CREATED", b[4]);
      assertEquals("persistent-input", b[12]);
      // The row listener runs inside putRow, before source acknowledgement. Wait for the
      // checkpoint so this test asserts an acknowledged restart, not the documented crash window.
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      Path checkpoint = temporary.resolve("state/persistent-input.json");
      while (!Files.readString(checkpoint).contains("B.csv") && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
      assertTrue(Files.readString(checkpoint).contains("B.csv"));
    } finally {
      stop(first);
    }
    Files.writeString(input.resolve("C.csv"), "c");
    LinkedBlockingQueue<Object[]> secondRows = new LinkedBlockingQueue<>();
    LocalPipelineEngine second =
        start("Monitor KPI Files", "POLLING", "COMPARE_WITH_STATE", secondRows);
    try {
      Object[] c = secondRows.poll(5, TimeUnit.SECONDS);
      assertNotNull(c);
      assertEquals("C.csv", c[1]);
      assertNull(secondRows.poll(200, TimeUnit.MILLISECONDS), "A and B must not be re-emitted");
    } finally {
      stop(second);
    }
  }

  @Test
  void nativeSourceEmitsStableFileAndStopsThroughHopLifecycle() throws Exception {
    Path input = Files.createDirectory(temporary.resolve("input"));
    LinkedBlockingQueue<Object[]> rows = new LinkedBlockingQueue<>();
    LocalPipelineEngine pipeline = start("Watch", "NATIVE", "COMPARE_WITH_STATE", rows);
    try {
      Files.writeString(input.resolve("native.csv"), "a");
      Object[] row = rows.poll(5, TimeUnit.SECONDS);
      assertNotNull(row);
      assertEquals("native.csv", row[1]);
      assertNull(rows.poll(150, TimeUnit.MILLISECONDS), "Native create/modify hints must coalesce");
    } finally {
      stop(pipeline);
    }
  }

  @Test
  void nativeSourceSupportsUnicodeAndEscapedCharactersInRootAndFilename() throws Exception {
    Path input = Files.createDirectory(temporary.resolve("árbol con espacios % #"));
    LinkedBlockingQueue<Object[]> rows = new LinkedBlockingQueue<>();
    LocalPipelineEngine pipeline = start("Watch", "NATIVE", "COMPARE_WITH_STATE", rows, input);
    try {
      Files.writeString(input.resolve("datos ü % +.csv"), "unicode");
      Object[] row = rows.poll(5, TimeUnit.SECONDS);
      assertNotNull(row);
      assertEquals("datos ü % +.csv", row[1]);
      assertEquals("CREATED", row[4]);
      try (org.apache.commons.vfs2.FileObject reader =
              org.apache.hop.core.vfs.HopVfs.getFileObject(row[0].toString(), pipeline);
          var stream = reader.getContent().getInputStream()) {
        assertEquals(
            "unicode", new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
      }
      assertNull(rows.poll(150, TimeUnit.MILLISECONDS));
    } finally {
      stop(pipeline);
    }
  }
}
