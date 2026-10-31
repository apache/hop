/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.debug;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.hop.core.CursorRowSet;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.debug.DebugController.Activity;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class DebugSessionStoreTest {

  @BeforeAll
  static void setUp() throws HopException {
    HopEnvironment.init();
    PluginRegistry.getInstance()
        .registerPluginClass(
            DebugFixtureMeta.class.getName(),
            TransformPluginType.class,
            org.apache.hop.core.annotations.Transform.class);
  }

  @Test
  void savedCursorIsTheNextRowOnANewEngine() throws Exception {
    String root = tempRoot();
    try (Run first = Run.start()) {
      assertEquals(
          Activity.STEPPED, first.controller.process(first.transform("gen"), 3).activity());
      assertEquals(
          Activity.STEPPED, first.controller.process(first.transform("pass"), 1).activity());
      assertEquals(1, first.hop("gen", "pass").getCursor());

      DebugSession session =
          DebugSessionStore.capture(first.pipeline, "pass", 10, "project", "dev");
      String directory =
          DebugSessionStore.sessionDirectory(
              root, "project", first.pipeline.getPipelineMeta().getName(), "keep");
      DebugSessionStore.save(session, directory);

      DebugSession loaded = DebugSessionStore.load(directory);
      assertEquals(
          1,
          loaded.getHops().stream()
              .filter(hop -> "gen".equals(hop.getOrigin()))
              .findFirst()
              .orElseThrow()
              .getCursor());
      assertEquals(
          3,
          loaded.getHops().stream()
              .filter(hop -> "gen".equals(hop.getOrigin()))
              .findFirst()
              .orElseThrow()
              .getRows()
              .size());

      try (Run second = Run.prepare()) {
        assertEquals(0, DebugSessionStore.apply(second.pipeline, loaded));
        assertEquals(1, second.hop("gen", "pass").getCursor());
        second.pipeline.startThreads();
        assertEquals(
            Activity.STEPPED, second.controller.process(second.transform("pass"), 1).activity());
        CursorRowSet produced = second.hop("pass", "sink");
        assertEquals(2L, produced.storedCount());
        assertEquals(2L, ((Number) produced.copyStoredRow(1)[0]).longValue());
      }
      assertTrue(
          DebugSessionStore.list(root, "project", first.pipeline.getPipelineMeta().getName())
              .contains("keep"));
      DebugSessionStore.delete(directory);
      assertTrue(
          DebugSessionStore.list(root, "project", first.pipeline.getPipelineMeta().getName())
              .isEmpty());
    } finally {
      DebugSessionStore.delete(root);
    }
  }

  @Test
  void importDelimitedAppendsRowsUsingTheHopMetadata() throws Exception {
    CursorRowSet rowSet = new CursorRowSet(10);
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("n"));
    rowSet.setRowMeta(rowMeta);
    Path file = Files.createTempFile("hop-debug-import", ".txt");
    Files.writeString(file, "4\n5\n", StandardCharsets.UTF_8);
    try {
      assertEquals(2, DebugSessionStore.importDelimited(rowSet, file.toString()));
      assertEquals(4L, ((Number) rowSet.copyStoredRow(0)[0]).longValue());
      assertEquals(5L, ((Number) rowSet.copyStoredRow(1)[0]).longValue());
      assertEquals(0, rowSet.getCursor());
    } finally {
      rowSet.clear();
      Files.deleteIfExists(file);
    }
  }

  private static String tempRoot() {
    return System.getProperty("java.io.tmpdir") + "/hop-debug-session-" + UUID.randomUUID();
  }

  private static final class Run implements AutoCloseable {
    private final LocalPipelineEngine pipeline;
    private final DebugController controller;

    private Run(LocalPipelineEngine pipeline, DebugController controller) {
      this.pipeline = pipeline;
      this.controller = controller;
    }

    static Run start() throws HopException {
      Run run = prepare();
      run.pipeline.startThreads();
      return run;
    }

    static Run prepare() throws HopException {
      PipelineMeta meta = new PipelineMeta();
      meta.setName("session");
      DebugFixtureMeta genMeta = new DebugFixtureMeta();
      genMeta.setMode(DebugFixtureMeta.Mode.GENERATE);
      genMeta.setRowsToGenerate(5);
      TransformMeta gen = new TransformMeta("gen", genMeta);
      DebugFixtureMeta passMeta = new DebugFixtureMeta();
      passMeta.setMode(DebugFixtureMeta.Mode.PASS);
      TransformMeta pass = new TransformMeta("pass", passMeta);
      TransformMeta sink =
          new TransformMeta("sink", new org.apache.hop.pipeline.transforms.dummy.DummyMeta());
      meta.addTransform(gen);
      meta.addTransform(pass);
      meta.addTransform(sink);
      meta.addPipelineHop(new PipelineHopMeta(gen, pass));
      meta.addPipelineHop(new PipelineHopMeta(pass, sink));
      LocalPipelineEngine pipeline =
          new LocalPipelineEngine(meta, new Variables(), new LoggingObject("session"));
      DebugController controller = new DebugController(pipeline);
      pipeline.setDebugController(controller);
      pipeline.prepareExecution();
      return new Run(pipeline, controller);
    }

    ITransform transform(String name) {
      return pipeline.getTransform(name, 0);
    }

    CursorRowSet hop(String from, String to) {
      for (CursorRowSet rowSet : controller.getCursorRowSets()) {
        if (from.equals(rowSet.getOriginTransformName())
            && to.equals(rowSet.getDestinationTransformName())) {
          return rowSet;
        }
      }
      throw new AssertionError("Missing hop " + from + " -> " + to);
    }

    @Override
    public void close() {
      pipeline.stopAll();
      pipeline.waitUntilFinished();
      pipeline.cleanup();
    }
  }
}
