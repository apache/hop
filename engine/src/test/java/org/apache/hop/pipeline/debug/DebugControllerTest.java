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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.Condition;
import org.apache.hop.core.CursorRowSet;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.debug.DebugController.Activity;
import org.apache.hop.pipeline.debug.DebugController.StepResult;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class DebugControllerTest {

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
  void aNormalRunStillUsesBlockingRowSets() throws Exception {
    PipelineMeta meta = new PipelineMeta();
    meta.setName("normal");
    TransformMeta first = new TransformMeta("first", new DummyMeta());
    TransformMeta second = new TransformMeta("second", new DummyMeta());
    meta.addTransform(first);
    meta.addTransform(second);
    meta.addPipelineHop(new PipelineHopMeta(first, second));

    LocalPipelineEngine pipeline =
        new LocalPipelineEngine(meta, new Variables(), new LoggingObject("normal"));
    pipeline.prepareExecution();
    pipeline.startThreads();
    pipeline.waitUntilFinished();

    assertInstanceOf(BlockingRowSet.class, pipeline.getRowsets().get(0));
    assertEquals(0, pipeline.getErrors());
  }

  @Test
  void processOneRowLeavesTheNextTransformPaused() throws Exception {
    try (Session session = Session.start(3)) {
      StepResult generated = session.controller.process(session.transform("gen", 0), 1);
      assertEquals(Activity.STEPPED, generated.activity());
      assertEquals(1, generated.completed());
      assertEquals(0, session.transform("drop", 0).getLinesRead());
      assertEquals(1, session.hop("gen", "drop", 0).size());
      assertEquals(1L, ((Number) session.peek("gen", "drop", 0)[0]).longValue());

      StepResult filtered = session.controller.process(session.transform("drop", 0), 1);
      assertEquals(Activity.STEPPED, filtered.activity());
      assertEquals(0, session.transform("sink", 0).getLinesRead());
      assertEquals(1, session.hop("drop", "sink", 0).size());
      assertEquals(0, session.hop("gen", "drop", 0).size());
    }
  }

  @Test
  void anEmptyInputReportsWaiting() throws Exception {
    try (Session session = Session.start(3)) {
      StepResult waiting = session.controller.process(session.transform("drop", 0), 1);
      assertEquals(Activity.WAITING, waiting.activity());
      assertEquals(0, session.transform("drop", 0).getLinesRead());
    }
  }

  @Test
  void aLargeStepKeepsGoingWhileRowsAreStillArriving() throws Exception {
    try (Session session = Session.start(2_000)) {
      // Shorter than this batch takes in total, longer than the gap between two rows.
      StepResult generated =
          session.controller.step(session.transform("gen", 0), 2_000, false, 200);
      assertEquals(Activity.STEPPED, generated.activity());
      assertEquals(2_000, generated.completed());
      assertEquals(2_000, session.hop("gen", "drop", 0).storedCount());
    }
  }

  @Test
  void aMatchingRowPausesTheTransformThatWroteIt() throws Exception {
    try (Session session = Session.start(5)) {
      Condition matchTwo =
          new Condition(
              "n",
              Condition.Function.EQUAL,
              null,
              new ValueMetaAndData(new ValueMetaInteger("n"), 2L));
      session.controller.setHopPause(
          session.hop("gen", "drop", 0), matchTwo, DebugPauseScope.TRANSFORM);

      StepResult generated = session.controller.step(session.transform("gen", 0), 5, false, 500);

      assertEquals(Activity.TIMED_OUT, generated.activity());
      assertEquals(2, generated.completed());
      assertTrue(session.controller.isAnyCopyPaused("gen"));
      assertFalse(session.controller.isAnyCopyFreeRunning("gen"));
    }
  }

  @Test
  void aMatchingRowCanPauseOnlyDownstream() throws Exception {
    try (Session session = Session.start(5)) {
      Condition matchOne =
          new Condition(
              "n",
              Condition.Function.EQUAL,
              null,
              new ValueMetaAndData(new ValueMetaInteger("n"), 1L));
      session.controller.setHopPause(
          session.hop("gen", "drop", 0), matchOne, DebugPauseScope.DOWNSTREAM);
      session.controller.runSelected(
          List.of(session.transform("gen", 0), session.transform("drop", 0)));

      long deadline = System.nanoTime() + 2_000_000_000L;
      while (System.nanoTime() < deadline
          && (session.controller.isAnyCopyFreeRunning("drop")
              || session.transform("gen", 0).getLinesWritten() < 1)) {
        Thread.sleep(20L);
      }

      assertTrue(session.controller.isAnyCopyPaused("drop"));
      assertFalse(session.controller.isAnyCopyPaused("gen"));
    }
  }

  @Test
  void severalTransformsCanRunWhileTheOthersStayPaused() throws Exception {
    try (Session session = Session.start(5)) {
      ITransform gen = session.transform("gen", 0);
      ITransform drop = session.transform("drop", 0);
      session.controller.runSelected(List.of(gen, drop));

      assertTrue(session.controller.isAnyCopyFreeRunning("gen"));
      assertTrue(session.controller.isAnyCopyFreeRunning("drop"));
      assertTrue(session.controller.isAnyCopyPaused("sink"));
      assertFalse(session.controller.isAnyCopyFreeRunning("sink"));
    }
  }

  @Test
  void aSourcePermitWritesOneRow() throws Exception {
    try (Session session = Session.start(3)) {
      session.controller.process(session.transform("gen", 0), 1);
      assertEquals(1, session.transform("gen", 0).getLinesWritten());
      session.controller.process(session.transform("gen", 0), 1);
      assertEquals(2, session.transform("gen", 0).getLinesWritten());
      assertEquals(2, session.hop("gen", "drop", 0).size());
    }
  }

  @Test
  void tryRewindsTheInputAndKeepsOutputAsATrial() throws Exception {
    try (Session session = Session.start(3)) {
      session.controller.process(session.transform("gen", 0), 1);
      CursorRowSet input = session.hop("gen", "drop", 0);
      CursorRowSet output = session.hop("drop", "sink", 0);
      int cursor = input.getCursor();
      long stored = output.storedCount();

      StepResult tried = session.controller.tryRows(session.transform("drop", 0), 1);
      assertEquals(Activity.STEPPED, tried.activity());
      assertEquals(cursor, input.getCursor());
      assertEquals(stored, output.storedCount());
      assertEquals(1, output.getTrialCount());
      assertEquals(1, input.size());
    }
  }

  @Test
  void runToHereFillsTheInputWithoutSteppingTheTarget() throws Exception {
    try (Session session = Session.start(3)) {
      StepResult arrived = session.controller.runToHere(session.transform("drop", 0), 5_000L);
      assertEquals(Activity.STEPPED, arrived.activity());
      assertTrue(session.hop("gen", "drop", 0).size() >= 1);
      assertEquals(0, session.transform("drop", 0).getLinesRead());
    }
  }

  @Test
  void copiesUseSeparateQueues() throws Exception {
    PipelineMeta meta = new PipelineMeta();
    meta.setName("copies");
    DebugFixtureMeta genMeta = new DebugFixtureMeta();
    genMeta.setMode(DebugFixtureMeta.Mode.GENERATE);
    genMeta.setRowsToGenerate(2);
    TransformMeta gen = new TransformMeta("gen", genMeta);
    DebugFixtureMeta passMeta = new DebugFixtureMeta();
    passMeta.setMode(DebugFixtureMeta.Mode.PASS);
    TransformMeta pass = new TransformMeta("pass", passMeta);
    pass.setCopies(2);
    meta.addTransform(gen);
    meta.addTransform(pass);
    meta.addPipelineHop(new PipelineHopMeta(gen, pass));

    try (Session session = Session.start(meta)) {
      session.controller.process(session.transform("gen", 0), 1);
      assertEquals(1, session.hop("gen", "pass", 0).size());
      assertEquals(0, session.hop("gen", "pass", 1).size());

      session.controller.process(session.transform("pass", 0), 1);
      assertEquals(0, session.hop("gen", "pass", 0).size());
      assertEquals(0, session.transform("pass", 1).getLinesRead());

      session.controller.process(session.transform("gen", 0), 1);
      assertEquals(1, session.hop("gen", "pass", 1).size());
      assertEquals(0, session.transform("pass", 1).getLinesRead());
    }
  }

  private static final class Session implements AutoCloseable {
    private final LocalPipelineEngine pipeline;
    private final DebugController controller;

    private Session(LocalPipelineEngine pipeline, DebugController controller) {
      this.pipeline = pipeline;
      this.controller = controller;
    }

    static Session start(int rows) throws HopException {
      PipelineMeta meta = new PipelineMeta();
      meta.setName("debug");
      DebugFixtureMeta genMeta = new DebugFixtureMeta();
      genMeta.setMode(DebugFixtureMeta.Mode.GENERATE);
      genMeta.setRowsToGenerate(rows);
      TransformMeta gen = new TransformMeta("gen", genMeta);
      DebugFixtureMeta dropMeta = new DebugFixtureMeta();
      dropMeta.setMode(DebugFixtureMeta.Mode.DROP_EVEN);
      TransformMeta drop = new TransformMeta("drop", dropMeta);
      TransformMeta sink = new TransformMeta("sink", new DummyMeta());
      meta.addTransform(gen);
      meta.addTransform(drop);
      meta.addTransform(sink);
      meta.addPipelineHop(new PipelineHopMeta(gen, drop));
      meta.addPipelineHop(new PipelineHopMeta(drop, sink));
      return start(meta);
    }

    static Session start(PipelineMeta meta) throws HopException {
      LocalPipelineEngine pipeline =
          new LocalPipelineEngine(meta, new Variables(), new LoggingObject("debug"));
      DebugController controller = new DebugController(pipeline);
      pipeline.setDebugController(controller);
      pipeline.prepareExecution();
      pipeline.startThreads();
      return new Session(pipeline, controller);
    }

    ITransform transform(String name, int copy) {
      return pipeline.getTransform(name, copy);
    }

    CursorRowSet hop(String from, String to, int toCopy) {
      for (CursorRowSet rowSet : controller.getCursorRowSets()) {
        if (from.equals(rowSet.getOriginTransformName())
            && to.equals(rowSet.getDestinationTransformName())
            && rowSet.getDestinationTransformCopy() == toCopy) {
          return rowSet;
        }
      }
      throw new AssertionError("Missing hop " + from + " -> " + to + "." + toCopy);
    }

    Object[] peek(String from, String to, int toCopy) {
      CursorRowSet rowSet = hop(from, to, toCopy);
      int cursor = rowSet.getCursor();
      Object[] row = rowSet.getRowImmediate();
      rowSet.setCursor(cursor);
      return row;
    }

    @Override
    public void close() {
      pipeline.stopAll();
      pipeline.waitUntilFinished();
      pipeline.cleanup();
    }
  }
}
