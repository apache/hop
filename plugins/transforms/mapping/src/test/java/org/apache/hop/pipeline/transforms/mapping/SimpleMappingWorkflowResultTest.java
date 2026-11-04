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
package org.apache.hop.pipeline.transforms.mapping;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The 0048 workflow ends its success path on Delete file, which reports success, and its failure
 * path on Abort. This runs that hop shape with stub actions and checks the result hop-run reads.
 */
class SimpleMappingWorkflowResultTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void failingPipelineFailsTheWorkflowAfterCleanup() {
    Harness harness = new Harness(false, true);

    assertFalse(harness.run().isResult());
    assertTrue(harness.failureCleanup.get());
    assertTrue(harness.aborted.get());
    assertFalse(harness.successCleanup.get());
  }

  @Test
  void failingUnitTestFailsTheWorkflowAfterCleanup() {
    Harness harness = new Harness(true, false);

    assertFalse(harness.run().isResult());
    assertTrue(harness.failureCleanup.get());
    assertTrue(harness.aborted.get());
    assertFalse(harness.successCleanup.get());
  }

  @Test
  void successfulRunEndsTrueAfterCleanup() {
    Harness harness = new Harness(true, true);

    assertTrue(harness.run().isResult());
    assertTrue(harness.successCleanup.get());
    assertFalse(harness.failureCleanup.get());
    assertFalse(harness.aborted.get());
  }

  /**
   * Start, then a pipeline and the unit-test action on success hops. Both failure hops share a
   * cleanup action that is followed by Abort. The success hop ends on a cleanup action that returns
   * true, the same way ActionDeleteFile does.
   */
  private static final class Harness {
    private final AtomicBoolean successCleanup = new AtomicBoolean();
    private final AtomicBoolean failureCleanup = new AtomicBoolean();
    private final AtomicBoolean aborted = new AtomicBoolean();
    private final boolean pipelineOk;
    private final boolean testsOk;

    private Harness(boolean pipelineOk, boolean testsOk) {
      this.pipelineOk = pipelineOk;
      this.testsOk = testsOk;
    }

    private Result run() {
      ActionMeta start = action("Start", new ActionStart("Start"));
      ActionMeta pipeline = action("pipeline", new FlagAction(pipelineOk, null));
      ActionMeta tests = action("Run simple-mapping tests", new FlagAction(testsOk, null));
      ActionMeta deleteOk = action("Delete success", new FlagAction(true, successCleanup));
      ActionMeta deleteFail = action("Delete failure", new FlagAction(true, failureCleanup));
      ActionMeta abort = action("Abort workflow", new AbortAction(aborted));

      WorkflowMeta meta = new WorkflowMeta();
      meta.setName("mapping-workflow-result");
      meta.addAction(start);
      meta.addAction(pipeline);
      meta.addAction(tests);
      meta.addAction(deleteOk);
      meta.addAction(deleteFail);
      meta.addAction(abort);

      meta.addWorkflowHop(new WorkflowHopMeta(start, pipeline));
      meta.addWorkflowHop(successHop(pipeline, tests));
      meta.addWorkflowHop(failureHop(pipeline, deleteFail));
      meta.addWorkflowHop(successHop(tests, deleteOk));
      meta.addWorkflowHop(failureHop(tests, deleteFail));
      meta.addWorkflowHop(unconditionalHop(deleteFail, abort));

      LocalWorkflowEngine workflow = new LocalWorkflowEngine(meta);
      workflow.setMetadataProvider(new MemoryMetadataProvider());
      return workflow.startExecution();
    }

    private static ActionMeta action(String name, ActionBase action) {
      action.setName(name);
      return new ActionMeta(action);
    }

    private static WorkflowHopMeta successHop(ActionMeta from, ActionMeta to) {
      WorkflowHopMeta hop = new WorkflowHopMeta(from, to);
      hop.setEvaluation(true);
      hop.setUnconditional(false);
      return hop;
    }

    private static WorkflowHopMeta failureHop(ActionMeta from, ActionMeta to) {
      WorkflowHopMeta hop = new WorkflowHopMeta(from, to);
      hop.setEvaluation(false);
      hop.setUnconditional(false);
      return hop;
    }

    private static WorkflowHopMeta unconditionalHop(ActionMeta from, ActionMeta to) {
      WorkflowHopMeta hop = new WorkflowHopMeta(from, to);
      hop.setUnconditional(true);
      return hop;
    }
  }

  /** Sets the result the way a pipeline, a unit-test action, or Delete file does. */
  private static final class FlagAction extends ActionBase {
    private final boolean success;
    private final AtomicBoolean ran;

    private FlagAction(boolean success, AtomicBoolean ran) {
      super("", "");
      this.success = success;
      this.ran = ran;
    }

    @Override
    public Result execute(Result result, int nr) {
      if (ran != null) {
        ran.set(true);
      }
      result.setResult(success);
      if (!success) {
        result.setNrErrors(1);
      }
      return result;
    }

    @Override
    public boolean isEvaluation() {
      return true;
    }
  }

  /** Same outcome as ActionAbort: the workflow result is false and execution stops. */
  private static final class AbortAction extends ActionBase {
    private final AtomicBoolean ran;

    private AbortAction(AtomicBoolean ran) {
      super("", "");
      this.ran = ran;
    }

    @Override
    public Result execute(Result result, int nr) {
      ran.set(true);
      result.setNrErrors(1);
      result.setResult(false);
      getParentWorkflow().stopExecution();
      return result;
    }

    @Override
    public boolean resetErrorsBeforeExecution() {
      return false;
    }

    @Override
    public boolean isEvaluation() {
      return true;
    }
  }
}
