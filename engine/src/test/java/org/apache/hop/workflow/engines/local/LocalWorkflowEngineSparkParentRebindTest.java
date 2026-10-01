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

package org.apache.hop.workflow.engines.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionState;
import org.junit.jupiter.api.Test;

/**
 * Regression for issue #7743: parentId must not be overwritten with the literal variable name
 * {@code Internal.Spark.TransformOwnerId} when the Spark owner variable is unset.
 */
class LocalWorkflowEngineSparkParentRebindTest {

  @Test
  void sparkTransformOwnerId_nullWhenUnset() {
    Variables vars = new Variables();
    vars.initializeFrom(null);
    assertNull(LocalWorkflowEngine.sparkTransformOwnerId(vars));
  }

  @Test
  void sparkTransformOwnerId_returnsValueWhenSet() {
    Variables vars = new Variables();
    vars.initializeFrom(null);
    vars.setVariable(LocalWorkflowEngine.VAR_SPARK_TRANSFORM_OWNER_ID, "pipe|Workflow Executor|0");
    assertEquals("pipe|Workflow Executor|0", LocalWorkflowEngine.sparkTransformOwnerId(vars));
  }

  @Test
  void sparkTransformOwnerId_nullVariables() {
    assertNull(LocalWorkflowEngine.sparkTransformOwnerId(null));
  }

  @Test
  void rebind_doesNotOverwriteParentWhenSparkVariableUnset() {
    LocalWorkflowEngine engine = new LocalWorkflowEngine();
    engine.initializeFrom(null);

    Execution execution = new Execution();
    execution.setParentId("parent-workflow-uuid");
    engine.rebindSparkTransformOwnerParent(execution);
    assertEquals("parent-workflow-uuid", execution.getParentId());

    Execution root = new Execution();
    root.setParentId(null);
    engine.rebindSparkTransformOwnerParent(root);
    assertNull(root.getParentId());
  }

  @Test
  void rebind_setsSparkOwnerWhenVariablePresent() {
    LocalWorkflowEngine engine = new LocalWorkflowEngine();
    engine.initializeFrom(null);
    engine.setVariable(
        LocalWorkflowEngine.VAR_SPARK_TRANSFORM_OWNER_ID, "spark-pipe|Workflow Executor|0");

    Execution execution = new Execution();
    execution.setParentId("would-be-overwritten");
    engine.rebindSparkTransformOwnerParent(execution);
    assertEquals("spark-pipe|Workflow Executor|0", execution.getParentId());

    ExecutionState state = new ExecutionState();
    state.setParentId("state-parent");
    engine.rebindSparkTransformOwnerParent(state);
    assertEquals("spark-pipe|Workflow Executor|0", state.getParentId());
  }

  @Test
  void rebind_ignoresEmptySparkOwner() {
    LocalWorkflowEngine engine = new LocalWorkflowEngine();
    engine.initializeFrom(null);
    engine.setVariable(LocalWorkflowEngine.VAR_SPARK_TRANSFORM_OWNER_ID, "");

    Execution execution = new Execution();
    execution.setParentId("keep-me");
    engine.rebindSparkTransformOwnerParent(execution);
    assertEquals("keep-me", execution.getParentId());
  }

  /**
   * Documents the bug: {@code resolve(bareName)} returns the name unchanged and must not be used
   * for variable lookup.
   */
  @Test
  void resolveBareName_returnsLiteral_notVariableValue() {
    Variables vars = new Variables();
    vars.initializeFrom(null);
    // Unset: resolve leaves the bare token as-is (no ${} markers)
    assertEquals(
        LocalWorkflowEngine.VAR_SPARK_TRANSFORM_OWNER_ID,
        vars.resolve(LocalWorkflowEngine.VAR_SPARK_TRANSFORM_OWNER_ID));
    // Set: resolve still returns the bare name (it is not a ${var} expression)
    vars.setVariable(LocalWorkflowEngine.VAR_SPARK_TRANSFORM_OWNER_ID, "actual-owner");
    assertEquals(
        LocalWorkflowEngine.VAR_SPARK_TRANSFORM_OWNER_ID,
        vars.resolve(LocalWorkflowEngine.VAR_SPARK_TRANSFORM_OWNER_ID));
    assertEquals(
        "actual-owner", vars.getVariable(LocalWorkflowEngine.VAR_SPARK_TRANSFORM_OWNER_ID));
  }
}
