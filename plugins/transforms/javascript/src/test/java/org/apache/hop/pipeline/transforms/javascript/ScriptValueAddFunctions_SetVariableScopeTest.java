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

package org.apache.hop.pipeline.transforms.javascript;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class ScriptValueAddFunctions_SetVariableScopeTest {
  private static final String VARIABLE_NAME = "variable-name";
  private static final String VARIABLE_VALUE = "variable-value";
  protected ILogChannel log = new LogChannel("junit");

  @Test
  void setParentScopeVariable_ParentIsPipeline() {
    Pipeline parent = createPipeline();
    Pipeline child = createPipeline(parent);

    ScriptValuesAddedFunctions.setParentScopeVariable(child, VARIABLE_NAME, VARIABLE_VALUE);

    verify(child).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
    verify(parent).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
  }

  @Test
  void setParentScopeVariable_ParentIsJob() {
    Workflow parent = createWorkflow();
    Pipeline child = createPipeline(parent);

    ScriptValuesAddedFunctions.setParentScopeVariable(child, VARIABLE_NAME, VARIABLE_VALUE);

    verify(child).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
    verify(parent).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
  }

  @Test
  void setParentScopeVariable_NoParent() {
    Pipeline pipeline = createPipeline();

    ScriptValuesAddedFunctions.setParentScopeVariable(pipeline, VARIABLE_NAME, VARIABLE_VALUE);

    verify(pipeline).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
  }

  @Test
  void setGrandParentScopeVariable_TwoLevelHierarchy() {
    Pipeline parent = createPipeline();
    Pipeline child = createPipeline(parent);

    ScriptValuesAddedFunctions.setGrandParentScopeVariable(child, VARIABLE_NAME, VARIABLE_VALUE);

    verify(child).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
    verify(parent).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
  }

  @Test
  void setGrandParentScopeVariable_ThreeLevelHierarchy() {
    Workflow grandParent = createWorkflow();
    Pipeline parent = createPipeline(grandParent);
    Pipeline child = createPipeline(parent);

    ScriptValuesAddedFunctions.setGrandParentScopeVariable(child, VARIABLE_NAME, VARIABLE_VALUE);

    verify(child).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
    verify(parent).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
    verify(grandParent).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
  }

  @Test
  void setGrandParentScopeVariable_FourLevelHierarchy() {
    Workflow grandGrandParent = createWorkflow();
    Pipeline grandParent = createPipeline(grandGrandParent);
    Pipeline parent = createPipeline(grandParent);
    Pipeline child = createPipeline(parent);

    ScriptValuesAddedFunctions.setGrandParentScopeVariable(child, VARIABLE_NAME, VARIABLE_VALUE);

    verify(child).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
    verify(parent).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
    verify(grandParent).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
    verify(grandGrandParent, never()).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
  }

  @Test
  void setGrandParentScopeVariable_NoParent() {
    Pipeline pipeline = createPipeline();

    ScriptValuesAddedFunctions.setGrandParentScopeVariable(pipeline, VARIABLE_NAME, VARIABLE_VALUE);

    verify(pipeline).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
  }

  @Test
  @Disabled("This test needs to be reviewed")
  void setRootScopeVariable_TwoLevelHierarchy() {
    Pipeline parent = createPipeline();
    Pipeline child = createPipeline(parent);

    ScriptValuesAddedFunctions.setRootScopeVariable(child, VARIABLE_NAME, VARIABLE_VALUE);

    verify(child).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
    verify(parent).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
  }

  @Test
  void setRootScopeVariable_FourLevelHierarchy() {
    Workflow grandGrandParent = createWorkflow();
    Pipeline grandParent = createPipeline(grandGrandParent);
    Pipeline parent = createPipeline(grandParent);
    Pipeline child = createPipeline(parent);

    ScriptValuesAddedFunctions.setRootScopeVariable(child, VARIABLE_NAME, VARIABLE_VALUE);

    verify(child).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
    verify(parent).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
    verify(grandParent).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
    verify(grandGrandParent).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
  }

  @Test
  void setRootScopeVariable_NoParent() {
    Pipeline pipeline = createPipeline();

    ScriptValuesAddedFunctions.setRootScopeVariable(pipeline, VARIABLE_NAME, VARIABLE_VALUE);

    verify(pipeline).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
  }

  @Test
  void setSystemScopeVariable_NoParent() {
    Pipeline pipeline = createPipeline();

    assertNull(System.getProperty(VARIABLE_NAME));

    try {
      ScriptValuesAddedFunctions.setSystemScopeVariable(pipeline, VARIABLE_NAME, VARIABLE_VALUE);

      assertEquals(VARIABLE_VALUE, System.getProperty(VARIABLE_NAME));
      verify(pipeline).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
    } finally {
      System.clearProperty(VARIABLE_NAME);
    }
  }

  @Test
  void setSystemScopeVariable_FourLevelHierarchy() {
    Workflow grandGrandParent = createWorkflow();
    Pipeline grandParent = createPipeline(grandGrandParent);
    Pipeline parent = createPipeline(grandParent);
    Pipeline child = createPipeline(parent);

    assertNull(System.getProperty(VARIABLE_NAME));

    try {
      ScriptValuesAddedFunctions.setSystemScopeVariable(child, VARIABLE_NAME, VARIABLE_VALUE);

      assertEquals(VARIABLE_VALUE, System.getProperty(VARIABLE_NAME));

      verify(child).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
      verify(parent).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
      verify(grandParent).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
      verify(grandGrandParent).setVariable(VARIABLE_NAME, VARIABLE_VALUE);
    } finally {
      System.clearProperty(VARIABLE_NAME);
    }
  }

  private Pipeline createPipeline(Pipeline parent) {
    Pipeline pipeline = createPipeline();

    pipeline.setParent(parent);
    pipeline.setParentVariables(parent);

    return pipeline;
  }

  private Pipeline createPipeline(Workflow parent) {
    Pipeline pipeline = createPipeline();

    pipeline.setParentWorkflow(parent);
    pipeline.setParentVariables(parent);

    return pipeline;
  }

  private Pipeline createPipeline() {
    Pipeline pipeline = new LocalPipelineEngine();
    pipeline.setLogChannel(log);

    pipeline = spy(pipeline);

    return pipeline;
  }

  private Workflow createWorkflow() {
    Workflow workflow = new LocalWorkflowEngine();
    workflow = spy(workflow);

    return workflow;
  }
}
