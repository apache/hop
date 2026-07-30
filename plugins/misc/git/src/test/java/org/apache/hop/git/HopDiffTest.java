/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.git;

import static org.apache.hop.git.HopDiff.ADDED;
import static org.apache.hop.git.HopDiff.ATTR_GIT;
import static org.apache.hop.git.HopDiff.ATTR_STATUS;
import static org.apache.hop.git.HopDiff.CHANGED;
import static org.apache.hop.git.HopDiff.REMOVED;
import static org.apache.hop.git.HopDiff.UNCHANGED;
import static org.apache.hop.git.HopDiff.compareActions;
import static org.apache.hop.git.HopDiff.compareTransforms;
import static org.apache.hop.git.HopDiff.getPipelineHopName;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.calculator.CalculatorMeta;
import org.apache.hop.pipeline.transforms.checksum.CheckSumMeta;
import org.apache.hop.pipeline.transforms.csvinput.CsvInputMeta;
import org.apache.hop.pipeline.transforms.selectvalues.SelectValuesMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.actions.pipeline.ActionPipeline;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.apache.hop.workflow.actions.workflow.ActionWorkflow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HopDiffTest {
  IHopMetadataProvider metadataProvider;

  @BeforeEach
  void setUp() throws HopException {
    HopClientEnvironment.getInstance().setClient(HopClientEnvironment.ClientType.OTHER);
    PluginRegistry.getInstance()
        .registerPluginClass(
            CsvInputMeta.class.getName(), TransformPluginType.class, Transform.class);
    PluginRegistry.getInstance()
        .registerPluginClass(
            SelectValuesMeta.class.getName(), TransformPluginType.class, Transform.class);
    PluginRegistry.getInstance()
        .registerPluginClass(
            CalculatorMeta.class.getName(), TransformPluginType.class, Transform.class);
    PluginRegistry.getInstance()
        .registerPluginClass(
            CheckSumMeta.class.getName(), TransformPluginType.class, Transform.class);

    PluginRegistry.getInstance()
        .registerPluginClass(ActionStart.class.getName(), ActionPluginType.class, Action.class);
    PluginRegistry.getInstance()
        .registerPluginClass(ActionWorkflow.class.getName(), ActionPluginType.class, Action.class);
    PluginRegistry.getInstance()
        .registerPluginClass(ActionPipeline.class.getName(), ActionPluginType.class, Action.class);

    metadataProvider = new MemoryMetadataProvider();
  }

  private PipelineMeta loadPipeline(String name) throws Exception {
    try (InputStream xmlStream = new FileInputStream(new File("src/test/resources/" + name))) {
      return new PipelineMeta(xmlStream, metadataProvider, Variables.getADefaultVariableSpace());
    }
  }

  private WorkflowMeta loadWorkflow(String name) throws Exception {
    try (InputStream xmlStream = new FileInputStream(new File("src/test/resources/" + name))) {
      return new WorkflowMeta(xmlStream, metadataProvider, new Variables());
    }
  }

  @Test
  void diffPipelineTest() throws Exception {
    PipelineMeta pipelineMeta1 = loadPipeline("r1.hpl");
    PipelineMeta pipelineMeta2 = loadPipeline("r2.hpl");

    PipelineMeta resultForward = compareTransforms(pipelineMeta1, pipelineMeta2, true, false);
    PipelineMeta resultBackward = compareTransforms(pipelineMeta2, pipelineMeta1, false, false);
    assertEquals(CHANGED, resultForward.getTransform(0).getAttribute(ATTR_GIT, ATTR_STATUS));
    assertEquals(UNCHANGED, resultForward.getTransform(1).getAttribute(ATTR_GIT, ATTR_STATUS));
    assertEquals(REMOVED, resultForward.getTransform(2).getAttribute(ATTR_GIT, ATTR_STATUS));
    assertEquals(ADDED, resultBackward.getTransform(2).getAttribute(ATTR_GIT, ATTR_STATUS));
  }

  /** The same pipeline with one transform dragged elsewhere, and nothing else touched. */
  private PipelineMeta movedPipeline() throws Exception {
    PipelineMeta pipelineMeta = loadPipeline("r1.hpl");
    TransformMeta transform = pipelineMeta.getTransform(0);
    transform.setLocation(transform.getLocation().x + 64, transform.getLocation().y + 32);
    return pipelineMeta;
  }

  @Test
  void movedTransformIsChangedWithoutTheOption() throws Exception {
    PipelineMeta result = compareTransforms(loadPipeline("r1.hpl"), movedPipeline(), true, false);

    assertEquals(CHANGED, result.getTransform(0).getAttribute(ATTR_GIT, ATTR_STATUS));
  }

  @Test
  void movedTransformIsUnchangedWithTheOption() throws Exception {
    PipelineMeta result = compareTransforms(loadPipeline("r1.hpl"), movedPipeline(), true, true);

    assertEquals(UNCHANGED, result.getTransform(0).getAttribute(ATTR_GIT, ATTR_STATUS));
  }

  @Test
  void diffWorkflowTest() throws Exception {
    WorkflowMeta jobMeta = loadWorkflow("r1.hwf");
    WorkflowMeta jobMeta2 = loadWorkflow("r2.hwf");

    jobMeta = compareActions(jobMeta, jobMeta2, true, false);
    jobMeta2 = compareActions(jobMeta2, jobMeta, false, false);
    assertEquals(CHANGED, jobMeta.getAction(0).getAttribute(ATTR_GIT, ATTR_STATUS));
    assertEquals(UNCHANGED, jobMeta.getAction(1).getAttribute(ATTR_GIT, ATTR_STATUS));
    assertEquals(REMOVED, jobMeta.getAction(2).getAttribute(ATTR_GIT, ATTR_STATUS));
    assertEquals(ADDED, jobMeta2.getAction(2).getAttribute(ATTR_GIT, ATTR_STATUS));
  }

  /** START only moved between r1 and r2, so it is unchanged once the position is left out. */
  @Test
  void diffWorkflowIgnoringPositionTest() throws Exception {
    WorkflowMeta jobMeta = loadWorkflow("r1.hwf");
    WorkflowMeta jobMeta2 = loadWorkflow("r2.hwf");

    jobMeta = compareActions(jobMeta, jobMeta2, true, true);
    jobMeta2 = compareActions(jobMeta2, jobMeta, false, true);
    assertEquals("START", jobMeta.getAction(0).getName());
    assertEquals(UNCHANGED, jobMeta.getAction(0).getAttribute(ATTR_GIT, ATTR_STATUS));
    assertEquals(UNCHANGED, jobMeta.getAction(1).getAttribute(ATTR_GIT, ATTR_STATUS));
    assertEquals(REMOVED, jobMeta.getAction(2).getAttribute(ATTR_GIT, ATTR_STATUS));
    assertEquals(ADDED, jobMeta2.getAction(2).getAttribute(ATTR_GIT, ATTR_STATUS));
  }

  /** The name has to name both ends, otherwise hops sharing a source transform collide. */
  @Test
  void pipelineHopNameTest() throws Exception {
    PipelineMeta pipelineMeta = loadPipeline("r1.hpl");

    assertEquals(
        "CSV file input - Add a checksum", getPipelineHopName(pipelineMeta.getPipelineHop(0)));
  }
}
