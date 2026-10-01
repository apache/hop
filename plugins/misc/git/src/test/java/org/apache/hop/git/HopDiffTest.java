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
import static org.apache.hop.git.HopDiff.ATTR_GIT_HOPS;
import static org.apache.hop.git.HopDiff.ATTR_STATUS;
import static org.apache.hop.git.HopDiff.CHANGED;
import static org.apache.hop.git.HopDiff.REMOVED;
import static org.apache.hop.git.HopDiff.UNCHANGED;
import static org.apache.hop.git.HopDiff.compareActions;
import static org.apache.hop.git.HopDiff.comparePipelineHops;
import static org.apache.hop.git.HopDiff.compareTransforms;
import static org.apache.hop.git.HopDiff.detectActionRenames;
import static org.apache.hop.git.HopDiff.detectTransformRenames;
import static org.apache.hop.git.HopDiff.getPipelineHopName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
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
  private static final Map<String, String> NO_RENAMES = Map.of();

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

    PipelineMeta resultForward =
        compareTransforms(pipelineMeta1, pipelineMeta2, true, false, NO_RENAMES);
    PipelineMeta resultBackward =
        compareTransforms(pipelineMeta2, pipelineMeta1, false, false, NO_RENAMES);
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
    PipelineMeta result =
        compareTransforms(loadPipeline("r1.hpl"), movedPipeline(), true, false, NO_RENAMES);

    assertEquals(CHANGED, result.getTransform(0).getAttribute(ATTR_GIT, ATTR_STATUS));
  }

  @Test
  void movedTransformIsUnchangedWithTheOption() throws Exception {
    PipelineMeta result =
        compareTransforms(loadPipeline("r1.hpl"), movedPipeline(), true, true, NO_RENAMES);

    assertEquals(UNCHANGED, result.getTransform(0).getAttribute(ATTR_GIT, ATTR_STATUS));
  }

  @Test
  void diffWorkflowTest() throws Exception {
    WorkflowMeta jobMeta = loadWorkflow("r1.hwf");
    WorkflowMeta jobMeta2 = loadWorkflow("r2.hwf");

    jobMeta = compareActions(jobMeta, jobMeta2, true, false, NO_RENAMES);
    jobMeta2 = compareActions(jobMeta2, jobMeta, false, false, NO_RENAMES);
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

    jobMeta = compareActions(jobMeta, jobMeta2, true, true, NO_RENAMES);
    jobMeta2 = compareActions(jobMeta2, jobMeta, false, true, NO_RENAMES);
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

  /** r1 with "Add a checksum" renamed, and nothing else touched. */
  private PipelineMeta renamedPipeline() throws Exception {
    PipelineMeta pipelineMeta = loadPipeline("r1.hpl");
    pipelineMeta.getTransform(1).setName("checksum renamed");
    return pipelineMeta;
  }

  @Test
  void renameIsDetected() throws Exception {
    Map<String, String> renames = detectTransformRenames(loadPipeline("r1.hpl"), renamedPipeline());

    assertEquals(Map.of("Add a checksum", "checksum renamed"), renames);
  }

  @Test
  void renamedTransformIsChangedRatherThanAddedAndRemoved() throws Exception {
    PipelineMeta original = loadPipeline("r1.hpl");
    PipelineMeta renamed = renamedPipeline();
    Map<String, String> renames = detectTransformRenames(original, renamed);
    Map<String, String> back = detectTransformRenames(renamed, original);

    PipelineMeta forward = compareTransforms(original, renamed, true, true, renames);
    PipelineMeta backward = compareTransforms(renamed, original, false, true, back);

    assertEquals(CHANGED, forward.getTransform(1).getAttribute(ATTR_GIT, ATTR_STATUS));
    assertEquals(CHANGED, backward.getTransform(1).getAttribute(ATTR_GIT, ATTR_STATUS));
    // What sits around it is untouched.
    assertEquals(UNCHANGED, forward.getTransform(0).getAttribute(ATTR_GIT, ATTR_STATUS));
    assertEquals(UNCHANGED, forward.getTransform(2).getAttribute(ATTR_GIT, ATTR_STATUS));
  }

  /** Both hops touch the renamed transform, and neither of them actually changed. */
  @Test
  void hopsOfARenamedTransformAreNotReported() throws Exception {
    PipelineMeta original = loadPipeline("r1.hpl");
    PipelineMeta renamed = renamedPipeline();
    Map<String, String> renames = detectTransformRenames(original, renamed);

    PipelineMeta forward = comparePipelineHops(original, renamed, true, renames);

    assertNull(forward.getAttribute(ATTR_GIT_HOPS, "CSV file input - Add a checksum"));
    assertNull(forward.getAttribute(ATTR_GIT_HOPS, "Add a checksum - Calculator"));
  }

  @Test
  void renameIsStillDetectedWhenTheTransformAlsoMoved() throws Exception {
    PipelineMeta renamed = renamedPipeline();
    TransformMeta transform = renamed.getTransform(1);
    transform.setLocation(transform.getLocation().x + 80, transform.getLocation().y + 40);

    Map<String, String> renames = detectTransformRenames(loadPipeline("r1.hpl"), renamed);

    assertEquals(Map.of("Add a checksum", "checksum renamed"), renames);
  }

  @Test
  void severalRenamesInOneCommitAreEachPairedUp() throws Exception {
    PipelineMeta renamed = loadPipeline("r1.hpl");
    renamed.getTransform(1).setName("checksum renamed");
    renamed.getTransform(2).setName("calculator renamed");

    Map<String, String> renames = detectTransformRenames(loadPipeline("r1.hpl"), renamed);

    assertEquals(
        Map.of("Add a checksum", "checksum renamed", "Calculator", "calculator renamed"), renames);
  }

  /** r1 drops Calculator and r2 adds Select values. Unrelated, so not a rename. */
  @Test
  void aRealRemovalIsNotReadAsARename() throws Exception {
    Map<String, String> renames =
        detectTransformRenames(loadPipeline("r1.hpl"), loadPipeline("r2.hpl"));

    assertEquals(Map.of(), renames);
  }

  @Test
  void renamedActionIsChangedRatherThanAddedAndRemoved() throws Exception {
    WorkflowMeta original = loadWorkflow("r1.hwf");
    WorkflowMeta renamed = loadWorkflow("r1.hwf");
    renamed.getAction(1).setName("workflow renamed");

    Map<String, String> renames = detectActionRenames(original, renamed);
    WorkflowMeta forward = compareActions(original, renamed, true, true, renames);

    assertEquals(Map.of("Workflow", "workflow renamed"), renames);
    assertEquals(CHANGED, forward.getAction(1).getAttribute(ATTR_GIT, ATTR_STATUS));
  }
}
