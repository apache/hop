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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.input.MappingInputMeta;
import org.apache.hop.pipeline.transforms.output.MappingOutputMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CreateMappingFromSelectionTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void threeTransformPathIsExtractedAndReplaced() {
    PipelineMeta parent = pipeline("In", "A", "B", "C", "Out");
    List<TransformMeta> selected = List.of(find(parent, "A"), find(parent, "B"), find(parent, "C"));

    CreateMappingFromSelection.Result result = CreateMappingFromSelection.analyze(parent, selected);
    assertTrue(result.isValid(), result.getValidationError());
    assertEquals("A", result.getEntry().getName());
    assertEquals("C", result.getExit().getName());
    assertEquals("In", result.getIncomingFrom().getName());
    assertEquals("Out", result.getOutgoingTo().getName());
    assertEquals(100, result.getSimpleMappingLocation().x);
    assertEquals(50, result.getSimpleMappingLocation().y);

    PipelineMeta mapping = result.getMappingPipeline();
    assertEquals(5, mapping.nrTransforms());
    assertNotNull(mapping.findTransform("A"));
    assertNotNullInstance(mapping, MappingInputMeta.class);
    assertNotNullInstance(mapping, MappingOutputMeta.class);
    assertEquals("A", mapping.findTransform("A").getName());
    assertEquals("B", mapping.findTransform("B").getName());
    assertEquals("C", mapping.findTransform("C").getName());
    assertEquals(4, mapping.nrPipelineHops());
    assertHop(mapping, "Mapping Input", "A");
    assertHop(mapping, "A", "B");
    assertHop(mapping, "B", "C");
    assertHop(mapping, "C", "Mapping Output");

    TransformMeta clonedA = mapping.findTransform("A");
    assertNotSame(find(parent, "A"), clonedA);
    assertNotSame(find(parent, "A").getTransform(), clonedA.getTransform());

    TransformMeta simpleMapping =
        CreateMappingFromSelection.replaceSelection(parent, result, "${PROJECT_HOME}/map.hpl");
    assertEquals("map", simpleMapping.getName());
    assertEquals(100, simpleMapping.getLocation().x);
    assertEquals(50, simpleMapping.getLocation().y);
    assertTrue(simpleMapping.getTransform() instanceof SimpleMappingMeta);
    assertEquals(
        "${PROJECT_HOME}/map.hpl",
        ((SimpleMappingMeta) simpleMapping.getTransform()).getFilename());
    assertNull(parent.findTransform("A"));
    assertNull(parent.findTransform("B"));
    assertNull(parent.findTransform("C"));
    assertHop(parent, "In", "map");
    assertHop(parent, "map", "Out");
    assertTrue(simpleMapping.isSelected());
    assertFalse(find(parent, "In").isSelected());
  }

  @Test
  void singleTransformWithNeighbors() {
    PipelineMeta parent = pipeline("In", "A", "Out");
    CreateMappingFromSelection.Result result =
        CreateMappingFromSelection.analyze(parent, List.of(find(parent, "A")));
    assertTrue(result.isValid(), result.getValidationError());
    assertEquals("A", result.getEntry().getName());
    assertEquals("A", result.getExit().getName());
    PipelineMeta mapping = result.getMappingPipeline();
    assertEquals(3, mapping.nrTransforms());
    assertHop(mapping, "Mapping Input", "A");
    assertHop(mapping, "A", "Mapping Output");

    CreateMappingFromSelection.replaceSelection(parent, result, "extracted.hpl");
    assertHop(parent, "In", "extracted");
    assertHop(parent, "extracted", "Out");
  }

  @Test
  void isolatedSingleTransform() {
    PipelineMeta parent = new PipelineMeta();
    parent.addTransform(dummy("Solo", 40, 80));
    CreateMappingFromSelection.Result result =
        CreateMappingFromSelection.analyze(parent, List.of(find(parent, "Solo")));
    assertTrue(result.isValid(), result.getValidationError());
    assertNull(result.getIncomingFrom());
    assertNull(result.getOutgoingTo());
    assertEquals(3, result.getMappingPipeline().nrTransforms());

    TransformMeta sm = CreateMappingFromSelection.replaceSelection(parent, result, "solo-map.hpl");
    assertEquals(1, parent.nrTransforms());
    assertEquals(0, parent.nrPipelineHops());
    assertEquals("solo-map", sm.getName());
    assertEquals(40, sm.getLocation().x);
    assertEquals(80, sm.getLocation().y);
  }

  @Test
  void preservesDisabledInternalHop() {
    PipelineMeta parent = pipeline("In", "A", "B", "Out");
    PipelineHopMeta ab = parent.findPipelineHop(find(parent, "A"), find(parent, "B"));
    ab.setEnabled(false);

    CreateMappingFromSelection.Result result =
        CreateMappingFromSelection.analyze(parent, List.of(find(parent, "A"), find(parent, "B")));
    assertTrue(result.isValid(), result.getValidationError());
    PipelineHopMeta cloned =
        result
            .getMappingPipeline()
            .findPipelineHop(
                find(result.getMappingPipeline(), "A"),
                find(result.getMappingPipeline(), "B"),
                true);
    assertFalse(cloned.isEnabled());
  }

  @Test
  void rejectsDisconnectedSelection() {
    PipelineMeta parent = pipeline("A", "B");
    parent.addTransform(dummy("D", 400, 50));
    parent.addTransform(dummy("E", 500, 50));
    parent.addPipelineHop(new PipelineHopMeta(find(parent, "D"), find(parent, "E")));

    CreateMappingFromSelection.Result result =
        CreateMappingFromSelection.analyze(
            parent, List.of(find(parent, "A"), find(parent, "B"), find(parent, "D")));
    assertFalse(result.isValid());
    assertEquals(CreateMappingFromSelection.KEY_NOT_CONNECTED, result.getValidationKey());
  }

  @Test
  void rejectsMissingMiddleTransform() {
    PipelineMeta parent = pipeline("A", "B", "C");
    CreateMappingFromSelection.Result result =
        CreateMappingFromSelection.analyze(parent, List.of(find(parent, "A"), find(parent, "C")));
    assertFalse(result.isValid());
    assertEquals(CreateMappingFromSelection.KEY_NOT_CONNECTED, result.getValidationKey());
  }

  @Test
  void rejectsSplit() {
    PipelineMeta parent = new PipelineMeta();
    parent.addTransform(dummy("A", 0, 50));
    parent.addTransform(dummy("B", 100, 0));
    parent.addTransform(dummy("C", 100, 100));
    parent.addPipelineHop(new PipelineHopMeta(find(parent, "A"), find(parent, "B")));
    parent.addPipelineHop(new PipelineHopMeta(find(parent, "A"), find(parent, "C")));

    CreateMappingFromSelection.Result result =
        CreateMappingFromSelection.analyze(
            parent, List.of(find(parent, "A"), find(parent, "B"), find(parent, "C")));
    assertFalse(result.isValid());
    assertEquals(CreateMappingFromSelection.KEY_NOT_A_PATH, result.getValidationKey());
  }

  @Test
  void rejectsJoin() {
    PipelineMeta parent = new PipelineMeta();
    parent.addTransform(dummy("A", 0, 0));
    parent.addTransform(dummy("B", 0, 100));
    parent.addTransform(dummy("C", 100, 50));
    parent.addPipelineHop(new PipelineHopMeta(find(parent, "A"), find(parent, "C")));
    parent.addPipelineHop(new PipelineHopMeta(find(parent, "B"), find(parent, "C")));

    CreateMappingFromSelection.Result result =
        CreateMappingFromSelection.analyze(
            parent, List.of(find(parent, "A"), find(parent, "B"), find(parent, "C")));
    assertFalse(result.isValid());
    assertEquals(CreateMappingFromSelection.KEY_NOT_A_PATH, result.getValidationKey());
  }

  @Test
  void rejectsTwoExternalInputs() {
    PipelineMeta parent = new PipelineMeta();
    parent.addTransform(dummy("X", 0, 0));
    parent.addTransform(dummy("Y", 0, 100));
    parent.addTransform(dummy("A", 100, 50));
    parent.addTransform(dummy("B", 200, 50));
    parent.addPipelineHop(new PipelineHopMeta(find(parent, "X"), find(parent, "A")));
    parent.addPipelineHop(new PipelineHopMeta(find(parent, "Y"), find(parent, "A")));
    parent.addPipelineHop(new PipelineHopMeta(find(parent, "A"), find(parent, "B")));

    CreateMappingFromSelection.Result result =
        CreateMappingFromSelection.analyze(parent, List.of(find(parent, "A"), find(parent, "B")));
    assertFalse(result.isValid());
    assertEquals(CreateMappingFromSelection.KEY_MULTIPLE_INPUTS, result.getValidationKey());
  }

  @Test
  void rejectsTwoExternalOutputs() {
    PipelineMeta parent = new PipelineMeta();
    parent.addTransform(dummy("A", 0, 50));
    parent.addTransform(dummy("B", 100, 50));
    parent.addTransform(dummy("X", 200, 0));
    parent.addTransform(dummy("Y", 200, 100));
    parent.addPipelineHop(new PipelineHopMeta(find(parent, "A"), find(parent, "B")));
    parent.addPipelineHop(new PipelineHopMeta(find(parent, "B"), find(parent, "X")));
    parent.addPipelineHop(new PipelineHopMeta(find(parent, "B"), find(parent, "Y")));

    CreateMappingFromSelection.Result result =
        CreateMappingFromSelection.analyze(parent, List.of(find(parent, "A"), find(parent, "B")));
    assertFalse(result.isValid());
    assertEquals(CreateMappingFromSelection.KEY_MULTIPLE_OUTPUTS, result.getValidationKey());
  }

  @Test
  void rejectsErrorHopLeavingSelection() {
    PipelineMeta parent = pipeline("A", "B");
    PipelineHopMeta hop = parent.findPipelineHop(find(parent, "A"), find(parent, "B"));
    hop.setErrorHop(true);

    CreateMappingFromSelection.Result result =
        CreateMappingFromSelection.analyze(parent, List.of(find(parent, "A")));
    assertFalse(result.isValid());
    assertEquals(CreateMappingFromSelection.KEY_ERROR_HOP, result.getValidationKey());
  }

  @Test
  void rejectsMappingTransform() {
    PipelineMeta parent = new PipelineMeta();
    SimpleMappingMeta meta = new SimpleMappingMeta();
    meta.setDefault();
    TransformMeta sm = new TransformMeta("SimpleMapping", "SM", meta);
    sm.setLocation(10, 10);
    parent.addTransform(sm);

    CreateMappingFromSelection.Result result =
        CreateMappingFromSelection.analyze(parent, List.of(sm));
    assertFalse(result.isValid());
    assertEquals(CreateMappingFromSelection.KEY_MAPPING_TRANSFORM, result.getValidationKey());
  }

  @Test
  void emptySelectionIsInvalid() {
    CreateMappingFromSelection.Result result =
        CreateMappingFromSelection.analyze(new PipelineMeta(), List.of());
    assertFalse(result.isValid());
    assertEquals(CreateMappingFromSelection.KEY_EMPTY, result.getValidationKey());
  }

  @Test
  void resolveSelectedUsesMultiSelectionWhenContextIsSelected() {
    PipelineMeta parent = pipeline("A", "B", "C");
    find(parent, "A").setSelected(true);
    find(parent, "B").setSelected(true);
    List<TransformMeta> resolved =
        CreateMappingFromSelection.resolveSelectedTransforms(parent, find(parent, "A"));
    assertEquals(2, resolved.size());
  }

  @Test
  void resolveSelectedFallsBackToContextWhenNotInSelection() {
    PipelineMeta parent = pipeline("A", "B");
    find(parent, "A").setSelected(true);
    List<TransformMeta> resolved =
        CreateMappingFromSelection.resolveSelectedTransforms(parent, find(parent, "B"));
    assertEquals(1, resolved.size());
    assertEquals("B", resolved.get(0).getName());
  }

  @Test
  void toProjectRelativePathRewritesUnderHome() {
    Variables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "/data/project");
    assertEquals(
        Const.VAR_PROJECT_HOME + "/mappings/child.hpl",
        CreateMappingFromSelection.toProjectRelativePath(
            "/data/project/mappings/child.hpl", variables));
    assertEquals(
        Const.VAR_PROJECT_HOME,
        CreateMappingFromSelection.toProjectRelativePath("/data/project", variables));
    assertEquals(
        "/elsewhere/file.hpl",
        CreateMappingFromSelection.toProjectRelativePath("/elsewhere/file.hpl", variables));
    assertEquals(
        Const.VAR_PROJECT_HOME + "/already.hpl",
        CreateMappingFromSelection.toProjectRelativePath(
            Const.VAR_PROJECT_HOME + "/already.hpl", variables));
  }

  @Test
  void suggestFilenameUsesParentFolderAndEntryName() {
    PipelineMeta parent = new PipelineMeta();
    parent.setFilename("/data/project/etl/main.hpl");
    TransformMeta entry = dummy("Calc Name", 0, 0);
    String suggested = CreateMappingFromSelection.suggestFilename(parent, entry, new Variables());
    assertTrue(suggested.endsWith("main-Calc-Name-mapping.hpl"), suggested);
    assertTrue(suggested.contains("etl"), suggested);
  }

  @Test
  void suggestFilenameResolvesProjectHomeInParentPath() {
    Variables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "/data/project");
    PipelineMeta parent = new PipelineMeta();
    parent.setFilename("${PROJECT_HOME}/etl/main.hpl");
    TransformMeta entry = dummy("Calculator", 0, 0);
    String suggested = CreateMappingFromSelection.suggestFilename(parent, entry, variables);
    assertFalse(suggested.contains("${"), suggested);
    assertTrue(suggested.contains("/data/project/etl"), suggested);
    assertTrue(suggested.endsWith("main-Calculator-mapping.hpl"), suggested);
  }

  @Test
  void transformNameFromFilenameUsesBaseName() {
    assertEquals(
        "map", CreateMappingFromSelection.transformNameFromFilename("${PROJECT_HOME}/map.hpl"));
    assertEquals(
        "child", CreateMappingFromSelection.transformNameFromFilename("/data/project/child.hpl"));
    assertEquals("Simple mapping", CreateMappingFromSelection.transformNameFromFilename(""));
  }

  @Test
  void resolveFilesystemPathExpandsVariables() {
    Variables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "/data/project");
    assertEquals(
        "/data/project/map.hpl",
        CreateMappingFromSelection.resolveFilesystemPath("${PROJECT_HOME}/map.hpl", variables));
    assertNull(
        CreateMappingFromSelection.resolveFilesystemPath(
            "${PROJECT_HOME}/map.hpl", new Variables()));
    assertEquals(
        "/tmp/a.hpl", CreateMappingFromSelection.resolveFilesystemPath("/tmp/a.hpl", variables));
  }

  @Test
  void copiesNamedParametersToMappingPipeline() throws Exception {
    PipelineMeta parent = pipeline("A");
    parent.addParameterDefinition("ENV", "dev", "environment");
    CreateMappingFromSelection.Result result =
        CreateMappingFromSelection.analyze(parent, List.of(find(parent, "A")));
    assertTrue(result.isValid(), result.getValidationError());
    assertEquals("dev", result.getMappingPipeline().getParameterDefault("ENV"));
  }

  @Test
  void mappingInputIsClampedOffTheLeftEdge() {
    PipelineMeta parent = new PipelineMeta();
    parent.addTransform(dummy("A", 10, 20));
    CreateMappingFromSelection.Result result =
        CreateMappingFromSelection.analyze(parent, List.of(find(parent, "A")));
    TransformMeta mappingInput = result.getMappingPipeline().findTransform("Mapping Input");
    assertEquals(0, mappingInput.getLocation().x);
    assertEquals(20, mappingInput.getLocation().y);
  }

  private static PipelineMeta pipeline(String... names) {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta previous = null;
    int x = 0;
    for (String name : names) {
      TransformMeta transformMeta = dummy(name, x, 50);
      pipelineMeta.addTransform(transformMeta);
      if (previous != null) {
        pipelineMeta.addPipelineHop(new PipelineHopMeta(previous, transformMeta));
      }
      previous = transformMeta;
      x += 100;
    }
    return pipelineMeta;
  }

  private static TransformMeta dummy(String name, int x, int y) {
    TransformMeta transformMeta = new TransformMeta("Dummy", name, new DummyMeta());
    transformMeta.setLocation(new Point(x, y));
    return transformMeta;
  }

  private static TransformMeta find(PipelineMeta pipelineMeta, String name) {
    return pipelineMeta.findTransform(name);
  }

  private static void assertHop(PipelineMeta pipelineMeta, String from, String to) {
    assertTrue(
        pipelineMeta.findPipelineHop(
                pipelineMeta.findTransform(from), pipelineMeta.findTransform(to), true)
            != null,
        "expected hop " + from + " -> " + to);
  }

  private static void assertNotNullInstance(PipelineMeta pipelineMeta, Class<?> type) {
    boolean found = false;
    for (TransformMeta transformMeta : pipelineMeta.getTransforms()) {
      if (type.isInstance(transformMeta.getTransform())) {
        found = true;
        break;
      }
    }
    assertTrue(found, "expected a transform of type " + type.getSimpleName());
  }
}
