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
package org.apache.hop.pipeline.transforms.multimapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MultiMappingMetaTest {

  @BeforeEach
  void setUp() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void testSerialization() throws Exception {
    MultiMappingMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/multi-mapping-transform.xml", MultiMappingMeta.class);
    assertEquals("${PROJECT_HOME}/multi-mapping-child.hpl", meta.getFilename());
    assertEquals("local", meta.getRunConfigurationName());
    assertEquals(2, meta.getInputMappings().size());
    assertEquals(2, meta.getOutputMappings().size());
    assertFalse(meta.getInputMappings().get(0).isMainDataPath());
    assertEquals("Lookup source", meta.getInputMappings().get(0).getInputTransformName());
    assertTrue(meta.getInputMappings().get(1).isMainDataPath());
    assertTrue(meta.getInputMappings().get(1).isRenamingOnOutput());
    assertEquals("Handle A", meta.getOutputMappings().get(1).getOutputTransformName());
    assertTrue(meta.getMappingParameters().isInheritingAllVariables());
    assertEquals(1, meta.getMappingParameters().getVariableMappings().size());
    assertEquals("PARAM_X", meta.getMappingParameters().getVariableMappings().get(0).getName());
  }

  @Test
  void testCloneAndDefault() {
    MultiMappingMeta meta = new MultiMappingMeta();
    meta.setDefault();
    assertEquals(1, meta.getInputMappings().size());
    assertEquals(1, meta.getOutputMappings().size());
    assertTrue(meta.getInputMappings().get(0).isMainDataPath());
    assertTrue(meta.getInputMappings().get(0).isRenamingOnOutput());

    MultiMappingMeta copy = (MultiMappingMeta) meta.clone();
    assertEquals(1, copy.getInputMappings().size());
    copy.getInputMappings().get(0).setInputTransformName("changed");
    assertTrue(
        meta.getInputMappings().get(0).getInputTransformName() == null
            || !meta.getInputMappings().get(0).getInputTransformName().equals("changed"));
  }

  @Test
  void testTransformIoMetaExposesInfoAndTargetStreams() {
    MultiMappingMeta meta = new MultiMappingMeta();
    meta.setDefault();

    MultiMappingInputDefinition info = new MultiMappingInputDefinition("Lookup", "Lookup input");
    info.setMainDataPath(false);
    meta.getInputMappings().add(info);

    MultiMappingOutputDefinition target = new MultiMappingOutputDefinition("Case out", "Handle A");
    target.setMainDataPath(false);
    meta.getOutputMappings().add(target);
    meta.resetTransformIoMeta();

    List<IStream> infoStreams = meta.getTransformIOMeta().getInfoStreams();
    List<IStream> targetStreams = meta.getTransformIOMeta().getTargetStreams();
    assertEquals(1, infoStreams.size());
    assertEquals(StreamType.INFO, infoStreams.get(0).getStreamType());
    assertEquals(1, targetStreams.size());
    assertEquals(StreamType.TARGET, targetStreams.get(0).getStreamType());
    assertNotNull(meta.getInfoTransforms());
    assertEquals("Lookup", meta.getInfoTransforms()[0]);
    assertNotNull(meta.getTargetTransforms());
    assertEquals("Handle A", meta.getTargetTransforms()[0]);
  }

  @Test
  void testSearchInfoAndTargetTransforms() {
    MultiMappingMeta meta = new MultiMappingMeta();
    MultiMappingInputDefinition info = new MultiMappingInputDefinition("Lookup", "Lookup input");
    info.setMainDataPath(false);
    meta.getInputMappings().add(info);
    MultiMappingOutputDefinition target = new MultiMappingOutputDefinition("Case out", "Handle A");
    target.setMainDataPath(false);
    meta.getOutputMappings().add(target);
    meta.resetTransformIoMeta();

    TransformMeta lookup = new TransformMeta();
    lookup.setName("Lookup");
    TransformMeta handleA = new TransformMeta();
    handleA.setName("Handle A");
    meta.searchInfoAndTargetTransforms(List.of(lookup, handleA));
    assertEquals(lookup, info.getInputTransform());
    assertEquals(lookup, meta.getTransformIOMeta().getInfoStreams().get(0).getTransformMeta());
    assertEquals(handleA, meta.getTransformIOMeta().getTargetStreams().get(0).getTransformMeta());
  }

  @Test
  void testCheckWithoutFilenameIsError() {
    MultiMappingMeta meta = new MultiMappingMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        new PipelineMeta(),
        new TransformMeta(),
        new RowMeta(),
        new String[0],
        new String[0],
        new RowMeta(),
        new Variables(),
        new MemoryMetadataProvider());
    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  void testCheckZeroInputIsWarningNotError() {
    MultiMappingMeta meta = new MultiMappingMeta();
    meta.setFilename("dummy.hpl");
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        new PipelineMeta(),
        new TransformMeta(),
        new RowMeta(),
        new String[0],
        new String[0],
        new RowMeta(),
        new Variables(),
        new MemoryMetadataProvider());
    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_WARNING
                        && r.getText().contains("No input")));
    assertFalse(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText().contains("No input")));
  }

  @Test
  void testFindOutputDefinitionPrefersNamedTarget() {
    MultiMappingMeta meta = new MultiMappingMeta();
    MultiMappingOutputDefinition main = new MultiMappingOutputDefinition("Main out", null);
    main.setMainDataPath(true);
    MultiMappingOutputDefinition target = new MultiMappingOutputDefinition("Case out", "Handle A");
    target.setMainDataPath(false);
    meta.getOutputMappings().add(main);
    meta.getOutputMappings().add(target);

    TransformMeta next = new TransformMeta();
    next.setName("Handle A");
    assertEquals(target, meta.findOutputDefinition(next));
    assertEquals(main, meta.findOutputDefinition(null));
  }

  @Test
  void testOptionalStreamsAddDefinitions() {
    MultiMappingMeta meta = new MultiMappingMeta();
    List<IStream> optional = meta.getOptionalStreams();
    assertEquals(2, optional.size());

    TransformMeta lookup = new TransformMeta();
    lookup.setName("Lookup");
    optional.get(0).setTransformMeta(lookup);
    meta.handleStreamSelection(optional.get(0));
    assertEquals(1, meta.getInputMappings().size());
    assertFalse(meta.getInputMappings().get(0).isMainDataPath());
    assertEquals("Lookup", meta.getInputMappings().get(0).getInputTransformName());

    TransformMeta handleA = new TransformMeta();
    handleA.setName("Handle A");
    optional.get(1).setTransformMeta(handleA);
    meta.handleStreamSelection(optional.get(1));
    assertEquals(1, meta.getOutputMappings().size());
    assertEquals("Handle A", meta.getOutputMappings().get(0).getOutputTransformName());
  }

  @Test
  void testCleanAfterHopRemove() {
    MultiMappingMeta meta = new MultiMappingMeta();
    MultiMappingInputDefinition info = new MultiMappingInputDefinition("Lookup", "Lookup input");
    info.setMainDataPath(false);
    meta.getInputMappings().add(info);
    MultiMappingOutputDefinition target = new MultiMappingOutputDefinition("Case out", "Handle A");
    target.setMainDataPath(false);
    meta.getOutputMappings().add(target);

    TransformMeta lookup = new TransformMeta();
    lookup.setName("Lookup");
    assertTrue(meta.cleanAfterHopToRemove(lookup));
    assertTrue(
        meta.getInputMappings().get(0).getInputTransformName() == null
            || meta.getInputMappings().get(0).getInputTransformName().isEmpty());

    TransformMeta handleA = new TransformMeta();
    handleA.setName("Handle A");
    assertTrue(meta.cleanAfterHopFromRemove(handleA));
    assertTrue(
        meta.getOutputMappings().get(0).getOutputTransformName() == null
            || meta.getOutputMappings().get(0).getOutputTransformName().isEmpty());
  }
}
