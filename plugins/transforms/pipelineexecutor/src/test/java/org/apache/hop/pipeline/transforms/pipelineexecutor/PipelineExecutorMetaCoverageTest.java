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

package org.apache.hop.pipeline.transforms.pipelineexecutor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Broader coverage for {@link PipelineExecutorMeta} behaviour beyond serialization. */
class PipelineExecutorMetaCoverageTest {

  @RegisterExtension
  static final RestoreHopEngineEnvironmentExtension env =
      new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void initHop() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void searchInfoAndTargetTransformsResolvesOptionalTargets() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();

    TransformMeta execResult = new TransformMeta("t_exec", mock(ITransformMeta.class));
    TransformMeta rowsOut = new TransformMeta("t_rows", mock(ITransformMeta.class));
    TransformMeta filesOut = new TransformMeta("t_files", mock(ITransformMeta.class));
    TransformMeta copyOut = new TransformMeta("t_copy", mock(ITransformMeta.class));
    List<TransformMeta> transforms =
        new ArrayList<>(List.of(execResult, rowsOut, filesOut, copyOut));

    meta.setExecutionResultTargetTransform("t_exec");
    meta.setOutputRowsSourceTransform("t_rows");
    meta.setResultFilesTargetTransform("t_files");
    meta.setExecutorsOutputTransform("t_copy");

    meta.searchInfoAndTargetTransforms(transforms);

    assertEquals(execResult, meta.getExecutionResultTargetTransformMeta());
    assertEquals(rowsOut, meta.getOutputRowsSourceTransformMeta());
    assertEquals(filesOut, meta.getResultFilesTargetTransformMeta());
    assertEquals(copyOut, meta.getExecutorsOutputTransformMeta());
  }

  @Test
  void cleanAfterHopFromRemoveClearsAllOptionalTargets() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    TransformMeta dummy = new TransformMeta("x", mock(ITransformMeta.class));
    meta.setExecutionResultTargetTransformMeta(dummy);
    meta.setExecutionResultTargetTransform("x");
    meta.setOutputRowsSourceTransformMeta(dummy);
    meta.setOutputRowsSourceTransform("x");

    assertTrue(meta.cleanAfterHopFromRemove());

    assertNull(meta.getExecutionResultTargetTransformMeta());
    assertNull(meta.getExecutionResultTargetTransform());
    assertNull(meta.getOutputRowsSourceTransformMeta());
    assertNull(meta.getOutputRowsSourceTransform());
  }

  @Test
  void cleanAfterHopFromRemoveByTransformClearsMatchingTarget() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    TransformMeta toClear = new TransformMeta("gone", mock(ITransformMeta.class));
    meta.setResultFilesTargetTransformMeta(toClear);
    meta.setResultFilesTargetTransform("gone");

    assertTrue(meta.cleanAfterHopFromRemove(toClear));
    assertNull(meta.getResultFilesTargetTransformMeta());
    assertNull(meta.getResultFilesTargetTransform());
  }

  @Test
  void cleanAfterHopFromRemoveByTransformReturnsFalseForUnknown() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    assertFalse(
        meta.cleanAfterHopFromRemove(new TransformMeta("other", mock(ITransformMeta.class))));
    assertFalse(meta.cleanAfterHopFromRemove(null));
  }

  @Test
  void getSupportedPipelineTypesReturnsNormal() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    assertArrayEquals(new PipelineType[] {PipelineType.Normal}, meta.getSupportedPipelineTypes());
  }

  @Test
  void flagsAndCapabilities() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    assertTrue(meta.excludeFromCopyDistributeVerification());
    assertTrue(meta.supportsDrillDown());
  }

  @Test
  void getReferencedObjectDescriptionsAndEnabledFlags() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setFilename("");
    assertEquals(1, meta.getReferencedObjectDescriptions().length);
    assertFalse(meta.isReferencedObjectEnabled()[0]);

    meta.setFilename("/some/path/child.hpl");
    assertTrue(meta.isReferencedObjectEnabled()[0]);
  }

  @Test
  void getResourceDependenciesIncludesResolvedFilename() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setFilename("${MY_CHILD}");
    TransformMeta self = new TransformMeta("pe", meta);
    IVariables variables = new org.apache.hop.core.variables.Variables();
    variables.setVariable("MY_CHILD", "/tmp/child.hpl");

    List<ResourceReference> refs = meta.getResourceDependencies(variables, self);
    assertEquals(1, refs.size());
    List<ResourceEntry> entries = new ArrayList<>(refs.get(0).getEntries());
    assertFalse(entries.isEmpty());
    assertEquals("/tmp/child.hpl", entries.get(0).getResource());
  }

  @Test
  void getInfoTransformsReturnsNullWhenEmpty() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    assertNull(meta.getInfoTransforms());
  }

  @Test
  void getTransformIOMetaCreatesOptionalTargetStreams() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    assertNotNull(meta.getTransformIOMeta());
    assertTrue(meta.getTransformIOMeta().getTargetStreams().size() >= 4);
  }

  @Test
  void checkAddsWarningWhenNoInputFields() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    TransformMeta self = new TransformMeta("pe", meta);
    PipelineMeta pipelineMeta = new PipelineMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    IRowMeta emptyPrev = new RowMeta();

    meta.check(
        remarks,
        pipelineMeta,
        self,
        emptyPrev,
        new String[] {"upstream"},
        new String[] {},
        null,
        new org.apache.hop.core.variables.Variables(),
        new MemoryMetadataProvider());

    assertTrue(
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_WARNING),
        "expected warning when previous transform sends no fields");
    assertTrue(
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_OK),
        "expected OK when input hops exist");
  }

  @Test
  void checkAddsErrorWhenNoInputTransforms() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    TransformMeta self = new TransformMeta("pe", meta);
    PipelineMeta pipelineMeta = new PipelineMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    RowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("x"));

    meta.check(
        remarks,
        pipelineMeta,
        self,
        prev,
        new String[] {},
        new String[] {},
        null,
        new org.apache.hop.core.variables.Variables(),
        new MemoryMetadataProvider());

    assertTrue(
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR),
        "expected error when no input hops");
  }

  @Test
  void getFieldsClearsAndBuildsExecutionResultRow() throws HopTransformException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    TransformMeta parent = new TransformMeta("parent", meta);
    meta.setParentTransformMeta(parent);

    TransformMeta execTarget = new TransformMeta("exec_target", mock(ITransformMeta.class));
    meta.setExecutionResultTargetTransformMeta(execTarget);

    RowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("keep_me"));
    IVariables variables = new org.apache.hop.core.variables.Variables();
    IHopMetadataProvider provider = new MemoryMetadataProvider();

    meta.getFields(row, "pe", new IRowMeta[] {}, execTarget, variables, provider);

    assertTrue(row.size() > 0);
    assertNotNull(row.searchValueMeta(meta.getExecutionTimeField()));
    assertNotNull(row.searchValueMeta(meta.getExecutionNrErrorsField()));
  }

  @Test
  void getFieldsClearsAndBuildsResultFilesRow() throws HopTransformException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    TransformMeta parent = new TransformMeta("parent", meta);
    meta.setParentTransformMeta(parent);

    TransformMeta fileTarget = new TransformMeta("file_target", mock(ITransformMeta.class));
    meta.setResultFilesTargetTransformMeta(fileTarget);

    RowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("x"));
    IVariables variables = new org.apache.hop.core.variables.Variables();
    IHopMetadataProvider provider = new MemoryMetadataProvider();

    meta.getFields(row, "pe", new IRowMeta[] {}, fileTarget, variables, provider);

    assertEquals(1, row.size());
    assertNotNull(row.searchValueMeta(meta.getResultFilesFileNameField()));
  }

  @Test
  void getFieldsClearsAndBuildsConfiguredResultRows() throws HopTransformException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    TransformMeta parent = new TransformMeta("parent", meta);
    meta.setParentTransformMeta(parent);

    PipelineExecutorResultRows rr = new PipelineExecutorResultRows();
    rr.setName("out_col");
    rr.setType("Integer");
    rr.setLength(9);
    rr.setPrecision(0);
    meta.setResultRows(new ArrayList<>(Collections.singletonList(rr)));

    TransformMeta rowsTarget = new TransformMeta("rows_target", mock(ITransformMeta.class));
    meta.setOutputRowsSourceTransformMeta(rowsTarget);

    RowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("x"));
    IVariables variables = new org.apache.hop.core.variables.Variables();
    IHopMetadataProvider provider = new MemoryMetadataProvider();

    meta.getFields(row, "pe", new IRowMeta[] {}, rowsTarget, variables, provider);

    assertEquals(1, row.size());
    assertNotNull(row.searchValueMeta("out_col"));
  }

  @Test
  void getFieldsLeavesRowUnchangedForMainOutput() throws HopTransformException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    RowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("only"));
    IVariables variables = new org.apache.hop.core.variables.Variables();
    IHopMetadataProvider provider = new MemoryMetadataProvider();

    meta.getFields(row, "pe", new IRowMeta[] {}, null, variables, provider);

    assertEquals(1, row.size());
    assertNotNull(row.searchValueMeta("only"));
  }

  @Test
  void prepareExecutionResultsFileFieldsAddsFileNameColumn() throws HopTransformException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    TransformMeta parent = new TransformMeta("parent", meta);
    meta.setParentTransformMeta(parent);
    TransformMeta fileTarget = new TransformMeta("files", mock(ITransformMeta.class));
    meta.setResultFilesTargetTransformMeta(fileTarget);

    RowMeta row = new RowMeta();
    meta.prepareExecutionResultsFileFields(row, fileTarget);
    assertEquals(1, row.size());
    assertEquals(meta.getResultFilesFileNameField(), row.getValueMeta(0).getName());
  }

  @Test
  void prepareResultsRowsFieldsAddsConfiguredColumns() throws HopTransformException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    TransformMeta parent = new TransformMeta("parent", meta);
    meta.setParentTransformMeta(parent);
    PipelineExecutorResultRows rr = new PipelineExecutorResultRows();
    rr.setName("metric");
    rr.setType("Integer");
    rr.setLength(5);
    rr.setPrecision(0);
    meta.setResultRows(new ArrayList<>(Collections.singletonList(rr)));

    RowMeta row = new RowMeta();
    meta.prepareResultsRowsFields(row);
    assertEquals(1, row.size());
    assertEquals("metric", row.getValueMeta(0).getName());
  }
}
