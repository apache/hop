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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit tests for {@link PipelineExecutor} runtime behaviour (parameter passing, helpers). */
class PipelineExecutorTest {

  @RegisterExtension
  static final RestoreHopEngineEnvironmentExtension env =
      new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void initHop() throws HopException {
    HopEnvironment.init();
  }

  private PipelineExecutor newExecutor(PipelineExecutorMeta meta, PipelineExecutorData data) {
    TransformMeta transformMeta = mock(TransformMeta.class);
    when(transformMeta.getName()).thenReturn("pipeline_executor");
    when(transformMeta.isPartitioned()).thenReturn(false);
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    when(pipelineMeta.findTransform(anyString())).thenReturn(transformMeta);
    LocalPipelineEngine pipeline = spy(new LocalPipelineEngine());
    PipelineExecutor executor =
        new PipelineExecutor(transformMeta, meta, data, 0, pipelineMeta, pipeline);
    executor.setMetadataProvider(new MemoryMetadataProvider());
    return executor;
  }

  @Test
  void getLastIncomingFieldValuesReturnsNullWhenBufferEmpty() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    PipelineExecutorData data = new PipelineExecutorData();
    data.groupBuffer = new ArrayList<>();
    PipelineExecutor executor = newExecutor(meta, data);

    assertNull(executor.getLastIncomingFieldValues());
  }

  @Test
  void getLastIncomingFieldValuesReturnsStringValuesFromLastBufferedRow() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    PipelineExecutorData data = new PipelineExecutorData();
    data.groupBuffer = new ArrayList<>();
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("a"));
    rowMeta.addValueMeta(new ValueMetaString("b"));
    data.groupBuffer.add(new RowMetaAndData(rowMeta, new Object[] {"first", "second"}));
    data.groupBuffer.add(new RowMetaAndData(rowMeta, new Object[] {"x", null}));

    PipelineExecutor executor = newExecutor(meta, data);
    List<String> last = executor.getLastIncomingFieldValues();

    assertNotNull(last);
    assertEquals(Collections.singletonList("x"), last);
  }

  @Test
  void discardLogLinesDoesNothingWhenExecutorPipelineIsNull() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    PipelineExecutorData data = new PipelineExecutorData();
    data.setExecutorPipeline(null);
    PipelineExecutor executor = newExecutor(meta, data);

    assertNotNull(executor);
    executor.discardLogLines(data);
  }

  @Test
  void passParametersToPipelineMapsIncomingFieldToChildParameter() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setInheritingAllVariables(false);
    PipelineExecutorParameters param = new PipelineExecutorParameters();
    param.setVariable("MYPARAM");
    param.setField("myField");
    param.setInput("");
    meta.setParameters(new ArrayList<>(Collections.singletonList(param)));

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("id"));
    inputRowMeta.addValueMeta(new ValueMetaString("myField"));

    PipelineExecutorData data = new PipelineExecutorData();
    data.setInputRowMeta(inputRowMeta);
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> child = mock(IPipelineEngine.class);
    when(child.listParameters()).thenReturn(new String[] {"MYPARAM"});
    when(child.getPipelineMeta()).thenReturn(mock(PipelineMeta.class));
    data.setExecutorPipeline(child);

    PipelineExecutor executor = newExecutor(meta, data);
    executor.passParametersToPipeline(Arrays.asList("1", "from-row"));

    verify(child, atLeastOnce()).setParameterValue("MYPARAM", "from-row");
  }

  @Test
  void passParametersToPipelineUsesStaticInputWhenFieldNotInRow() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setInheritingAllVariables(false);
    PipelineExecutorParameters param = new PipelineExecutorParameters();
    param.setVariable("P");
    param.setField("notInStream");
    param.setInput("static-value");
    meta.setParameters(new ArrayList<>(Collections.singletonList(param)));

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("id"));

    PipelineExecutorData data = new PipelineExecutorData();
    data.setInputRowMeta(inputRowMeta);
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> child = mock(IPipelineEngine.class);
    when(child.listParameters()).thenReturn(new String[] {"P"});
    when(child.getPipelineMeta()).thenReturn(mock(PipelineMeta.class));
    data.setExecutorPipeline(child);

    PipelineExecutor executor = newExecutor(meta, data);
    executor.passParametersToPipeline(Collections.singletonList("row-id"));

    verify(child, atLeastOnce()).setParameterValue("P", "static-value");
  }

  @Test
  void passParametersToPipelineTreatsNullIncomingListAsEmpty() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setInheritingAllVariables(false);
    PipelineExecutorParameters param = new PipelineExecutorParameters();
    param.setVariable("P");
    param.setField("");
    param.setInput("fallback");
    meta.setParameters(new ArrayList<>(Collections.singletonList(param)));

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("id"));

    PipelineExecutorData data = new PipelineExecutorData();
    data.setInputRowMeta(inputRowMeta);
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> child = mock(IPipelineEngine.class);
    when(child.listParameters()).thenReturn(new String[] {"P"});
    when(child.getPipelineMeta()).thenReturn(mock(PipelineMeta.class));
    data.setExecutorPipeline(child);

    PipelineExecutor executor = newExecutor(meta, data);
    executor.passParametersToPipeline(null);

    verify(child, atLeastOnce()).setParameterValue("P", "fallback");
  }

  @Test
  void collectPipelineResultsDoesNotForwardRowsWhenNoResultRowsTarget() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();

    PipelineExecutorData data = new PipelineExecutorData();
    data.setResultRowsRowSet(null);

    PipelineExecutor executor = spy(newExecutor(meta, data));
    Result result = new Result();
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("x"));
    result.setRows(Collections.singletonList(new RowMetaAndData(rowMeta, new Object[] {"v"})));

    executor.collectPipelineResults(result);

    verify(executor, never()).putRowTo(any(), any(), any());
  }

  @Test
  void stopRunningDoesNothingWhenNoExecutorPipeline() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    PipelineExecutorData data = new PipelineExecutorData();
    data.setExecutorPipeline(null);
    PipelineExecutor executor = newExecutor(meta, data);

    assertNotNull(executor);
    executor.stopRunning();
  }

  @Test
  void stopAllDelegatesToExecutorWhenPresent() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    PipelineExecutorData data = new PipelineExecutorData();
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> child = mock(IPipelineEngine.class);
    data.setExecutorPipeline(child);

    PipelineExecutor executor = newExecutor(meta, data);
    executor.stopAll();

    verify(child).stopAll();
  }

  @Test
  void stopRunningStopsChildPipelineWhenPresent() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    PipelineExecutorData data = new PipelineExecutorData();
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> child = mock(IPipelineEngine.class);
    data.setExecutorPipeline(child);
    PipelineExecutor executor = newExecutor(meta, data);

    executor.stopRunning();

    verify(child).stopAll();
  }

  @Test
  void disposeClearsGroupBuffer() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    PipelineExecutorData data = new PipelineExecutorData();
    data.groupBuffer = new ArrayList<>();
    data.groupBuffer.add(new RowMetaAndData(new RowMeta(), new Object[0]));

    PipelineExecutor executor = newExecutor(meta, data);
    executor.dispose();

    assertNull(data.groupBuffer);
  }

  @Test
  void initFailsWhenStaticFilenameMissing() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setFilenameInField(false);
    meta.setFilename("");
    meta.setRunConfigurationName("local");

    PipelineExecutor executor = newExecutor(meta, new PipelineExecutorData());
    assertFalse(executor.init());
  }

  @Test
  void initSucceedsWhenFilenameComesFromField() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setFilenameInField(true);
    meta.setFilenameField("child_path");
    meta.setFilename("/tmp/ignored-default.hpl");
    meta.setRunConfigurationName("local");

    PipelineExecutor executor = newExecutor(meta, new PipelineExecutorData());
    assertTrue(executor.init());
  }

  @Test
  void initLoadsChildPipelineFromFilesystemPath() throws URISyntaxException {
    String path =
        Paths.get(
                Objects.requireNonNull(
                        PipelineExecutorTest.class.getResource(
                            "/org/apache/hop/pipeline/transforms/pipelineexecutor/minimal-child.hpl"))
                    .toURI())
            .toAbsolutePath()
            .toString();

    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setFilenameInField(false);
    meta.setFilename(path);
    meta.setRunConfigurationName("local");

    PipelineExecutor executor = newExecutor(meta, new PipelineExecutorData());
    assertTrue(executor.init());
    assertNotNull(executor.getData().getExecutorPipelineMeta());
  }

  @Test
  void loadMappingMetaHonorsExplicitFilenameOverWrongMetaFilename()
      throws HopException, URISyntaxException {
    String goodPath =
        Paths.get(
                Objects.requireNonNull(
                        PipelineExecutorTest.class.getResource(
                            "/org/apache/hop/pipeline/transforms/pipelineexecutor/minimal-child.hpl"))
                    .toURI())
            .toAbsolutePath()
            .toString();

    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setFilename("/this/path/does/not/exist/bad-child.hpl");

    PipelineMeta loaded =
        TransformWithMappingMeta.loadMappingMeta(
            meta, goodPath, new MemoryMetadataProvider(), new Variables());

    assertNotNull(loaded);
    assertFalse(loaded.getTransforms().isEmpty());
  }
}
