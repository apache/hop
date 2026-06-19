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
 */

package org.apache.hop.parquet.transforms.output;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.parquet.column.ParquetProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Unit test for {@link ParquetOutput} */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class ParquetOutputTest {
  private TransformMockHelper<ParquetOutputMeta, ParquetOutputData> mockHelper;

  @BeforeEach
  void setUp() throws Exception {
    mockHelper =
        new TransformMockHelper<>(
            "Parquet Output", ParquetOutputMeta.class, ParquetOutputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void testInitCalculatesSizes() {
    ParquetOutputMeta meta = new ParquetOutputMeta();
    meta.setDataPageSize("1024");
    meta.setDictionaryPageSize("512");
    meta.setRowGroupSize("128");
    meta.setFileSplitSize("100");

    ParquetOutputData data = new ParquetOutputData();
    ParquetOutput output = createTransform(meta, data);

    assertTrue(output.init());
    assertEquals(1024, data.pageSize);
    assertEquals(512, data.dictionaryPageSize);
    assertEquals(128, data.rowGroupSize);
    assertEquals(100, data.maxSplitSizeRows);
  }

  @Test
  void testInitUsesDefaultsForInvalidValues() {
    ParquetOutputMeta meta = new ParquetOutputMeta();
    meta.setDataPageSize("invalid");
    meta.setDictionaryPageSize("");
    meta.setRowGroupSize(null);
    meta.setFileSplitSize("not-a-number");

    ParquetOutputData data = new ParquetOutputData();
    ParquetOutput output = createTransform(meta, data);

    assertTrue(output.init());
    assertEquals(ParquetProperties.DEFAULT_PAGE_SIZE, data.pageSize);
    assertEquals(ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE, data.dictionaryPageSize);
    assertEquals(ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT, data.rowGroupSize);
    assertEquals(-1, data.maxSplitSizeRows);
  }

  @Test
  void testProcessRowWithNoInputRows() throws Exception {
    ParquetOutputMeta meta = new ParquetOutputMeta();
    ParquetOutputData data = new ParquetOutputData();
    ParquetOutput output = spy(createTransform(meta, data));

    assertTrue(output.init());
    doReturn(null).when(output).getRow();

    assertFalse(output.processRow());
  }

  @Test
  void testProcessRowMissingSourceField() throws Exception {
    ParquetOutputMeta meta = new ParquetOutputMeta();
    meta.getFields().add(new ParquetField("missing", "missing"));

    ParquetOutputData data = new ParquetOutputData();
    ParquetOutput output = spy(createTransform(meta, data));
    output.setInputRowMeta(new RowMeta());

    assertTrue(output.init());
    doReturn(new Object[] {1L}).when(output).getRow();

    org.junit.jupiter.api.Assertions.assertThrows(HopException.class, output::processRow);
  }

  @Test
  void testResolveOutputFieldsUsesAllInputFieldsWhenNoneConfigured() throws Exception {
    ParquetOutputMeta meta = new ParquetOutputMeta();
    ParquetOutputData data = new ParquetOutputData();
    ParquetOutput output = createTransform(meta, data);

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger("id"));
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    output.setInputRowMeta(inputRowMeta);

    output.resolveOutputFields();

    assertEquals(2, data.outputFields.size());
    assertEquals("id", data.outputFields.get(0).getSourceFieldName());
    assertEquals("id", data.outputFields.get(0).getTargetFieldName());
    assertEquals("name", data.outputFields.get(1).getSourceFieldName());
    assertEquals("name", data.outputFields.get(1).getTargetFieldName());
    assertEquals(0, data.sourceFieldIndexes.get(0));
    assertEquals(1, data.sourceFieldIndexes.get(1));
  }

  @Test
  void testResolveOutputFieldsUsesConfiguredFields() throws Exception {
    ParquetOutputMeta meta = new ParquetOutputMeta();
    meta.getFields().add(new ParquetField("id", "identifier"));
    meta.getFields().add(new ParquetField("name", ""));

    ParquetOutputData data = new ParquetOutputData();
    ParquetOutput output = createTransform(meta, data);

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger("id"));
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    output.setInputRowMeta(inputRowMeta);

    output.resolveOutputFields();

    assertEquals(2, data.outputFields.size());
    assertEquals("identifier", data.outputFields.get(0).getTargetFieldName());
    assertEquals("name", data.outputFields.get(1).getTargetFieldName());
  }

  private ParquetOutput createTransform(ParquetOutputMeta meta, ParquetOutputData data) {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta transformMeta = new TransformMeta("Parquet Output", meta);
    pipelineMeta.addTransform(transformMeta);
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    return new ParquetOutput(transformMeta, meta, data, 0, pipelineMeta, pipeline);
  }
}
