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

package org.apache.hop.pipeline.transforms.maskfields;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class MaskFieldsTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<MaskFieldsMeta, MaskFieldsData> mockHelper;

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>("Mask fields", MaskFieldsMeta.class, MaskFieldsData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
    when(mockHelper.transformMeta.getCopies(any())).thenReturn(1);
    when(mockHelper.transformMeta.isPartitioned()).thenReturn(false);
    when(mockHelper.transformMeta.isDoingErrorHandling()).thenReturn(false);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void processRowRemembersANameForThisRun() throws Exception {
    MaskingPattern pattern = new MaskingPattern();
    pattern.setName("First name");
    pattern.setValueSource(MaskingValueSource.SYNTHETIC);
    pattern.setToken(MaskingToken.SEQUENCE);
    pattern.setStorage(MaskingStorage.MEMORY);
    pattern.setPrefix("first-name-");
    pattern.setSequenceStart("1");
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(MaskingPattern.class).save(pattern);

    MaskFieldsMeta meta = new MaskFieldsMeta();
    meta.getFields().add(new MaskField("name", "First name"));
    MaskFields transform = createTransform(meta);
    transform.setMetadataProvider(provider);
    assertTrue(transform.init());

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    BlockingRowSet input =
        input(
            "people", rowMeta, new Object[] {"Matt"}, new Object[] {"Ann"}, new Object[] {"Matt"});
    BlockingRowSet output = output(rowMeta);
    transform.addRowSetToInputRowSets(input);
    transform.addRowSetToOutputRowSets(output);

    assertTrue(transform.processRow());
    assertTrue(transform.processRow());
    assertTrue(transform.processRow());
    assertFalse(transform.processRow());

    assertArrayEquals(new Object[] {"first-name-1"}, output.getRowWait(1, TimeUnit.SECONDS));
    assertArrayEquals(new Object[] {"first-name-2"}, output.getRowWait(1, TimeUnit.SECONDS));
    assertArrayEquals(new Object[] {"first-name-1"}, output.getRowWait(1, TimeUnit.SECONDS));
    transform.dispose();
  }

  @Test
  void processRowReadsTheInfoList() throws Exception {
    MaskingPattern pattern = new MaskingPattern();
    pattern.setName("First name");
    pattern.setValueSource(MaskingValueSource.LIST);
    pattern.setStorage(MaskingStorage.MEMORY);
    pattern.setListField("replacement");
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(MaskingPattern.class).save(pattern);

    MaskFieldsMeta meta = new MaskFieldsMeta();
    meta.setInfoTransformName("Replacements");
    meta.getFields().add(new MaskField("name", "First name"));
    MaskFields transform = createTransform(meta);
    transform.setMetadataProvider(provider);

    TransformMeta source = mock(TransformMeta.class);
    when(source.getName()).thenReturn("Replacements");
    when(source.isPartitioned()).thenReturn(false);
    when(source.getCopies(any())).thenReturn(1);
    when(mockHelper.pipelineMeta.findTransform("Replacements")).thenReturn(source);
    assertTrue(transform.init());

    RowMeta people = new RowMeta();
    people.addValueMeta(new ValueMetaString("name"));
    RowMeta replacements = new RowMeta();
    replacements.addValueMeta(new ValueMetaString("replacement"));
    transform.addRowSetToInputRowSets(
        input("Replacements", replacements, new Object[] {"Ada"}, new Object[] {"Bea"}));
    transform.addRowSetToInputRowSets(
        input(
            "People", people, new Object[] {"Matt"}, new Object[] {"Ann"}, new Object[] {"Matt"}));
    BlockingRowSet output = output(people);
    transform.addRowSetToOutputRowSets(output);

    assertTrue(transform.processRow());
    assertTrue(transform.processRow());
    assertTrue(transform.processRow());
    assertEquals("Ada", output.getRowWait(1, TimeUnit.SECONDS)[0]);
    assertEquals("Bea", output.getRowWait(1, TimeUnit.SECONDS)[0]);
    assertEquals("Ada", output.getRowWait(1, TimeUnit.SECONDS)[0]);
    transform.dispose();
  }

  private MaskFields createTransform(MaskFieldsMeta meta) {
    when(mockHelper.transformMeta.getTransform()).thenReturn(meta);
    return new MaskFields(
        mockHelper.transformMeta,
        meta,
        new MaskFieldsData(),
        0,
        mockHelper.pipelineMeta,
        mockHelper.pipeline);
  }

  private static BlockingRowSet input(String origin, IRowMeta rowMeta, Object[]... rows) {
    BlockingRowSet rowSet = new BlockingRowSet(10);
    rowSet.setThreadNameFromToCopy(origin, 0, "Mask fields", 0);
    rowSet.setRowMeta(rowMeta);
    for (Object[] row : rows) {
      rowSet.putRow(rowMeta, row);
    }
    rowSet.setDone();
    return rowSet;
  }

  private static BlockingRowSet output(IRowMeta rowMeta) {
    BlockingRowSet rowSet = new BlockingRowSet(10);
    rowSet.setThreadNameFromToCopy("Mask fields", 0, "next", 0);
    rowSet.setRowMeta(rowMeta);
    return rowSet;
  }
}
