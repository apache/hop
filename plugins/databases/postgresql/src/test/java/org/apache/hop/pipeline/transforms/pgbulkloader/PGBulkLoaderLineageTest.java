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

package org.apache.hop.pipeline.transforms.pgbulkloader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.lineage.model.RelationalWriteColumn;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Covers the per-column provenance the PostgreSQL bulk loader contributes to lineage. The bulk
 * loaders are the case where the target columns come from an explicit mapping rather than from the
 * stream, so target and stream names differ and the mapping has to be carried through.
 */
class PGBulkLoaderLineageTest {

  private TransformMockHelper<PGBulkLoaderMeta, PGBulkLoaderData> mockHelper;
  private PGBulkLoaderMeta meta;
  private PGBulkLoader transform;

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>(
            "PostgreSQL bulk loader", PGBulkLoaderMeta.class, PGBulkLoaderData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    meta = mockHelper.iTransformMeta;
    transform =
        spy(
            new PGBulkLoader(
                mockHelper.transformMeta,
                meta,
                mockHelper.iTransformData,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void buildWriteColumns_carriesTheMappingAndTheOriginTransform() {
    when(meta.getMappings())
        .thenReturn(
            List.of(
                new PGBulkLoaderMappingMeta("order_id", "id", null),
                new PGBulkLoaderMappingMeta("order_amount", "amount", null)));
    doReturn(rowMeta("Read orders_source", "id", "amount")).when(transform).getInputRowMeta();

    List<RelationalWriteColumn> columns = transform.buildWriteColumns();

    assertEquals(2, columns.size());
    assertEquals("order_id", columns.get(0).getTargetColumn());
    assertEquals("id", columns.get(0).getStreamField());
    assertEquals("Read orders_source", columns.get(0).getOriginTransform());
    assertEquals("order_amount", columns.get(1).getTargetColumn());
    assertEquals("amount", columns.get(1).getStreamField());
  }

  @Test
  void buildWriteColumns_leavesOriginUnsetForUnresolvableStreamFields() {
    when(meta.getMappings())
        .thenReturn(List.of(new PGBulkLoaderMappingMeta("order_id", "not_in_stream", null)));
    doReturn(rowMeta("Read orders_source", "id")).when(transform).getInputRowMeta();

    List<RelationalWriteColumn> columns = transform.buildWriteColumns();

    assertEquals(1, columns.size());
    assertNull(columns.get(0).getOriginTransform());
  }

  @Test
  void buildWriteColumns_isEmptyWithoutAnInputRowShape() {
    when(meta.getMappings())
        .thenReturn(List.of(new PGBulkLoaderMappingMeta("order_id", "id", null)));
    doReturn(null).when(transform).getInputRowMeta();

    assertTrue(transform.buildWriteColumns().isEmpty());
  }

  private static IRowMeta rowMeta(String origin, String... fields) {
    RowMeta rowMeta = new RowMeta();
    for (String field : fields) {
      ValueMetaString valueMeta = new ValueMetaString(field);
      valueMeta.setOrigin(origin);
      rowMeta.addValueMeta(valueMeta);
    }
    return rowMeta;
  }
}
