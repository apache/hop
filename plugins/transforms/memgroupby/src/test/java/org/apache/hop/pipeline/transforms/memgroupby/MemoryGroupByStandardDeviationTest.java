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

package org.apache.hop.pipeline.transforms.memgroupby;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Standard deviation skips null values, like the Group by transform does. */
class MemoryGroupByStandardDeviationTest {

  static TransformMockHelper<MemoryGroupByMeta, MemoryGroupByData> mockHelper;

  private MemoryGroupBy transform;
  private MemoryGroupByData data;

  @BeforeAll
  static void setUpBeforeClass() {
    mockHelper =
        new TransformMockHelper<>(
            "Memory Group By", MemoryGroupByMeta.class, MemoryGroupByData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterAll
  static void cleanUp() {
    mockHelper.cleanUp();
  }

  @BeforeEach
  void setUp() {
    MemoryGroupByMeta meta = new MemoryGroupByMeta();
    meta.setDefault();
    meta.getAggregates().add(new GAggregate("sd", "x", GroupType.StandardDeviation, null));
    when(mockHelper.transformMeta.getTransform()).thenReturn(meta);

    data = new MemoryGroupByData();
    data.inputRowMeta = new RowMeta();
    data.inputRowMeta.addValueMeta(new ValueMetaNumber("x"));
    data.groupMeta = new RowMeta();
    data.groupnrs = new int[] {};
    data.subjectnrs = new int[] {0};
    data.aggMeta = new RowMeta();
    data.aggMeta.addValueMeta(new ValueMetaNumber("sd"));
    data.map = new HashMap<>();

    transform =
        new MemoryGroupBy(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
  }

  private Aggregate theAggregate() {
    return data.map.get(data.getHashEntry(new Object[0]));
  }

  @Test
  void nullValuesAreSkipped() throws Exception {
    transform.addToAggregate(new Object[] {2.0});
    transform.addToAggregate(new Object[] {null});
    transform.addToAggregate(new Object[] {4.0});

    Object[] result = transform.getAggregateResult(theAggregate());

    // population standard deviation of {2, 4}
    assertEquals(1.0, (Double) result[0], 1e-9);
  }

  @Test
  void onlyNullValuesGiveNull() throws Exception {
    transform.addToAggregate(new Object[] {null});
    transform.addToAggregate(new Object[] {null});

    Object[] result = transform.getAggregateResult(theAggregate());

    assertNull(result[0]);
  }
}
