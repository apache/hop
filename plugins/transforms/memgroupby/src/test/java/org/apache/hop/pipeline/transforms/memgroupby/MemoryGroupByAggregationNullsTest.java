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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByData.HashEntry;
import org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MemoryGroupByAggregationNullsTest {

  static TransformMockHelper<MemoryGroupByMeta, MemoryGroupByData> mockHelper;

  MemoryGroupBy transform;
  MemoryGroupByData data;

  static int def = 113;

  Aggregate aggregate;
  private IValueMeta vmi;
  private IRowMeta rmi;
  private MemoryGroupByMeta meta;

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
    data = new MemoryGroupByData();
    data.subjectnrs = new int[] {0};
    meta = new MemoryGroupByMeta();
    meta.setDefault();
    meta.getAggregates().add(new GAggregate("x", null, GroupType.Minimum, null));
    vmi = new ValueMetaInteger();
    when(mockHelper.transformMeta.getTransform()).thenReturn(meta);
    rmi = Mockito.mock(IRowMeta.class);
    data.inputRowMeta = rmi;
    data.outputRowMeta = rmi;
    data.groupMeta = rmi;
    data.groupnrs = new int[] {};
    data.map = new HashMap<>();
    when(rmi.getValueMeta(Mockito.anyInt())).thenReturn(vmi);
    data.aggMeta = rmi;
    transform =
        new MemoryGroupBy(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);

    // put aggregate into map with default predefined value
    aggregate = new Aggregate();
    aggregate.agg = new Object[] {def};
    data.map.put(getHashEntry(), aggregate);
  }

  // test hash entry
  HashEntry getHashEntry() {
    return data.getHashEntry(new Object[data.groupMeta.size()]);
  }

  /**
   * "Group by" transform - Minimum aggregation doesn't work
   *
   * <p>HOP_AGGREGATION_MIN_NULL_IS_VALUED
   *
   * <p>Set this variable to Y to set the minimum to NULL if NULL is within an aggregate. Otherwise
   * by default NULL is ignored by the MIN aggregate and MIN is set to the minimum value that is not
   * NULL. See also the variable HOP_AGGREGATION_ALL_NULLS_ARE_ZERO.
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void calcAggregateResulTestMin_1_Test() throws HopException {
    transform.setMinNullIsValued(true);
    transform.addToAggregate(new Object[] {null});

    Aggregate agg = data.map.get(getHashEntry());
    assertNotNull(agg, "Hash code strategy changed?");

    assertNull(agg.agg[0], "Value is set");
  }

  @Test
  @Disabled("This test needs to be reviewed")
  void calcAggregateResulTestMin_5_Test() throws HopException {
    transform.setMinNullIsValued(false);
    transform.addToAggregate(new Object[] {null});

    Aggregate agg = data.map.get(getHashEntry());
    assertNotNull(agg, "Hash code strategy changed?");

    assertEquals(def, agg.agg[0], "Value is NOT set");
  }

  /**
   * Set this variable to Y to return 0 when all values within an aggregate are NULL. Otherwise by
   * default a NULL is returned when all values are NULL.
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void getAggregateResulTestMin_0_Test() throws HopValueException {
    // data.agg[0] is not null - this is the default behavior
    transform.setAllNullsAreZero(true);
    Object[] row = transform.getAggregateResult(aggregate);
    assertEquals(def, row[0], "Default value is not corrupted");
  }

  @Test
  @Disabled("This test needs to be reviewed")
  void getAggregateResulTestMin_1_Test() throws HopValueException {
    aggregate.agg[0] = null;
    transform.setAllNullsAreZero(true);
    Object[] row = transform.getAggregateResult(aggregate);
    assertEquals(0L, row[0], "Returns 0 if aggregation is null");
  }

  @Test
  @Disabled("This test needs to be reviewed")
  void getAggregateResulTestMin_3_Test() throws HopValueException {
    aggregate.agg[0] = null;
    transform.setAllNullsAreZero(false);
    Object[] row = transform.getAggregateResult(aggregate);
    assertNull(row[0], "Returns null if aggregation is null");
  }

  @Test
  @Disabled("This test needs to be reviewed")
  void addToAggregateLazyConversionMinTest() throws Exception {
    vmi.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    vmi.setStorageMetadata(new ValueMetaString());
    aggregate.agg = new Object[] {new byte[0]};
    byte[] bytes = {51};
    transform.addToAggregate(new Object[] {bytes});
    Aggregate result = data.map.get(getHashEntry());
    assertEquals(bytes, result.agg[0], "Returns non-null value");
  }

  @Test
  @Disabled("This test needs to be reviewed")
  void addToAggregateBinaryData() throws Exception {
    MemoryGroupByMeta memoryGroupByMeta = spy(meta);
    memoryGroupByMeta.setAggregates(
        List.of(new GAggregate("f", "test", GroupType.CountDistinct, null)));
    when(mockHelper.transformMeta.getTransform()).thenReturn(memoryGroupByMeta);
    vmi.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
    vmi.setStorageMetadata(new ValueMetaString());
    aggregate.counts = new long[] {0L};
    aggregate.agg = new Object[] {new byte[0]};
    transform =
        new MemoryGroupBy(
            mockHelper.transformMeta,
            memoryGroupByMeta,
            data,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);

    String binaryData0 = "11011";
    String binaryData1 = "01011";
    transform.addToAggregate(new Object[] {binaryData0.getBytes()});
    transform.addToAggregate(new Object[] {binaryData1.getBytes()});

    Object[] distinctObjs = data.map.get(getHashEntry()).distinctObjs[0].toArray();

    assertEquals(binaryData0, distinctObjs[1]);
    assertEquals(binaryData1, distinctObjs[0]);
  }
}
