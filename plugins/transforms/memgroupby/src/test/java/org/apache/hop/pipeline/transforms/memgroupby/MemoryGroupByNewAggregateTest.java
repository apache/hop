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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MemoryGroupByNewAggregateTest {

  static TransformMockHelper<MemoryGroupByMeta, MemoryGroupByData> mockHelper;

  /** GroupTypes whose initial aggregate storage is a StringBuilder (Appendable). */
  private static final Set<GroupType> APPENDABLE_TYPES =
      EnumSet.of(GroupType.ConcatComma, GroupType.ConcatString);

  /** GroupTypes whose initial aggregate storage is a Collection (List/Set). */
  private static final Set<GroupType> COLLECTION_TYPES =
      EnumSet.of(GroupType.Median, GroupType.Percentile, GroupType.ConcatDistinct);

  /** Aggregate types that newAggregate handles. GroupType.None is not a real aggregation. */
  private static final List<GroupType> AGGREGATE_TYPES =
      Arrays.stream(GroupType.values()).filter(t -> t != GroupType.None).toList();

  MemoryGroupBy transform;
  MemoryGroupByData data;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
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
  void setUp() throws Exception {
    data = new MemoryGroupByData();

    List<GAggregate> aggregates = new ArrayList<>();
    data.subjectnrs = new int[AGGREGATE_TYPES.size()];

    for (int i = 0; i < AGGREGATE_TYPES.size(); i++) {
      data.subjectnrs[i] = i;
      // Each aggregate gets a distinct output field name so any error message identifies which.
      aggregates.add(new GAggregate("agg_" + i, "x", AGGREGATE_TYPES.get(i), null));
    }

    MemoryGroupByMeta meta = new MemoryGroupByMeta();
    meta.setAggregates(aggregates);

    IValueMeta vmi = new ValueMetaInteger();
    when(mockHelper.transformMeta.getTransform()).thenReturn(meta);
    IRowMeta rmi = Mockito.mock(IRowMeta.class);
    data.inputRowMeta = rmi;
    when(rmi.getValueMeta(Mockito.anyInt())).thenReturn(vmi);
    data.aggMeta = rmi;

    transform =
        new MemoryGroupBy(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
  }

  @Test
  void testNewAggregate() throws HopException {
    Object[] r = new Object[AGGREGATE_TYPES.size()];
    Arrays.fill(r, null);

    Aggregate agg = new Aggregate();

    transform.newAggregate(r, agg);

    assertEquals(
        AGGREGATE_TYPES.size(), agg.agg.length, "All possible aggregation cases considered");

    for (int i = 0; i < agg.agg.length; i++) {
      GroupType type = AGGREGATE_TYPES.get(i);
      if (APPENDABLE_TYPES.contains(type)) {
        assertInstanceOf(Appendable.class, agg.agg[i], "Expected appendable for type=" + type);
      } else if (COLLECTION_TYPES.contains(type)) {
        assertInstanceOf(Collection.class, agg.agg[i], "Expected collection for type=" + type);
      } else {
        assertNull(agg.agg[i], "Expected null aggregate storage for type=" + type);
      }
    }
  }
}
