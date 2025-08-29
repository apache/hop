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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class MemoryGroupByMetaGetFieldsTest {

  private static MockedStatic<ValueMetaFactory> mockedValueMetaFactory;

  private MemoryGroupByMeta memoryGroupByMeta;
  private IRowMeta rowMeta;

  private IRowMeta[] mockInfo;
  private TransformMeta mockNextTransform;
  private IVariables mockSpace;
  private IHopMetadataProvider mockIHopMetadataProvider;

  // Static mock is now managed at method level in @BeforeEach/@AfterEach

  @BeforeEach
  public void setup() {
    // Create static mock first
    mockedValueMetaFactory = mockStatic(ValueMetaFactory.class);

    // Then set up other mocks
    mockSpace = mock(IVariables.class);

    doReturn("N").when(mockSpace).getVariable(any(), anyString());

    rowMeta = spy(new RowMeta());
    memoryGroupByMeta = spy(new MemoryGroupByMeta());
    mockedValueMetaFactory
        .when(() -> ValueMetaFactory.createValueMeta(anyInt()))
        .thenCallRealMethod();
    mockedValueMetaFactory
        .when(() -> ValueMetaFactory.createValueMeta(anyString(), anyInt()))
        .thenCallRealMethod();
    mockedValueMetaFactory
        .when(() -> ValueMetaFactory.createValueMeta("maxDate", 3, -1, -1))
        .thenReturn(new ValueMetaDate("maxDate"));
    mockedValueMetaFactory
        .when(() -> ValueMetaFactory.createValueMeta("minDate", 3, -1, -1))
        .thenReturn(new ValueMetaDate("minDate"));
    mockedValueMetaFactory
        .when(() -> ValueMetaFactory.createValueMeta("countDate", 5, -1, -1))
        .thenReturn(new ValueMetaInteger("countDate"));
    mockedValueMetaFactory.when(() -> ValueMetaFactory.getValueMetaName(3)).thenReturn("Date");
    mockedValueMetaFactory.when(() -> ValueMetaFactory.getValueMetaName(5)).thenReturn("Integer");
  }

  @AfterEach
  void tearDown() {
    if (mockedValueMetaFactory != null) {
      mockedValueMetaFactory.close();
    }
  }

  @Test
  public void getFieldsWithSubject_WithFormat() {
    ValueMetaDate valueMeta = new ValueMetaDate();
    valueMeta.setConversionMask("yyyy-MM-dd");
    valueMeta.setName("date");

    doReturn(valueMeta).when(rowMeta).searchValueMeta("date");

    memoryGroupByMeta.getGroups().clear();
    memoryGroupByMeta.setAggregates(
        List.of(new GAggregate("maxDate", "date", MemoryGroupByMeta.GroupType.Maximum, null)));

    memoryGroupByMeta.getFields(
        rowMeta,
        "Memory Group by",
        mockInfo,
        mockNextTransform,
        mockSpace,
        mockIHopMetadataProvider);

    verify(rowMeta, times(1)).clear();
    verify(rowMeta, times(1)).addRowMeta(any());
    assertEquals("yyyy-MM-dd", rowMeta.searchValueMeta("maxDate").getConversionMask());
  }

  @Test
  public void getFieldsWithSubject_NoFormat() {
    ValueMetaDate valueMeta = new ValueMetaDate();
    valueMeta.setName("date");

    doReturn(valueMeta).when(rowMeta).searchValueMeta("date");

    memoryGroupByMeta.setAggregates(
        List.of(new GAggregate("minDate", "date", MemoryGroupByMeta.GroupType.Minimum, null)));

    memoryGroupByMeta.getFields(
        rowMeta, "Group by", mockInfo, mockNextTransform, mockSpace, mockIHopMetadataProvider);

    verify(rowMeta, times(1)).clear();
    verify(rowMeta, times(1)).addRowMeta(any());
    assertNull(rowMeta.searchValueMeta("minDate").getConversionMask());
  }

  @Test
  public void getFieldsWithoutSubject() {
    ValueMetaDate valueMeta = new ValueMetaDate();
    valueMeta.setName("date");

    doReturn(valueMeta).when(rowMeta).searchValueMeta("date");

    memoryGroupByMeta.setGroups(List.of(new GGroup("date")));
    memoryGroupByMeta.setAggregates(
        List.of(new GAggregate("countDate", "date", MemoryGroupByMeta.GroupType.CountAny, null)));

    memoryGroupByMeta.getFields(
        rowMeta, "Group by", mockInfo, mockNextTransform, mockSpace, mockIHopMetadataProvider);

    verify(rowMeta, times(1)).clear();
    verify(rowMeta, times(1)).addRowMeta(any());
    assertNotNull(rowMeta.searchValueMeta("countDate"));
  }
}
