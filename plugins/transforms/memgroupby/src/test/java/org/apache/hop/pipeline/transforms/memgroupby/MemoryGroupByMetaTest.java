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

import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.Average;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.ConcatComma;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.ConcatString;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.CountAll;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.CountAny;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.CountDistinct;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.First;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.FirstIncludingNull;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.Last;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.LastIncludingNull;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.Maximum;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.Median;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.Minimum;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.Percentile;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.StandardDeviation;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType.Sum;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MemoryGroupByMetaTest {

  @BeforeEach
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  public void testSerialization() throws Exception {
    MemoryGroupByMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/memory-group-by-transform.xml", MemoryGroupByMeta.class);

    assertEquals(1, meta.getGroups().size());
    assertEquals("fruit", meta.getGroups().get(0).getField());
    assertEquals(8, meta.getAggregates().size());
  }

  private IRowMeta getInputRowMeta() {
    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("myGroupField2"));
    rm.addValueMeta(new ValueMetaString("myGroupField1"));
    rm.addValueMeta(new ValueMetaString("myString"));
    rm.addValueMeta(new ValueMetaInteger("myInteger"));
    rm.addValueMeta(new ValueMetaNumber("myNumber"));
    rm.addValueMeta(new ValueMetaBigNumber("myBigNumber"));
    rm.addValueMeta(new ValueMetaBinary("myBinary"));
    rm.addValueMeta(new ValueMetaBoolean("myBoolean"));
    rm.addValueMeta(new ValueMetaDate("myDate"));
    rm.addValueMeta(new ValueMetaTimestamp("myTimestamp"));
    rm.addValueMeta(new ValueMetaInternetAddress("myInternetAddress"));
    return rm;
  }

  @Test
  public void testGetFields() {
    final String transformName = "this transform name";
    MemoryGroupByMeta meta = new MemoryGroupByMeta();
    meta.setDefault();

    // Declare input fields
    IRowMeta rm = getInputRowMeta();

    meta.setGroups(List.of(new GGroup("myGroupField1"), new GGroup("myGroupField2")));

    meta.setAggregates(
        Arrays.asList(
            new GAggregate("ConcatComma", "myString", ConcatComma, null),
            new GAggregate("ConcatString", "myString", ConcatString, null),
            new GAggregate("CountAll", "myString", CountAll, null),
            new GAggregate("CountAny", "myString", CountAny, null),
            new GAggregate("CountDistinct", "myString", CountDistinct, null),
            new GAggregate("First(String)", "myString", First, null),
            new GAggregate("First(Integer)", "myInteger", First, null),
            new GAggregate("FirstInclNull(Number)", "myNumber", FirstIncludingNull, null),
            new GAggregate("FirstInclNull(BigNumber)", "myBigNumber", FirstIncludingNull, null),
            new GAggregate("Last(Binary)", "myBinary", Last, null),
            new GAggregate("Last(Boolean)", "myBoolean", Last, null),
            new GAggregate("LastInclNull(Date)", "myDate", LastIncludingNull, null),
            new GAggregate("LastInclNull(Timestamp)", "myTimestamp", LastIncludingNull, null),
            new GAggregate("Max(InternetAddress)", "myInternetAddress", Maximum, null),
            new GAggregate("Max(String)", "myString", Maximum, null),
            new GAggregate("Median(Integer)", "myInteger", Median, null),
            new GAggregate("Min(Number)", "myNumber", Minimum, null),
            new GAggregate("Min(BigNumber)", "myBigNumber", Minimum, null),
            new GAggregate("Percentile(Binary)", "myBinary", Percentile, null),
            new GAggregate("StandardDeviation(Boolean)", "myBoolean", StandardDeviation, null),
            new GAggregate("Sum(Date)", "myDate", Sum, null),
            new GAggregate("Sum(Integer)", "myInteger", Sum, null),
            new GAggregate("Average(Integer)", "myInteger", Average, null),
            new GAggregate("Average(Date)", "myDate", Average, null)));

    Variables vars = new Variables();
    meta.getFields(rm, transformName, null, null, vars, null);
    assertNotNull(rm);
    assertEquals(26, rm.size());
    assertTrue(rm.indexOfValue("myGroupField1") >= 0);
    assertEquals(
        IValueMeta.TYPE_STRING, rm.getValueMeta(rm.indexOfValue("myGroupField1")).getType());
    assertTrue(rm.indexOfValue("myGroupField2") >= 0);
    assertEquals(
        IValueMeta.TYPE_STRING, rm.getValueMeta(rm.indexOfValue("myGroupField2")).getType());
    assertTrue(rm.indexOfValue("myGroupField2") > rm.indexOfValue("myGroupField1"));
    assertTrue(rm.indexOfValue("ConcatComma") >= 0);
    assertEquals(IValueMeta.TYPE_STRING, rm.getValueMeta(rm.indexOfValue("ConcatComma")).getType());
    assertTrue(rm.indexOfValue("ConcatString") >= 0);
    assertEquals(
        IValueMeta.TYPE_STRING, rm.getValueMeta(rm.indexOfValue("ConcatString")).getType());
    assertTrue(rm.indexOfValue("CountAll") >= 0);
    assertEquals(IValueMeta.TYPE_INTEGER, rm.getValueMeta(rm.indexOfValue("CountAll")).getType());
    assertTrue(rm.indexOfValue("CountAny") >= 0);
    assertEquals(IValueMeta.TYPE_INTEGER, rm.getValueMeta(rm.indexOfValue("CountAny")).getType());
    assertTrue(rm.indexOfValue("CountDistinct") >= 0);
    assertEquals(
        IValueMeta.TYPE_INTEGER, rm.getValueMeta(rm.indexOfValue("CountDistinct")).getType());
    assertTrue(rm.indexOfValue("First(String)") >= 0);
    assertEquals(
        IValueMeta.TYPE_STRING, rm.getValueMeta(rm.indexOfValue("First(String)")).getType());
    assertTrue(rm.indexOfValue("First(Integer)") >= 0);
    assertEquals(
        IValueMeta.TYPE_INTEGER, rm.getValueMeta(rm.indexOfValue("First(Integer)")).getType());
    assertTrue(rm.indexOfValue("FirstInclNull(Number)") >= 0);
    assertEquals(
        IValueMeta.TYPE_NUMBER,
        rm.getValueMeta(rm.indexOfValue("FirstInclNull(Number)")).getType());
    assertTrue(rm.indexOfValue("FirstInclNull(BigNumber)") >= 0);
    assertEquals(
        IValueMeta.TYPE_BIGNUMBER,
        rm.getValueMeta(rm.indexOfValue("FirstInclNull(BigNumber)")).getType());
    assertTrue(rm.indexOfValue("Last(Binary)") >= 0);
    assertEquals(
        IValueMeta.TYPE_BINARY, rm.getValueMeta(rm.indexOfValue("Last(Binary)")).getType());
    assertTrue(rm.indexOfValue("Last(Boolean)") >= 0);
    assertEquals(
        IValueMeta.TYPE_BOOLEAN, rm.getValueMeta(rm.indexOfValue("Last(Boolean)")).getType());
    assertTrue(rm.indexOfValue("LastInclNull(Date)") >= 0);
    assertEquals(
        IValueMeta.TYPE_DATE, rm.getValueMeta(rm.indexOfValue("LastInclNull(Date)")).getType());
    assertTrue(rm.indexOfValue("LastInclNull(Timestamp)") >= 0);
    assertEquals(
        IValueMeta.TYPE_TIMESTAMP,
        rm.getValueMeta(rm.indexOfValue("LastInclNull(Timestamp)")).getType());
    assertTrue(rm.indexOfValue("Max(InternetAddress)") >= 0);
    assertEquals(
        IValueMeta.TYPE_INET, rm.getValueMeta(rm.indexOfValue("Max(InternetAddress)")).getType());
    assertTrue(rm.indexOfValue("Max(String)") >= 0);
    assertEquals(IValueMeta.TYPE_STRING, rm.getValueMeta(rm.indexOfValue("Max(String)")).getType());
    assertTrue(rm.indexOfValue("Median(Integer)") >= 0);
    assertEquals(
        IValueMeta.TYPE_NUMBER, rm.getValueMeta(rm.indexOfValue("Median(Integer)")).getType());
    assertTrue(rm.indexOfValue("Min(Number)") >= 0);
    assertEquals(IValueMeta.TYPE_NUMBER, rm.getValueMeta(rm.indexOfValue("Min(Number)")).getType());
    assertTrue(rm.indexOfValue("Min(BigNumber)") >= 0);
    assertEquals(
        IValueMeta.TYPE_BIGNUMBER, rm.getValueMeta(rm.indexOfValue("Min(BigNumber)")).getType());
    assertTrue(rm.indexOfValue("Percentile(Binary)") >= 0);
    assertEquals(
        IValueMeta.TYPE_NUMBER, rm.getValueMeta(rm.indexOfValue("Percentile(Binary)")).getType());
    assertTrue(rm.indexOfValue("StandardDeviation(Boolean)") >= 0);
    assertEquals(
        IValueMeta.TYPE_NUMBER,
        rm.getValueMeta(rm.indexOfValue("StandardDeviation(Boolean)")).getType());
    assertTrue(rm.indexOfValue("Sum(Date)") >= 0);
    assertEquals(
        IValueMeta.TYPE_NUMBER,
        rm.getValueMeta(rm.indexOfValue("Sum(Date)")).getType()); // Force changed to Numeric
    assertTrue(rm.indexOfValue("Sum(Integer)") >= 0);
    assertEquals(
        IValueMeta.TYPE_INTEGER, rm.getValueMeta(rm.indexOfValue("Sum(Integer)")).getType());
    assertTrue(rm.indexOfValue("Average(Integer)") >= 0);
    assertEquals(
        IValueMeta.TYPE_INTEGER, rm.getValueMeta(rm.indexOfValue("Average(Integer)")).getType());
    assertTrue(rm.indexOfValue("Average(Date)") >= 0);
    assertEquals(
        IValueMeta.TYPE_NUMBER, rm.getValueMeta(rm.indexOfValue("Average(Date)")).getType());
  }
}
