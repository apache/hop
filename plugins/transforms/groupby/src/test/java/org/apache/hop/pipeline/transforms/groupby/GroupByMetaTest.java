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

package org.apache.hop.pipeline.transforms.groupby;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class GroupByMetaTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void testClone() {
    GroupByMeta meta1 = generateTestMeta();
    GroupByMeta meta2 = (GroupByMeta) meta1.clone();

    compareMetas(meta1, meta2);
  }

  @Test
  void testSerialization() throws Exception {
    GroupByMeta meta1 = generateTestMeta();
    String xml = "<transform>" + XmlMetadataUtil.serializeObjectToXml(meta1) + "</transform>";

    GroupByMeta meta2 =
        XmlMetadataUtil.deSerializeFromXml(
            XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "transform"),
            GroupByMeta.class,
            new MemoryMetadataProvider());

    compareMetas(meta1, meta2);
  }

  @Test
  void testSetDefault() {
    GroupByMeta meta = new GroupByMeta();
    meta.setDefault();

    assertEquals("${java.io.tmpdir}", meta.getDirectory());
    assertEquals("grp", meta.getPrefix());
    assertFalse(meta.isPassAllRows());
    assertFalse(meta.isAggregateIgnored());
    assertNull(meta.getAggregateIgnoredField());
    assertNotNull(meta.getGroupingFields());
    assertNotNull(meta.getAggregations());
  }

  @Test
  void testGetFieldsWithoutPassAllRows() {
    GroupByMeta meta = new GroupByMeta();
    meta.setDefault();
    meta.getGroupingFields().add(new GroupingField("grp"));
    meta.getAggregations()
        .add(
            new Aggregation(
                "sum_amount",
                "amount",
                Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_SUM),
                null));
    meta.getAggregations()
        .add(
            new Aggregation(
                "cnt",
                "amount",
                Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_COUNT_ALL),
                null));

    IRowMeta rowMeta = inputRowMeta();
    meta.getFields(rowMeta, "Group by", null, null, new Variables(), null);

    assertEquals(3, rowMeta.size());
    assertEquals("grp", rowMeta.getValueMeta(0).getName());
    assertEquals("sum_amount", rowMeta.getValueMeta(1).getName());
    assertEquals(IValueMeta.TYPE_NUMBER, rowMeta.getValueMeta(1).getType());
    assertEquals("cnt", rowMeta.getValueMeta(2).getName());
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(2).getType());
  }

  @Test
  void testGetFieldsWithPassAllRowsAndLineNr() {
    GroupByMeta meta = new GroupByMeta();
    meta.setDefault();
    meta.setPassAllRows(true);
    meta.setAddingLineNrInGroup(true);
    meta.setLineNrInGroupField("linenr");
    meta.getGroupingFields().add(new GroupingField("grp"));
    meta.getAggregations()
        .add(
            new Aggregation(
                "sum_amount",
                "amount",
                Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_SUM),
                null));

    IRowMeta rowMeta = inputRowMeta();
    meta.getFields(rowMeta, "Group by", null, null, new Variables(), null);

    assertTrue(rowMeta.indexOfValue("grp") >= 0);
    assertTrue(rowMeta.indexOfValue("amount") >= 0);
    assertTrue(rowMeta.indexOfValue("skip") >= 0);
    assertTrue(rowMeta.indexOfValue("sum_amount") >= 0);
    assertTrue(rowMeta.indexOfValue("linenr") >= 0);
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.searchValueMeta("linenr").getType());
  }

  @Test
  void testSupportsMultiCopyExecution() {
    assertFalse(new GroupByMeta().supportsMultiCopyExecution());
  }

  private void compareMetas(GroupByMeta meta1, GroupByMeta meta2) {
    assertEquals(meta1.getGroupingFields().size(), meta2.getGroupingFields().size());
    for (int i = 0; i < meta1.getGroupingFields().size(); i++) {
      assertEquals(meta1.getGroupingFields().get(i), meta2.getGroupingFields().get(i));
    }
    assertEquals(meta1.getAggregations().size(), meta2.getAggregations().size());
    for (int i = 0; i < meta1.getAggregations().size(); i++) {
      assertEquals(meta1.getAggregations().get(i), meta2.getAggregations().get(i));
    }
    assertEquals(meta1.isPassAllRows(), meta2.isPassAllRows());
    assertEquals(meta1.isAddingLineNrInGroup(), meta2.isAddingLineNrInGroup());
    assertEquals(meta1.getLineNrInGroupField(), meta2.getLineNrInGroupField());
    assertEquals(meta1.getDirectory(), meta2.getDirectory());
    assertEquals(meta1.getPrefix(), meta2.getPrefix());
    assertEquals(meta1.isAlwaysGivingBackOneRow(), meta2.isAlwaysGivingBackOneRow());
    assertEquals(meta1.isAggregateIgnored(), meta2.isAggregateIgnored());
    assertEquals(meta1.getAggregateIgnoredField(), meta2.getAggregateIgnoredField());
  }

  private GroupByMeta generateTestMeta() {
    GroupByMeta meta = new GroupByMeta();
    meta.setGroupingFields(List.of(new GroupingField("field1")));
    meta.setAggregations(
        List.of(
            new Aggregation("field02", "subject02", getDesc("SUM"), null),
            new Aggregation("field03", "subject03", getDesc("AVERAGE"), null),
            new Aggregation("field04", "subject04", getDesc("MEDIAN"), null),
            new Aggregation("field05", "subject05", getDesc("PERCENTILE"), null),
            new Aggregation("field06", "subject06", getDesc("MIN"), null),
            new Aggregation("field07", "subject07", getDesc("MAX"), null),
            new Aggregation("field08", "subject08", getDesc("COUNT_ALL"), null),
            new Aggregation("field09", "subject09", getDesc("CONCAT_COMMA"), null),
            new Aggregation("field10", "subject10", getDesc("FIRST"), null),
            new Aggregation("field11", "subject11", getDesc("LAST"), null),
            new Aggregation("field12", "subject12", getDesc("FIRST_INCL_NULL"), null),
            new Aggregation("field13", "subject13", getDesc("LAST_INCL_NULL"), null),
            new Aggregation("field14", "subject14", getDesc("CUM_SUM"), null),
            new Aggregation("field15", "subject15", getDesc("CUM_AVG"), null),
            new Aggregation("field16", "subject16", getDesc("STD_DEV"), null),
            new Aggregation("field17", "subject17", getDesc("CONCAT_STRING"), "value17"),
            new Aggregation("field18", "subject18", getDesc("COUNT_DISTINCT"), null),
            new Aggregation("field19", "subject19", getDesc("COUNT_ANY"), null),
            new Aggregation("field20", "subject20", getDesc("COUNT_ANY"), null),
            new Aggregation("field21", "subject21", getDesc("STD_DEV_SAMPLE"), "value21"),
            new Aggregation("field22", "subject22", getDesc("PERCENTILE_NEAREST_RANK"), "value22"),
            new Aggregation("field23", "subject23", getDesc("CONCAT_STRING_CRLF"), null),
            new Aggregation("field24", "subject23", getDesc("CONCAT_DISTINCT"), "value24"),
            new Aggregation("field25", "subject25", getDesc("MOVING_AVG"), "5", "order_field_25")));
    meta.setPassAllRows(true);
    meta.setAlwaysGivingBackOneRow(true);
    meta.setDirectory("directory");
    meta.setPrefix("prefix");
    meta.setAddingLineNrInGroup(true);
    meta.setLineNrInGroupField("lineNr");
    meta.setAggregateIgnored(true);
    meta.setAggregateIgnoredField("skip");
    return meta;
  }

  private IRowMeta inputRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("grp"));
    rowMeta.addValueMeta(new ValueMetaNumber("amount"));
    rowMeta.addValueMeta(new ValueMetaBoolean("skip"));
    return rowMeta;
  }

  private String getDesc(String label) {
    int type = Aggregation.getTypeCodeFromLabel(label);
    return Aggregation.getTypeDescLongFromCode(type);
  }
}
