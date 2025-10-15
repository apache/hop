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
 *
 */

package org.apache.hop.pipeline.transforms;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transforms.groupby.Aggregation;
import org.apache.hop.pipeline.transforms.groupby.GroupByMeta;
import org.apache.hop.pipeline.transforms.groupby.GroupingField;
import org.junit.jupiter.api.Test;

class GroupByMetaTest {

  @Test
  void testClone() throws Exception {
    GroupByMeta meta1 = generateTestMeta();
    GroupByMeta meta2 = meta1.clone();

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

  public void compareMetas(GroupByMeta meta1, GroupByMeta meta2) {
    assertEquals(meta1.getGroupingFields().size(), meta2.getGroupingFields().size());
    for (int i = 0; i < meta1.getGroupingFields().size(); i++) {
      GroupingField field1 = meta1.getGroupingFields().get(i);
      GroupingField field2 = meta2.getGroupingFields().get(i);
      assertEquals(field1, field2);
    }
    assertEquals(meta1.getAggregations().size(), meta2.getAggregations().size());
    for (int i = 0; i < meta1.getAggregations().size(); i++) {
      Aggregation agg1 = meta1.getAggregations().get(i);
      Aggregation agg2 = meta2.getAggregations().get(i);
      assertEquals(agg1, agg2);
    }
    assertEquals(meta1.isPassAllRows(), meta2.isPassAllRows());
    assertEquals(meta1.isAddingLineNrInGroup(), meta2.isAddingLineNrInGroup());
    assertEquals(meta1.getLineNrInGroupField(), meta2.getLineNrInGroupField());
    assertEquals(meta1.getDirectory(), meta2.getDirectory());
    assertEquals(meta1.getPrefix(), meta2.getPrefix());
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
            new Aggregation("field24", "subject23", getDesc("CONCAT_DISTINCT"), "value24")));
    meta.setPassAllRows(true);
    meta.setAlwaysGivingBackOneRow(true);
    meta.setDirectory("directory");
    meta.setPrefix("prefix");
    meta.setAddingLineNrInGroup(true);
    meta.setLineNrInGroupField("lineNr");
    return meta;
  }

  private String getDesc(String label) {
    int type = Aggregation.getTypeCodeFromLabel(label);
    return Aggregation.getTypeDescLongFromCode(type);
  }
}
