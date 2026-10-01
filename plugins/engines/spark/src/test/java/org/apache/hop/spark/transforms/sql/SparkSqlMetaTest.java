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

package org.apache.hop.spark.transforms.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.spark.transforms.io.SparkField;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SparkSqlMetaTest {

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void defaultViewNameReplacesCharactersThatAreNotSqlSafe() {
    assertEquals("orders", SparkSqlMeta.defaultViewName("orders"));
    assertEquals("Read_orders__raw_", SparkSqlMeta.defaultViewName("Read orders (raw)"));
    assertEquals("a_b_c", SparkSqlMeta.defaultViewName("a-b.c"));
    assertEquals("keep_underscores", SparkSqlMeta.defaultViewName("keep_underscores"));
  }

  @Test
  void defaultViewNameEscapesALeadingDigit() {
    assertEquals("_1_orders", SparkSqlMeta.defaultViewName("1 orders"));
    assertEquals("_2024", SparkSqlMeta.defaultViewName("2024"));
  }

  @Test
  void defaultViewNameHandlesNullAndEmpty() {
    assertNull(SparkSqlMeta.defaultViewName(null));
    assertNull(SparkSqlMeta.defaultViewName(""));
  }

  @Test
  void findViewNameOverrideIgnoresBlankAndUnknownEntries() {
    SparkSqlMeta meta = new SparkSqlMeta();
    meta.setViews(
        List.of(
            new SparkSqlView("orders", "o"),
            new SparkSqlView("customers", ""),
            new SparkSqlView("blank", null)));

    assertEquals("o", meta.findViewNameOverride("orders"));
    assertNull(meta.findViewNameOverride("customers"));
    assertNull(meta.findViewNameOverride("blank"));
    assertNull(meta.findViewNameOverride("unknown"));
    assertNull(meta.findViewNameOverride(null));
  }

  @Test
  void getFieldsReplacesTheIncomingRowWithTheDeclaredFields() throws Exception {
    SparkSqlMeta meta = new SparkSqlMeta();
    meta.setFields(List.of(new SparkField("name", "String"), new SparkField("total", "Integer")));

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("carried_over"));

    meta.getFields(rowMeta, "sql", null, null, new Variables(), new MemoryMetadataProvider());

    assertEquals(2, rowMeta.size());
    assertEquals("name", rowMeta.getValueMeta(0).getName());
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.getValueMeta(0).getType());
    assertEquals("total", rowMeta.getValueMeta(1).getName());
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(1).getType());
  }

  @Test
  void getFieldsSkipsUnnamedFields() throws Exception {
    SparkSqlMeta meta = new SparkSqlMeta();
    meta.setFields(List.of(new SparkField("", "String"), new SparkField("kept", "String")));

    IRowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "sql", null, null, new Variables(), new MemoryMetadataProvider());

    assertEquals(1, rowMeta.size());
    assertEquals("kept", rowMeta.getValueMeta(0).getName());
  }
}
