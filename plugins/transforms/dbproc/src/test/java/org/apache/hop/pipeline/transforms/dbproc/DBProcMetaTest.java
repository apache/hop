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
package org.apache.hop.pipeline.transforms.dbproc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSetMetaData;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class DBProcMetaTest {
  @BeforeAll
  static void setUp() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void testSerialization() throws Exception {
    DBProcMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/db-proc-transform.xml", DBProcMeta.class);

    assertEquals("addCount", meta.getProcedure());
    assertEquals("unit-test-db", meta.getConnection());
    assertEquals("count", meta.getResultName());
    assertEquals("Integer", meta.getResultType());
    assertFalse(meta.isResultRows());
    assertTrue(meta.isAutoCommit());
    assertEquals(1, meta.getArguments().size());
    assertEquals("value", meta.getArguments().get(0).getName());
    assertEquals("IN", meta.getArguments().get(0).getDirection());
    assertEquals("String", meta.getArguments().get(0).getType());
    assertTrue(meta.getResultFields() == null || meta.getResultFields().isEmpty());
  }

  @Test
  void rowResultSerializationKeepsTheScalarNameAndTheFieldList() throws Exception {
    DBProcMeta meta =
        TransformSerializationTestUtil.testSerialization("/db-proc-rows.xml", DBProcMeta.class);

    assertEquals("list_customers", meta.getProcedure());
    assertEquals("count", meta.getResultName());
    assertEquals(DBProcMeta.RESULT_TYPE_ROW, meta.getResultType());
    assertTrue(meta.isResultRows());
    assertFalse(meta.isAutoCommit());
    assertEquals(2, meta.getArguments().size());
    assertEquals("OUT", meta.getArguments().get(1).getDirection());
    assertEquals(2, meta.getResultFields().size());
    assertEquals("id", meta.getResultFields().get(0).getName());
    assertEquals("Integer", meta.getResultFields().get(0).getType());
    assertEquals("#", meta.getResultFields().get(0).getFormat());
    assertEquals(9, meta.getResultFields().get(0).getLength());
    assertEquals(0, meta.getResultFields().get(0).getPrecision());
    assertEquals("customer_name", meta.getResultFields().get(1).getName());
    assertEquals(-1, meta.getResultFields().get(1).getLength());
    assertEquals(-1, meta.getResultFields().get(1).getPrecision());

    meta.getResult().setType("row");
    assertTrue(meta.isResultRows());
  }

  @Test
  void getFieldsAddsScalarThenOutAndRowColumnsInsteadOfTheScalar() throws Exception {
    DBProcMeta meta = new DBProcMeta();
    meta.setDefault();
    meta.getResult().setName("count");
    meta.getResult().setType("Integer");
    DBProcMeta.ProcArgument out = new DBProcMeta.ProcArgument();
    out.setName("total");
    out.setDirection("OUT");
    out.setType("Number");
    meta.getArguments().add(out);

    RowMeta scalar = new RowMeta();
    scalar.addValueMeta(new ValueMetaString("value"));
    meta.getFields(scalar, "proc", null, null, new Variables(), null);

    assertEquals(3, scalar.size());
    assertEquals("value", scalar.getValueMeta(0).getName());
    assertEquals("count", scalar.getValueMeta(1).getName());
    assertEquals(IValueMeta.TYPE_INTEGER, scalar.getValueMeta(1).getType());
    assertEquals("total", scalar.getValueMeta(2).getName());
    assertEquals("proc", scalar.getValueMeta(1).getOrigin());

    meta.setResultType(DBProcMeta.RESULT_TYPE_ROW);
    DBProcField id = new DBProcField();
    id.setName("${COL}");
    id.setType("Integer");
    id.setLength(9);
    id.setPrecision(0);
    id.setFormat("${MASK}");
    DBProcField blank = new DBProcField();
    blank.setName("");
    meta.getResultFields().add(id);
    meta.getResultFields().add(blank);
    Variables variables = new Variables();
    variables.setVariable("COL", "id");
    variables.setVariable("MASK", "#");

    RowMeta rows = new RowMeta();
    rows.addValueMeta(new ValueMetaString("value"));
    meta.getFields(rows, "proc", null, null, variables, null);

    assertEquals(3, rows.size());
    assertEquals("id", rows.getValueMeta(1).getName());
    assertEquals(IValueMeta.TYPE_INTEGER, rows.getValueMeta(1).getType());
    assertEquals(9, rows.getValueMeta(1).getLength());
    assertEquals(0, rows.getValueMeta(1).getPrecision());
    assertEquals("#", rows.getValueMeta(1).getConversionMask());
    assertEquals("total", rows.getValueMeta(2).getName());
  }

  @Test
  void unknownResultFieldTypeBecomesString() throws Exception {
    DBProcField field = new DBProcField();
    field.setName("value");
    field.setType("Row");
    assertEquals(IValueMeta.TYPE_STRING, field.toValueMeta("origin", null).getType());
    assertEquals(-1, new DBProcField().getLength());
    assertEquals(-1, new DBProcField().getPrecision());
  }

  @Test
  void resultTypeNamesEndWithASingleRowEntry() {
    List<String> names = new DBProcMeta().getResultTypeNames(null, null);
    assertEquals(DBProcMeta.RESULT_TYPE_ROW, names.get(names.size() - 1));
    assertEquals(1, names.stream().filter(DBProcMeta.RESULT_TYPE_ROW::equalsIgnoreCase).count());
    assertFalse(names.get(0).equalsIgnoreCase(DBProcMeta.RESULT_TYPE_ROW));
  }

  @Test
  void buildOutputRowKeepsTheScalarPathAndCopiesResultRows() {
    Object[] row = new Object[] {"in", null, null};
    DBProcMeta.ProcArgument in = new DBProcMeta.ProcArgument();
    in.setDirection("IN");
    DBProcMeta.ProcArgument out = new DBProcMeta.ProcArgument();
    out.setDirection("out");
    List<DBProcMeta.ProcArgument> arguments = List.of(in, out);
    int[] argnrs = new int[] {0, -1};

    Object[] scalar =
        DBProc.buildOutputRow(row, 1, 3, new Object[] {7L, 42L}, argnrs, arguments, true, 0, false);
    assertSame(row, scalar);
    assertEquals("in", scalar[0]);
    assertEquals(7L, scalar[1]);
    assertEquals(42L, scalar[2]);

    Object[] wide = new Object[] {"old"};
    DBProcMeta.ProcArgument inout = new DBProcMeta.ProcArgument();
    inout.setDirection("INOUT");
    Object[] replaced =
        DBProc.buildOutputRow(
            wide, 1, 1, new Object[] {"new"}, new int[] {0}, List.of(inout), false, 0, true);
    assertNotSame(wide, replaced);
    assertEquals("new", replaced[0]);
    assertEquals("old", wide[0]);

    Object[] template =
        DBProc.buildOutputRow(
            new Object[] {"in"}, 1, 3, new Object[] {42L}, argnrs, arguments, false, 1, true);
    assertNull(template[1]);
    assertEquals(42L, template[2]);
    Object[] copy = RowDataUtil.createResizedCopy(template, 3);
    copy[1] = "row";
    assertNull(template[1]);
    assertEquals(42L, copy[2]);
  }

  @Test
  void resultColumnsMatchLabelThenNameIgnoringCase() throws Exception {
    ResultSetMetaData metadata = org.mockito.Mockito.mock(ResultSetMetaData.class);
    org.mockito.Mockito.when(metadata.getColumnCount()).thenReturn(3);
    org.mockito.Mockito.when(metadata.getColumnLabel(1)).thenReturn("ID");
    org.mockito.Mockito.when(metadata.getColumnName(1)).thenReturn("id_col");
    org.mockito.Mockito.when(metadata.getColumnLabel(2)).thenReturn("");
    org.mockito.Mockito.when(metadata.getColumnName(2)).thenReturn("Name");
    org.mockito.Mockito.when(metadata.getColumnLabel(3)).thenReturn("ID");
    org.mockito.Mockito.when(metadata.getColumnName(3)).thenReturn("other");

    String[] columns = DBProc.resultColumnNames(metadata);
    assertEquals("ID", columns[0]);
    assertEquals("Name", columns[1]);

    DBProcField id = new DBProcField();
    id.setName("Id");
    DBProcField missing = new DBProcField();
    missing.setName("city");
    DBProcField fromVariable = new DBProcField();
    fromVariable.setName("${COL}");
    Variables variables = new Variables();
    variables.setVariable("COL", "name");

    int[] indexes =
        DBProc.resultColumnIndexes(columns, List.of(id, missing, fromVariable), variables);
    assertEquals(0, indexes[0]);
    assertEquals(-1, indexes[1]);
    assertEquals(1, indexes[2]);
  }
}
