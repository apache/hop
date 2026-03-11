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

package org.apache.hop.pipeline.transforms.terafast;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FastloadControlBuilderTest {

  private FastloadControlBuilder builder;

  @BeforeEach
  void setUp() {
    builder = new FastloadControlBuilder();
  }

  @Test
  void testNewline() {
    builder.newline();
    String out = builder.toString();
    assertTrue(out.endsWith(";" + System.lineSeparator()));
  }

  @Test
  void testLogonWithConnectionString() {
    builder.logon("localtd/user,pass");
    String out = builder.toString();
    assertTrue(out.contains("LOGON localtd/user,pass"));
  }

  @Test
  void testLogonWithConnectionStringThrowsWhenBlank() {
    assertThrows(IllegalArgumentException.class, () -> builder.logon(""));
    assertThrows(IllegalArgumentException.class, () -> builder.logon("   "));
    assertThrows(IllegalArgumentException.class, () -> builder.logon(null));
  }

  @Test
  void testLogonWithHostUserPassword() {
    builder.logon("localtd", "user", "pass");
    String out = builder.toString();
    assertTrue(out.contains("LOGON localtd/user,pass"));
  }

  @Test
  void testLogonWithHostUserPasswordThrowsWhenInvalid() {
    assertThrows(IllegalArgumentException.class, () -> builder.logon("", "u", "p"));
    assertThrows(IllegalArgumentException.class, () -> builder.logon("h", "", "p"));
    assertThrows(IllegalArgumentException.class, () -> builder.logon("h", "u", null));
  }

  @Test
  void testSetRecordFormat() {
    builder.setRecordFormat(FastloadControlBuilder.RECORD_VARTEXT);
    String out = builder.toString();
    assertTrue(out.contains("SET RECORD " + FastloadControlBuilder.RECORD_VARTEXT));
  }

  @Test
  void testSetRecordFormatThrowsWhenBlank() {
    assertThrows(IllegalArgumentException.class, () -> builder.setRecordFormat(""));
    assertThrows(IllegalArgumentException.class, () -> builder.setRecordFormat(null));
  }

  @Test
  void testSetSessions() {
    builder.setSessions(4);
    String out = builder.toString();
    assertTrue(out.contains("SESSIONS 4"));
  }

  @Test
  void testSetSessionsThrowsWhenZeroOrNegative() {
    assertThrows(IllegalArgumentException.class, () -> builder.setSessions(0));
    assertThrows(IllegalArgumentException.class, () -> builder.setSessions(-1));
  }

  @Test
  void testSetErrorLimit() {
    builder.setErrorLimit(100);
    String out = builder.toString();
    assertTrue(out.contains("ERRLIMIT 100"));
  }

  @Test
  void testSetErrorLimitThrowsWhenZeroOrNegative() {
    assertThrows(IllegalArgumentException.class, () -> builder.setErrorLimit(0));
    assertThrows(IllegalArgumentException.class, () -> builder.setErrorLimit(-1));
  }

  @Test
  void testDefine() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("col1", 10, 0));
    rowMeta.addValueMeta(new ValueMetaInteger("col2", 8, 0));
    List<String> tableFieldList = Arrays.asList("col1", "col2");

    builder.define(rowMeta, tableFieldList, "/path/data.dat");
    String out = builder.toString();

    assertTrue(out.contains("DEFINE "));
    assertTrue(out.contains("col1"));
    assertTrue(out.contains("col2"));
    assertTrue(out.contains("FILE=/path/data.dat"));
    assertTrue(out.contains("VARCHAR("));
    assertTrue(out.contains("NEWLINECHAR"));
  }

  @Test
  void testDefineWithDateField() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaDate("dt"));
    List<String> tableFieldList = Arrays.asList("dt");

    builder.define(rowMeta, tableFieldList, "data.dat");
    String out = builder.toString();

    assertTrue(out.contains("dt"));
    assertTrue(out.contains("VARCHAR"));
    assertTrue(out.contains("?")); // DEFAULT_NULL_VALUE
  }

  @Test
  void testDefineThrowsWhenNull() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("a", 5, 0));
    assertThrows(
        IllegalArgumentException.class, () -> builder.define(null, Arrays.asList("a"), "f.dat"));
    assertThrows(
        IllegalArgumentException.class, () -> builder.define(rowMeta, Arrays.asList("a"), null));
  }

  @Test
  void testInsert() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("col1", 10, 0));
    rowMeta.addValueMeta(new ValueMetaInteger("col2", 8, 0));
    List<String> tableFieldList = Arrays.asList("col1", "col2");

    builder.insert(rowMeta, tableFieldList, "mytable");
    String out = builder.toString();

    assertTrue(out.contains("INSERT INTO mytable("));
    assertTrue(out.contains(":col1"));
    assertTrue(out.contains(":col2"));
  }

  @Test
  void testInsertWithDateField() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaDate("dt"));
    List<String> tableFieldList = Arrays.asList("dt");

    builder.insert(rowMeta, tableFieldList, "t");
    String out = builder.toString();

    assertTrue(
        out.contains(":dt(DATE, FORMAT '" + FastloadControlBuilder.DEFAULT_DATE_FORMAT + "')"));
  }

  @Test
  void testInsertThrowsWhenNull() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("a", 5, 0));
    assertThrows(
        IllegalArgumentException.class, () -> builder.insert(null, Arrays.asList("a"), "t"));
    assertThrows(
        IllegalArgumentException.class, () -> builder.insert(rowMeta, Arrays.asList("a"), null));
  }

  @Test
  void testShow() {
    builder.show();
    assertTrue(builder.toString().contains("SHOW"));
  }

  @Test
  void testEndLoading() {
    builder.endLoading();
    assertTrue(builder.toString().contains("END LOADING"));
  }

  @Test
  void testBeginLoadingWithTableOnly() {
    builder.beginLoading("", "mytable");
    String out = builder.toString();
    assertTrue(out.contains("BEGIN LOADING mytable"));
    assertTrue(out.contains("ERRORFILES " + FastloadControlBuilder.DEFAULT_ERROR_TABLE1));
    assertTrue(out.contains(FastloadControlBuilder.DEFAULT_ERROR_TABLE2));
  }

  @Test
  void testBeginLoadingWithSchemaAndTable() {
    builder.beginLoading("myschema", "mytable");
    String out = builder.toString();
    assertTrue(out.contains("BEGIN LOADING mytable"));
    assertTrue(out.contains("myschema." + FastloadControlBuilder.DEFAULT_ERROR_TABLE1));
    assertTrue(out.contains("myschema." + FastloadControlBuilder.DEFAULT_ERROR_TABLE2));
  }

  @Test
  void testBeginLoadingThrowsWhenTableBlank() {
    assertThrows(IllegalArgumentException.class, () -> builder.beginLoading("", ""));
    assertThrows(IllegalArgumentException.class, () -> builder.beginLoading("", "  "));
  }

  @Test
  void testLine() {
    builder.line("SOME COMMAND");
    assertTrue(builder.toString().contains("SOME COMMAND"));
  }

  @Test
  void testLineIgnoresBlank() {
    builder.line("x");
    builder.line("");
    builder.line("   ");
    builder.line("y");
    String out = builder.toString();
    assertTrue(out.contains("x"));
    assertTrue(out.contains("y"));
    // blank lines do not append content
    assertTrue(out.indexOf("x") < out.indexOf("y"));
  }

  @Test
  void testLogoff() {
    builder.logoff();
    assertTrue(builder.toString().contains("LOGOFF"));
  }

  @Test
  void testFullSequence() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("a", 5, 0));
    List<String> tableFieldList = Arrays.asList("a");

    builder
        .logon("host/user,pass")
        .setRecordFormat(FastloadControlBuilder.RECORD_VARTEXT)
        .setSessions(2)
        .setErrorLimit(25)
        .define(rowMeta, tableFieldList, "data.dat")
        .show()
        .beginLoading("", "target")
        .insert(rowMeta, tableFieldList, "target")
        .endLoading()
        .logoff();

    String out = builder.toString();
    assertTrue(out.contains("LOGON host/user,pass"));
    assertTrue(out.contains("SET RECORD"));
    assertTrue(out.contains("SESSIONS 2"));
    assertTrue(out.contains("ERRLIMIT 25"));
    assertTrue(out.contains("DEFINE"));
    assertTrue(out.contains("SHOW"));
    assertTrue(out.contains("BEGIN LOADING target"));
    assertTrue(out.contains("INSERT INTO target"));
    assertTrue(out.contains("END LOADING"));
    assertTrue(out.contains("LOGOFF"));
  }
}
