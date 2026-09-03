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

package org.apache.hop.pipeline.transforms.javafilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link JavaFilterCondition}, in particular for conditions that run on streams
 * which don't have every field the condition talks about.
 */
class JavaFilterConditionTest {

  @BeforeAll
  static void initPlugins() throws Exception {
    HopLogStore.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (String cls :
        new String[] {
          ValueMetaString.class.getName(),
          ValueMetaInteger.class.getName(),
          org.apache.hop.core.row.value.ValueMetaDate.class.getName(),
          org.apache.hop.core.row.value.ValueMetaNumber.class.getName(),
          org.apache.hop.core.row.value.ValueMetaTimestamp.class.getName()
        }) {
      registry.registerPluginClass(cls, ValueMetaPluginType.class, ValueMetaPlugin.class);
    }
  }

  private static RowMeta rowWithGroup() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("department"));
    return rowMeta;
  }

  private static RowMeta rowWithoutGroup() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  // ---------------------------------------------------------------- field binding

  @Test
  void compile_bindsOnlyTheFieldsTheConditionMentions() throws HopException {
    JavaFilterCondition condition = JavaFilterCondition.compile(rowWithGroup(), "id > 2");

    assertEquals(List.of("id"), condition.getBoundFieldNames());
  }

  @Test
  void compile_conditionWithoutFields_bindsNothing() throws HopException {
    JavaFilterCondition condition = JavaFilterCondition.compile(rowWithGroup(), "true");

    assertTrue(condition.getBoundFieldNames().isEmpty());
  }

  // ---------------------------------------------------------------- row helper

  @Test
  void rowHelper_reportsAFieldThatIsPresent() throws HopException {
    JavaFilterCondition condition =
        JavaFilterCondition.compile(rowWithGroup(), "row.exists(\"department\")");

    assertTrue(condition.evaluate(rowWithGroup(), new Object[] {1L, "A"}));
  }

  @Test
  void rowHelper_missingFieldDoesNotBreakTheCondition() throws HopException {
    // The very case a plain field reference can not handle: department is not in this stream.
    JavaFilterCondition condition =
        JavaFilterCondition.compile(
            rowWithoutGroup(),
            "!row.exists(\"department\") || \"A\".equals(row.getString(\"department\"))");

    assertTrue(condition.evaluate(rowWithoutGroup(), new Object[] {1L, "alice"}));
    assertTrue(condition.getBoundFieldNames().isEmpty());
  }

  @Test
  void rowHelper_filtersOnTheFieldWhenTheStreamHasIt() throws HopException {
    JavaFilterCondition condition =
        JavaFilterCondition.compile(
            rowWithGroup(),
            "!row.exists(\"department\") || \"A\".equals(row.getString(\"department\"))");

    assertTrue(condition.evaluate(rowWithGroup(), new Object[] {1L, "A"}));
    assertFalse(condition.evaluate(rowWithGroup(), new Object[] {2L, "B"}));
  }

  @Test
  void rowHelper_readsFieldsByNameWithTheirType() throws HopException {
    JavaFilterCondition condition =
        JavaFilterCondition.compile(
            rowWithGroup(),
            "row.getInteger(\"id\") != null && row.getInteger(\"id\").longValue() > 2");

    assertTrue(condition.evaluate(rowWithGroup(), new Object[] {3L, "A"}));
    assertFalse(condition.evaluate(rowWithGroup(), new Object[] {1L, "A"}));
  }

  @Test
  void rowHelper_absentFieldReadsAsNullAndIsNull() throws HopException {
    JavaFilterCondition condition =
        JavaFilterCondition.compile(
            rowWithoutGroup(),
            "row.getString(\"department\") == null && row.isNull(\"department\")");

    assertTrue(condition.evaluate(rowWithoutGroup(), new Object[] {1L, "alice"}));
  }

  // ---------------------------------------------------------------- name conflict

  @Test
  void aStreamFieldNamedRow_conditionUsingTheHelper_isRejectedWithAClearMessage() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("row"));

    HopException e =
        assertThrows(
            HopException.class,
            () -> JavaFilterCondition.compile(rowMeta, "row.exists(\"department\")"));

    assertTrue(e.getMessage().contains("conflicts with the built-in"));
  }

  @Test
  void aStreamFieldNamedRow_conditionWithoutTheHelper_stillWorks() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("row"));

    JavaFilterCondition condition = JavaFilterCondition.compile(rowMeta, "\"a\".equals(row)");

    assertTrue(condition.evaluate(rowMeta, new Object[] {"a"}));
    assertFalse(condition.isRowHelperAvailable());
  }

  // ---------------------------------------------------------------- validation

  @Test
  void validate_rejectsAConditionThatDoesNotReturnABoolean() {
    assertThrows(
        HopException.class,
        () -> JavaFilterCondition.validate(rowWithGroup(), "\"not a boolean\""));
  }

  @Test
  void validate_acceptsABooleanCondition() throws HopException {
    assertEquals(
        List.of("id"), JavaFilterCondition.validate(rowWithGroup(), "id > 2").getBoundFieldNames());
  }

  @Test
  void compile_reportsAConditionThatDoesNotCompile() {
    HopException e =
        assertThrows(
            HopException.class, () -> JavaFilterCondition.compile(rowWithGroup(), "id >>> "));

    assertTrue(e.getMessage().contains("could not be compiled"));
  }

  @Test
  void evaluate_nonBooleanResultIsReported() throws HopException {
    JavaFilterCondition condition = JavaFilterCondition.compile(rowWithGroup(), "\"text\"");

    HopException e =
        assertThrows(
            HopException.class, () -> condition.evaluate(rowWithGroup(), new Object[] {1L, "A"}));

    assertTrue(e.getMessage().contains("must be a boolean"));
  }

  // ---------------------------------------------------------------- field types

  @Test
  void aTimestampFieldIsUsedAsATimestamp() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaTimestamp("ts"));

    JavaFilterCondition condition = JavaFilterCondition.compile(rowMeta, "ts.getTime() > 1000");

    assertTrue(condition.evaluate(rowMeta, new Object[] {new java.sql.Timestamp(2000L)}));
    assertFalse(condition.evaluate(rowMeta, new Object[] {new java.sql.Timestamp(500L)}));
  }

  @Test
  void anInternetAddressFieldIsUsedAsAnInetAddress() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaInternetAddress("address"));

    JavaFilterCondition condition =
        JavaFilterCondition.compile(rowMeta, "address.getHostAddress().startsWith(\"10.\")");

    assertTrue(
        condition.evaluate(rowMeta, new Object[] {java.net.InetAddress.getByName("10.0.0.1")}));
    assertFalse(
        condition.evaluate(rowMeta, new Object[] {java.net.InetAddress.getByName("192.168.0.1")}));
  }

  @Test
  void aJsonFieldIsUsedAsAJsonNode() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaJson("doc"));

    JavaFilterCondition condition =
        JavaFilterCondition.compile(rowMeta, "doc != null && doc.get(\"a\").asInt() == 1");

    Object node =
        new com.fasterxml.jackson.databind.ObjectMapper().readTree("{\"a\": 1, \"b\": 2}");
    assertTrue(condition.evaluate(rowMeta, new Object[] {node}));
  }

  @Test
  void aTypeWithoutAJavaClassOfItsOwnArrivesAsAnObject() throws HopException {
    // A Serializable field has no native class, it used to be declared as a String and then blow up
    // on the first row when the value turned out to be something else.
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaSerializable("payload"));

    JavaFilterCondition condition =
        JavaFilterCondition.compile(rowMeta, "payload != null && payload.toString().length() > 0");

    assertTrue(condition.evaluate(rowMeta, new Object[] {new java.util.ArrayList<>(List.of("a"))}));
    assertFalse(condition.evaluate(rowMeta, new Object[] {null}));
  }

  // ---------------------------------------------------------------- comments

  @Test
  void defaultCondition_compilesAndKeepsEveryRow() throws HopException {
    JavaFilterCondition condition =
        JavaFilterCondition.validate(rowWithGroup(), JavaFilterMeta.DEFAULT_CONDITION);

    assertTrue(condition.evaluate(rowWithGroup(), new Object[] {1L, "A"}));
  }

  @Test
  void aCommentedExampleIsNotAUseOfTheRowHelper() throws HopException {
    // An example in a comment should not be read as a use of the helper by a stream that has a
    // field named "row".
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("row"));

    JavaFilterCondition condition =
        JavaFilterCondition.validate(rowMeta, "/* e.g. row.exists(\"department\") */ true");

    assertTrue(condition.evaluate(rowMeta, new Object[] {"a"}));
  }

  @Test
  void commentsDoNotBindFields() throws HopException {
    JavaFilterCondition condition =
        JavaFilterCondition.compile(rowWithGroup(), "/* id > 2 */ true // department");

    assertTrue(condition.getBoundFieldNames().isEmpty());
  }

  @Test
  void withoutComments_leavesCodeAndLiteralsAlone() {
    assertEquals(
        "\"a // b\".equals(department)",
        JavaFilterCondition.withoutComments("\"a // b\".equals(department)"));
    String lineComment = JavaFilterCondition.withoutComments("true // comment");
    assertEquals("true", lineComment.trim());
    assertEquals("true // comment".length(), lineComment.length());

    String blockComment = JavaFilterCondition.withoutComments("a /* x */b");
    assertEquals("a /* x */b".length(), blockComment.length());
    assertEquals("a", blockComment.substring(0, 1));
    assertEquals("b", blockComment.substring(blockComment.length() - 1));
    assertTrue(blockComment.substring(1, blockComment.length() - 1).isBlank());
    assertEquals("'\"' == c", JavaFilterCondition.withoutComments("'\"' == c"));
  }

  // ---------------------------------------------------------------- built-in functions

  @Test
  void builtInFunctionsAreAvailable() throws HopException {
    JavaFilterCondition condition =
        JavaFilterCondition.compile(
            rowWithGroup(), "\"A\".equals(HopFunctions.nvl(department, \"A\"))");

    assertTrue(condition.evaluate(rowWithGroup(), new Object[] {1L, ""}));
    assertFalse(condition.evaluate(rowWithGroup(), new Object[] {1L, "B"}));
  }
}
