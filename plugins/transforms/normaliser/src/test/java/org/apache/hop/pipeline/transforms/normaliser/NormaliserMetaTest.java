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
package org.apache.hop.pipeline.transforms.normaliser;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class NormaliserMetaTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void testSerialization() throws Exception {
    NormaliserMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/normaliser-transform.xml", NormaliserMeta.class);

    assertEquals("type", meta.getTypeField());
    assertEquals("content", meta.getValueField());
    assertEquals(4, meta.getNormaliserFields().size());
    NormaliserField last = meta.getNormaliserFields().get(3);
    assertEquals("fieldD", last.getName());
    assertEquals("D", last.getValue());
    assertEquals("field", last.getNorm());
  }

  /** The type field is as long as the longest type it holds. */
  @Test
  void theTypeFieldIsAsLongAsTheLongestType() throws Exception {
    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaInteger("i1"));
    row.addValueMeta(new ValueMetaInteger("i2"));

    meta(field("i1", "short", "value"), field("i2", "much longer", "value"))
        .getFields(row, "normaliser", null, null, new Variables(), null);

    IValueMeta type = row.searchValueMeta("typefield");
    assertEquals(IValueMeta.TYPE_STRING, type.getType());
    assertEquals("much longer".length(), type.getLength());
  }

  @Test
  void aNormalisedFieldOfOneTypeTakesThatTypeFromItsFirstField() throws Exception {
    IRowMeta row = new RowMeta();
    ValueMetaInteger first = new ValueMetaInteger("i1");
    first.setLength(7);
    row.addValueMeta(first);
    row.addValueMeta(new ValueMetaInteger("i2"));

    meta(field("i1", "A", "value"), field("i2", "B", "value"))
        .getFields(row, "normaliser", null, null, new Variables(), null);

    assertArrayEquals(new String[] {"typefield", "value"}, row.getFieldNames());
    IValueMeta value = row.getValueMeta(1);
    assertEquals(IValueMeta.TYPE_INTEGER, value.getType());
    assertEquals(7, value.getLength());
    assertEquals("normaliser", value.getOrigin());
  }

  /** Issue #3636: the first field used to decide, whatever the others were. */
  @Test
  void aNormalisedFieldOfMixedTypesIsAString() throws Exception {
    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaInteger("integer"));
    row.addValueMeta(new ValueMetaNumber("number"));
    row.addValueMeta(new ValueMetaString("kept"));

    meta(field("integer", "integer", "value"), field("number", "number", "value"))
        .getFields(row, "normaliser", null, null, new Variables(), null);

    assertArrayEquals(new String[] {"kept", "typefield", "value"}, row.getFieldNames());
    assertEquals(IValueMeta.TYPE_STRING, row.getValueMeta(2).getType());
  }

  @Test
  void aNormalisedFieldWhoseFirstFieldIsMissingCannotBeDescribed() {
    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaInteger("i2"));

    HopTransformException e =
        assertThrows(
            HopTransformException.class,
            () ->
                meta(field("i1", "A", "value"), field("i2", "B", "value"))
                    .getFields(row, "normaliser", null, null, new Variables(), null));
    assertTrue(e.getMessage().contains("i1"), e.getMessage());
  }

  /** A later field that is missing is left to the transform to report when it runs. */
  @Test
  void aMissingLaterFieldDoesNotDecideTheType() throws Exception {
    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaInteger("i1"));

    meta(field("i1", "A", "value"), field("missing", "B", "value"))
        .getFields(row, "normaliser", null, null, new Variables(), null);

    assertEquals(IValueMeta.TYPE_INTEGER, row.getValueMeta(row.indexOfValue("value")).getType());
  }

  @Test
  void checkReportsMissingFieldsAndStillWarnsAboutTheOthers() {
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaInteger("integer"));
    prev.addValueMeta(new ValueMetaString("text"));

    List<ICheckResult> remarks =
        check(
            meta(
                field("integer", "integer", "value"),
                field("text", "text", "value"),
                field("missing", "missing", "value")),
            prev);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText().contains("missing")),
        remarks.toString());
    assertTrue(
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_WARNING),
        remarks.toString());
  }

  @Test
  void checkWarnsAboutAFieldOfMixedTypes() {
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaInteger("integer"));
    prev.addValueMeta(new ValueMetaString("text"));

    List<ICheckResult> remarks =
        check(meta(field("integer", "integer", "value"), field("text", "text", "value")), prev);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_WARNING
                        && r.getText().contains("value")),
        remarks.toString());
  }

  @Test
  void checkReportsTwoFieldsFillingOneNormalisedFieldOfOneType() {
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("a1"));
    prev.addValueMeta(new ValueMetaString("a2"));

    List<ICheckResult> remarks = check(meta(field("a1", "A", "X"), field("a2", "A", "X")), prev);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText().contains("a1")
                        && r.getText().contains("a2")),
        remarks.toString());
  }

  @Test
  void checkIsQuietAboutFieldsOfOneType() {
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaInteger("i1"));
    prev.addValueMeta(new ValueMetaInteger("i2"));

    List<ICheckResult> remarks = check(meta(field("i1", "A", "X"), field("i2", "B", "X")), prev);

    assertTrue(
        remarks.stream().allMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_OK),
        remarks.toString());
  }

  /** Issue #9397: no listed fields means every input field, into the value field. */
  @Test
  void withoutListedFieldsEveryInputFieldIsNormalisedIntoTheValueField() throws Exception {
    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaInteger("i"));
    row.addValueMeta(new ValueMetaString("a much longer name"));

    NormaliserMeta meta = meta();
    meta.setValueField("content");
    meta.getFields(row, "normaliser", null, null, new Variables(), null);

    assertArrayEquals(new String[] {"typefield", "content"}, row.getFieldNames());
    assertEquals("a much longer name".length(), row.getValueMeta(0).getLength());
    assertEquals(IValueMeta.TYPE_STRING, row.getValueMeta(1).getType());
  }

  /** A transform saved before the value field existed has none: it is called value then. */
  @Test
  void aMissingValueFieldIsCalledValue() throws Exception {
    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaInteger("i"));

    NormaliserMeta meta = meta();
    meta.setValueField(null);
    meta.getFields(row, "normaliser", null, null, new Variables(), null);

    assertArrayEquals(new String[] {"typefield", "value"}, row.getFieldNames());
    assertEquals(IValueMeta.TYPE_INTEGER, row.getValueMeta(1).getType());
  }

  @Test
  void checkSaysEveryInputFieldIsNormalisedWhenNoneAreListed() {
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaInteger("i1"));
    prev.addValueMeta(new ValueMetaInteger("i2"));

    List<ICheckResult> remarks = check(meta(), prev);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_OK
                        && r.getText().contains("2")
                        && r.getText().contains("value")),
        remarks.toString());
    assertTrue(
        remarks.stream().allMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_OK),
        remarks.toString());
  }

  /**
   * Input fields that are not known yet, while designing a pipeline that gets them at run time,
   * still leave the value field for the transforms after this one.
   */
  @Test
  void withoutListedFieldsTheValueFieldIsDeclaredWhileTheInputIsUnknown() throws Exception {
    IRowMeta row = new RowMeta();

    meta().getFields(row, "normaliser", null, null, new Variables(), null);

    assertArrayEquals(new String[] {"typefield", "value"}, row.getFieldNames());
    assertEquals(IValueMeta.TYPE_STRING, row.getValueMeta(1).getType());
  }

  /**
   * With every input field normalised, which one comes first is up to the input: its length would
   * be too short for the values of the others. The value field keeps only what they all share.
   */
  @Test
  void withoutListedFieldsTheValueFieldKeepsOnlyALengthAndPrecisionAllFieldsShare()
      throws Exception {
    IRowMeta texts = new RowMeta();
    texts.addValueMeta(new ValueMetaString("code", 10, -1));
    texts.addValueMeta(new ValueMetaString("description", 500, -1));
    meta().getFields(texts, "normaliser", null, null, new Variables(), null);
    assertEquals(-1, texts.searchValueMeta("value").getLength());

    IRowMeta amounts = new RowMeta();
    amounts.addValueMeta(new ValueMetaNumber("net", 9, 2));
    amounts.addValueMeta(new ValueMetaNumber("rate", 9, 4));
    meta().getFields(amounts, "normaliser", null, null, new Variables(), null);
    IValueMeta amount = amounts.searchValueMeta("value");
    assertEquals(9, amount.getLength());
    assertEquals(-1, amount.getPrecision());

    IRowMeta same = new RowMeta();
    same.addValueMeta(new ValueMetaNumber("q1", 9, 2));
    same.addValueMeta(new ValueMetaNumber("q2", 9, 2));
    meta().getFields(same, "normaliser", null, null, new Variables(), null);
    IValueMeta quarter = same.searchValueMeta("value");
    assertEquals(9, quarter.getLength());
    assertEquals(2, quarter.getPrecision());
  }

  /** The type field comes first: a new field with its name is renamed, which Verify points out. */
  @Test
  void checkWarnsAboutANewFieldWithTheNameOfTheTypeField() {
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaInteger("i1"));
    prev.addValueMeta(new ValueMetaInteger("i2"));

    NormaliserMeta allFields = meta();
    allFields.setTypeField("Value");
    assertTrue(
        check(allFields, prev).stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_WARNING
                        && r.getText().contains("value")),
        check(allFields, prev).toString());

    NormaliserMeta listed = meta(field("i1", "A", "kind"), field("i2", "B", "kind"));
    listed.setTypeField("kind");
    assertTrue(
        check(listed, prev).stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_WARNING
                        && r.getText().contains("kind")),
        check(listed, prev).toString());
  }

  /** No field list at all, from metadata injection for example, is an empty one. */
  @Test
  void aMissingFieldListIsAnEmptyOne() {
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaInteger("i1"));

    NormaliserMeta meta = meta();
    meta.setNormaliserFields(null);

    assertEquals(List.of(), meta.getNormaliserFields());
    assertTrue(meta.isNormalisingAllFields());
    assertNull(NormaliserMeta.getDuplicateMapping(null));
    List<ICheckResult> remarks = check(meta, prev);
    assertTrue(
        remarks.stream().allMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_OK),
        remarks.toString());
  }

  private static List<ICheckResult> check(NormaliserMeta meta, IRowMeta prev) {
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        prev,
        new String[] {"input"},
        new String[0],
        null,
        new Variables(),
        null);
    return remarks;
  }

  private static NormaliserField field(String name, String type, String norm) {
    NormaliserField field = new NormaliserField();
    field.setName(name);
    field.setValue(type);
    field.setNorm(norm);
    return field;
  }

  private static NormaliserMeta meta(NormaliserField... fields) {
    NormaliserMeta meta = new NormaliserMeta();
    meta.setDefault();
    meta.setNormaliserFields(new ArrayList<>(List.of(fields)));
    return meta;
  }
}
