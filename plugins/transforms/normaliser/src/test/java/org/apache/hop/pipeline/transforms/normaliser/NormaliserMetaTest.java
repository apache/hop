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

    // assertEquals(2, meta.getFields().size());
    // assertEquals("fieldName", meta.getFields().get(0).getName());
    // assertEquals("two", meta.getFields().get(0).getValue());
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
