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
package org.apache.hop.pipeline.transforms.standardizephonenumber;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit test for {@link StandardizePhoneNumberMeta} */
class StandardizePhoneNumberMetaTest {

  @BeforeEach
  void setUp() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    StandardizePhoneNumberMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/standardize-phone-number-transform.xml", StandardizePhoneNumberMeta.class);

    assertEquals(1, meta.getFields().size());
    StandardizePhoneField field = meta.getFields().get(0);
    assertEquals("PHONE", field.getInputField());
    assertEquals("PHONE_CLEANED", field.getOutputField());
    assertEquals("E164", field.getNumberFormat());
    assertEquals("COUNTRY", field.getCountryField());
    assertEquals("BE", field.getDefaultCountry());
    assertEquals("TYPE", field.getNumberTypeField());
    assertEquals("VALID", field.getIsValidNumberField());
  }

  @Test
  void setDefaultLeavesFieldsEmpty() {
    StandardizePhoneNumberMeta meta = new StandardizePhoneNumberMeta();
    meta.setDefault();
    assertTrue(meta.getFields().isEmpty());
  }

  @Test
  void getAndSetFieldsRoundTrip() {
    StandardizePhoneNumberMeta meta = new StandardizePhoneNumberMeta();
    StandardizePhoneField field = new StandardizePhoneField();
    field.setInputField("PHONE");
    List<StandardizePhoneField> fields = List.of(field);
    meta.setFields(fields);
    assertEquals(fields, meta.getFields());
  }

  @Test
  void checkDoesNotRequireCountryField() {
    StandardizePhoneNumberMeta meta = metaWith("PHONE", "PHONE_E164", null);

    List<ICheckResult> remarks = check(meta, phonePrev(), new String[] {"prev"});

    assertFalse(
        remarks.stream()
            .anyMatch(
                remark ->
                    remark.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && remark.getText() != null
                        && remark.getText().contains("country")),
        "Optional country field should not produce a check error");
  }

  @Test
  void checkReportsMissingCountryFieldWhenConfigured() {
    StandardizePhoneNumberMeta meta = metaWith("PHONE", "PHONE_E164", "COUNTRY");

    List<ICheckResult> remarks = check(meta, phonePrev(), new String[] {"prev"});

    assertTrue(
        remarks.stream()
            .anyMatch(
                remark ->
                    remark.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && remark.getText() != null
                        && remark.getText().contains("COUNTRY")));
  }

  @Test
  void checkReportsMissingInputField() {
    StandardizePhoneNumberMeta meta = metaWith("MISSING", "OUT", null);

    List<ICheckResult> remarks = check(meta, phonePrev(), new String[] {"prev"});

    assertTrue(
        remarks.stream()
            .anyMatch(
                remark ->
                    remark.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && remark.getText() != null
                        && remark.getText().contains("MISSING")));
  }

  @Test
  void checkWarnsWhenPreviousFieldsAreMissing() {
    StandardizePhoneNumberMeta meta = new StandardizePhoneNumberMeta();

    List<ICheckResult> remarks = check(meta, new RowMeta(), new String[] {"prev"});

    assertTrue(
        remarks.stream().anyMatch(remark -> remark.getType() == ICheckResult.TYPE_RESULT_WARNING));
  }

  @Test
  void checkErrorsWhenNoInputStreams() {
    StandardizePhoneNumberMeta meta = metaWith("PHONE", "OUT", null);

    List<ICheckResult> remarks = check(meta, phonePrev(), new String[] {});

    String expected =
        BaseMessages.getString(
            StandardizePhoneNumberMeta.class,
            "StandardizePhoneNumberMeta.CheckResult.NotReceivingInfoFromOtherTransforms");
    assertTrue(
        remarks.stream()
            .anyMatch(
                remark ->
                    remark.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && expected.equals(remark.getText())));
  }

  @Test
  void checkOkWhenReceivingFieldsAndInput() {
    StandardizePhoneNumberMeta meta = metaWith("PHONE", "OUT", "COUNTRY");
    RowMeta prev = phonePrev();
    prev.addValueMeta(new ValueMetaString("COUNTRY"));

    List<ICheckResult> remarks = check(meta, prev, new String[] {"prev"});

    assertTrue(
        remarks.stream().anyMatch(remark -> remark.getType() == ICheckResult.TYPE_RESULT_OK));
    assertFalse(
        remarks.stream().anyMatch(remark -> remark.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  void checkResolvesVariableFieldNames() {
    StandardizePhoneNumberMeta meta = metaWith("${IN}", "OUT", "${COUNTRY}");
    RowMeta prev = phonePrev();
    prev.addValueMeta(new ValueMetaString("COUNTRY"));

    Variables variables = new Variables();
    variables.setVariable("IN", "PHONE");
    variables.setVariable("COUNTRY", "COUNTRY");

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        new PipelineMeta(),
        new TransformMeta("standardize", meta),
        prev,
        new String[] {"prev"},
        new String[] {},
        null,
        variables,
        null);

    assertFalse(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  void getFieldsResolvesOutputFieldVariables() throws Exception {
    StandardizePhoneNumberMeta meta = new StandardizePhoneNumberMeta();
    StandardizePhoneField field = new StandardizePhoneField();
    field.setInputField("PHONE");
    field.setOutputField("${OUTPUT_FIELD}");
    field.setNumberTypeField("${TYPE_FIELD}");
    field.setIsValidNumberField("${VALID_FIELD}");
    meta.setFields(List.of(field));

    Variables variables = new Variables();
    variables.setVariable("OUTPUT_FIELD", "E164_OUT");
    variables.setVariable("TYPE_FIELD", "TYPE");
    variables.setVariable("VALID_FIELD", "VALID");

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("PHONE"));
    meta.getFields(rowMeta, "standardize", null, null, variables, null);

    assertEquals("PHONE", rowMeta.getValueMeta(0).getName());
    assertEquals("E164_OUT", rowMeta.getValueMeta(1).getName());
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.getValueMeta(1).getType());
    assertEquals("standardize", rowMeta.getValueMeta(1).getOrigin());
    assertEquals("TYPE", rowMeta.getValueMeta(2).getName());
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.getValueMeta(2).getType());
    assertEquals("VALID", rowMeta.getValueMeta(3).getName());
    assertEquals(IValueMeta.TYPE_BOOLEAN, rowMeta.getValueMeta(3).getType());
  }

  @Test
  void getFieldsDoesNotAddOutputWhenOverwritingInput() throws Exception {
    StandardizePhoneNumberMeta meta = metaWith("PHONE", "PHONE", null);

    RowMeta rowMeta = phonePrev();
    meta.getFields(rowMeta, "standardize", null, null, new Variables(), null);

    assertEquals(1, rowMeta.size());
    assertEquals("PHONE", rowMeta.getValueMeta(0).getName());
    assertEquals("standardize", rowMeta.getValueMeta(0).getOrigin());
  }

  @Test
  void getFieldsDoesNotAddEmptyOptionalFields() throws Exception {
    StandardizePhoneNumberMeta meta = new StandardizePhoneNumberMeta();
    StandardizePhoneField field = new StandardizePhoneField();
    field.setInputField("PHONE");
    field.setOutputField("E164");
    meta.setFields(List.of(field));

    RowMeta rowMeta = phonePrev();
    meta.getFields(rowMeta, "standardize", null, null, new Variables(), null);

    assertEquals(2, rowMeta.size());
    assertEquals("PHONE", rowMeta.getValueMeta(0).getName());
    assertEquals("E164", rowMeta.getValueMeta(1).getName());
  }

  @Test
  void getFieldsAddsOutputWhenInputIsMissing() throws Exception {
    StandardizePhoneNumberMeta meta = metaWith("MISSING", "E164", null);

    RowMeta rowMeta = phonePrev();
    meta.getFields(rowMeta, "standardize", null, null, new Variables(), null);

    assertEquals("E164", rowMeta.searchValueMeta("E164").getName());
    assertEquals("standardize", rowMeta.searchValueMeta("E164").getOrigin());
  }

  @Test
  void getSupportedFormatsListsLibphonenumberFormats() {
    String[] formats = new StandardizePhoneNumberMeta().getSupportedFormats();
    assertArrayEquals(
        new String[] {
          PhoneNumberFormat.E164.name(),
          PhoneNumberFormat.INTERNATIONAL.name(),
          PhoneNumberFormat.NATIONAL.name(),
          PhoneNumberFormat.RFC3966.name()
        },
        formats);
  }

  @Test
  void getSupportedCountriesIsSortedAndIncludesCommonRegions() {
    String[] countries = new StandardizePhoneNumberMeta().getSupportedCountries();
    assertTrue(countries.length > 10);
    assertTrue(Arrays.asList(countries).contains("BE"));
    assertTrue(Arrays.asList(countries).contains("US"));
    assertTrue(Arrays.asList(countries).contains("FR"));
    String[] sorted = countries.clone();
    Arrays.sort(sorted);
    assertArrayEquals(sorted, countries);
  }

  @Test
  void resolveNameHandlesNullsAndVariables() {
    assertNull(StandardizePhoneNumberMeta.resolveName(new Variables(), null));
    assertEquals("", StandardizePhoneNumberMeta.resolveName(new Variables(), ""));
    assertEquals("${X}", StandardizePhoneNumberMeta.resolveName(null, "${X}"));

    Variables variables = new Variables();
    variables.setVariable("X", "PHONE");
    assertEquals("PHONE", StandardizePhoneNumberMeta.resolveName(variables, "${X}"));
  }

  private static StandardizePhoneNumberMeta metaWith(String input, String output, String country) {
    StandardizePhoneNumberMeta meta = new StandardizePhoneNumberMeta();
    StandardizePhoneField field = new StandardizePhoneField();
    field.setInputField(input);
    field.setOutputField(output);
    field.setCountryField(country);
    field.setDefaultCountry("BE");
    meta.setFields(List.of(field));
    return meta;
  }

  private static RowMeta phonePrev() {
    RowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("PHONE"));
    return prev;
  }

  private static List<ICheckResult> check(
      StandardizePhoneNumberMeta meta, RowMeta prev, String[] input) {
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        new PipelineMeta(),
        new TransformMeta("standardize", meta),
        prev,
        input,
        new String[] {},
        null,
        new Variables(),
        null);
    return remarks;
  }
}
