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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat;
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber;
import java.util.List;
import java.util.Locale;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit test for {@link StandardizePhoneNumber} */
class StandardizePhoneNumberTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<StandardizePhoneNumberMeta, StandardizePhoneNumberData> mockHelper;

  @BeforeEach
  void setUp() throws HopException {
    HopEnvironment.init();
    mockHelper =
        new TransformMockHelper<>(
            "StandardizePhoneNumber",
            StandardizePhoneNumberMeta.class,
            StandardizePhoneNumberData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void normalizeRegionTrimsAndUsesRootLocale() {
    assertEquals("BE", StandardizePhoneNumber.normalizeRegion("BE"));
    assertEquals("BE", StandardizePhoneNumber.normalizeRegion("be"));
    assertEquals("BE", StandardizePhoneNumber.normalizeRegion(" BE "));
    assertNull(StandardizePhoneNumber.normalizeRegion("   "));
    assertNull(StandardizePhoneNumber.normalizeRegion(null));

    Locale original = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr"));
      assertEquals("IN", StandardizePhoneNumber.normalizeRegion("in"));
    } finally {
      Locale.setDefault(original);
    }
  }

  @Test
  void processRowFormatsValidNumber() throws Exception {
    List<Object[]> rows = execute(createMeta("PHONE_CLEANED"), new Object[] {"0499500158", "BE"});

    Object[] out = rows.get(0);
    assertEquals("0499500158", out[0]);
    assertEquals("BE", out[1]);
    assertEquals("+32499500158", out[2]);
    assertEquals("MOBILE", out[3]);
    assertEquals(Boolean.TRUE, out[4]);
  }

  @Test
  void processRowKeepsOriginalOnParseFailure() throws Exception {
    List<Object[]> rows = execute(createMeta("PHONE_CLEANED"), new Object[] {"error", "BE"});

    Object[] out = rows.get(0);
    assertEquals("error", out[2]);
    assertEquals(StandardizePhoneNumber.NUMBER_TYPE_ERROR, out[3]);
    assertEquals(Boolean.FALSE, out[4]);
  }

  @Test
  void processRowSetsErrorTypeForEmptyNumber() throws Exception {
    List<Object[]> rows = execute(createMeta("PHONE_CLEANED"), new Object[] {null, "BE"});

    Object[] out = rows.get(0);
    assertNull(out[2]);
    assertEquals(StandardizePhoneNumber.NUMBER_TYPE_ERROR, out[3]);
    assertEquals(Boolean.FALSE, out[4]);
  }

  @Test
  void processRowSetsErrorTypeForEmptyStringNumber() throws Exception {
    List<Object[]> rows = execute(createMeta("PHONE_CLEANED"), new Object[] {"", "BE"});

    Object[] out = rows.get(0);
    assertNull(out[2]);
    assertEquals(StandardizePhoneNumber.NUMBER_TYPE_ERROR, out[3]);
    assertEquals(Boolean.FALSE, out[4]);
  }

  @Test
  void processRowKeepsWhitespaceOnlyNumberAsParseFailure() throws Exception {
    List<Object[]> rows = execute(createMeta("PHONE_CLEANED"), new Object[] {"   ", "BE"});

    Object[] out = rows.get(0);
    assertEquals("   ", out[2]);
    assertEquals(StandardizePhoneNumber.NUMBER_TYPE_ERROR, out[3]);
    assertEquals(Boolean.FALSE, out[4]);
  }

  @Test
  void processRowTrimsCountryCodeWithSpaces() throws Exception {
    List<Object[]> rows = execute(createMeta("PHONE_CLEANED"), new Object[] {"0499500158", "BE "});

    Object[] out = rows.get(0);
    assertEquals("+32499500158", out[2]);
    assertEquals("MOBILE", out[3]);
    assertEquals(Boolean.TRUE, out[4]);
  }

  @Test
  void processRowAcceptsLowercaseCountryCode() throws Exception {
    List<Object[]> rows = execute(createMeta("PHONE_CLEANED"), new Object[] {"0499500158", "be"});

    assertEquals("+32499500158", rows.get(0)[2]);
  }

  @Test
  void processRowResolvesOutputFieldVariable() throws Exception {
    StandardizePhoneNumberMeta meta = createMeta("${OUTPUT_FIELD}");
    StandardizePhoneNumberData data = new StandardizePhoneNumberData();
    StandardizePhoneNumber transform =
        new StandardizePhoneNumber(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    transform.init();
    transform.setVariable("OUTPUT_FIELD", "E164_OUT");

    RowMeta rowMeta = inputRowMeta();
    IRowSet rowSet = mockHelper.getMockInputRowSet(new Object[] {"0499500158", "BE"});
    when(rowSet.getRowMeta()).thenReturn(rowMeta);
    transform.addRowSetToInputRowSets(rowSet);
    transform.setInputRowMeta(rowMeta);

    List<Object[]> rows = PipelineTestingUtil.execute(transform, 1, false);
    assertEquals("+32499500158", rows.get(0)[2]);
    assertEquals("E164_OUT", data.outputRowMeta.getValueMeta(2).getName());
  }

  @Test
  void processRowResolvesTypeAndValidFieldVariables() throws Exception {
    StandardizePhoneNumberMeta meta = createMeta("PHONE_CLEANED");
    meta.getFields().get(0).setNumberTypeField("${TYPE_FIELD}");
    meta.getFields().get(0).setIsValidNumberField("${VALID_FIELD}");

    StandardizePhoneNumberData data = new StandardizePhoneNumberData();
    StandardizePhoneNumber transform =
        new StandardizePhoneNumber(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    transform.init();
    transform.setVariable("TYPE_FIELD", "TYPE");
    transform.setVariable("VALID_FIELD", "VALID");

    RowMeta rowMeta = inputRowMeta();
    IRowSet rowSet = mockHelper.getMockInputRowSet(new Object[] {"0499500158", "BE"});
    when(rowSet.getRowMeta()).thenReturn(rowMeta);
    transform.addRowSetToInputRowSets(rowSet);
    transform.setInputRowMeta(rowMeta);

    List<Object[]> rows = PipelineTestingUtil.execute(transform, 1, false);
    assertEquals("MOBILE", rows.get(0)[3]);
    assertEquals(Boolean.TRUE, rows.get(0)[4]);
    assertEquals("TYPE", data.outputRowMeta.getValueMeta(3).getName());
    assertEquals("VALID", data.outputRowMeta.getValueMeta(4).getName());
  }

  @Test
  void processRowUsesDefaultCountryWhenCountryFieldEmpty() throws Exception {
    StandardizePhoneNumberMeta meta = createMeta("PHONE_CLEANED");
    meta.getFields().get(0).setCountryField(null);

    List<Object[]> rows = execute(meta, new Object[] {"0499500158", "US"});
    assertEquals("+32499500158", rows.get(0)[2]);
  }

  @Test
  void processRowUsesDefaultCountryWhenCountryValueBlank() throws Exception {
    List<Object[]> rows = execute(createMeta("PHONE_CLEANED"), new Object[] {"0499500158", "  "});
    assertEquals("+32499500158", rows.get(0)[2]);
  }

  @Test
  void processRowFallsBackToDefaultForUnsupportedRegion() throws Exception {
    List<Object[]> rows = execute(createMeta("PHONE_CLEANED"), new Object[] {"0499500158", "XX"});
    assertEquals("+32499500158", rows.get(0)[2]);
  }

  @Test
  void processRowTrimsDefaultCountry() throws Exception {
    StandardizePhoneNumberMeta meta = createMeta("PHONE_CLEANED");
    meta.getFields().get(0).setCountryField(null);
    meta.getFields().get(0).setDefaultCountry(" be ");

    List<Object[]> rows = execute(meta, new Object[] {"0499500158", "US"});
    assertEquals("+32499500158", rows.get(0)[2]);
  }

  @Test
  void processRowOverwritesInputWhenOutputMatchesInput() throws Exception {
    StandardizePhoneNumberMeta meta = createMeta("PHONE");

    List<Object[]> rows = execute(meta, new Object[] {"0499500158", "BE"});
    Object[] out = rows.get(0);
    assertEquals("+32499500158", out[0]);
    assertEquals("MOBILE", out[2]);
    assertEquals(Boolean.TRUE, out[3]);
  }

  @Test
  void processRowOverwritesInputWhenOutputFieldBlank() throws Exception {
    StandardizePhoneNumberMeta meta = createMeta(null);

    List<Object[]> rows = execute(meta, new Object[] {"0499500158", "BE"});
    assertEquals("+32499500158", rows.get(0)[0]);
  }

  @Test
  void processRowAppliesAllSupportedFormats() throws Exception {
    PhoneNumberUtil util = PhoneNumberUtil.getInstance();
    PhoneNumber parsed = util.parse("0499500158", "BE");

    for (PhoneNumberFormat format :
        List.of(
            PhoneNumberFormat.E164,
            PhoneNumberFormat.INTERNATIONAL,
            PhoneNumberFormat.NATIONAL,
            PhoneNumberFormat.RFC3966)) {
      StandardizePhoneNumberMeta meta = createMeta("PHONE_CLEANED");
      meta.getFields().get(0).setNumberFormat(format.name());

      List<Object[]> rows = execute(meta, new Object[] {"0499500158", "BE"});
      assertEquals(util.format(parsed, format), rows.get(0)[2], format.name());
    }
  }

  @Test
  void processRowFallsBackToE164ForUnknownFormat() throws Exception {
    StandardizePhoneNumberMeta meta = createMeta("PHONE_CLEANED");
    meta.getFields().get(0).setNumberFormat("NOT_A_FORMAT");

    List<Object[]> rows = execute(meta, new Object[] {"0499500158", "BE"});
    assertEquals("+32499500158", rows.get(0)[2]);
  }

  @Test
  void processRowReplacesCommaBeforeParsing() throws Exception {
    List<Object[]> rows = execute(createMeta("PHONE_CLEANED"), new Object[] {"0499,500158", "BE"});
    assertEquals("+32499500158", rows.get(0)[2]);
  }

  @Test
  void processRowMarksParsedButInvalidNumber() throws Exception {
    List<Object[]> rows = execute(createMeta("PHONE_CLEANED"), new Object[] {"999", "FR"});

    Object[] out = rows.get(0);
    assertEquals("+33999", out[2]);
    assertEquals("UNKNOWN", out[3]);
    assertEquals(Boolean.FALSE, out[4]);
  }

  @Test
  void processRowSkipsOptionalTypeAndValidFields() throws Exception {
    StandardizePhoneNumberMeta meta = createMeta("PHONE_CLEANED");
    meta.getFields().get(0).setNumberTypeField(null);
    meta.getFields().get(0).setIsValidNumberField(null);

    List<Object[]> rows = execute(meta, new Object[] {"0499500158", "BE"});
    assertEquals("+32499500158", rows.get(0)[2]);
    assertEquals(3, rows.get(0).length);
  }

  @Test
  void processRowStandardizesMultipleFields() throws Exception {
    StandardizePhoneField first = field("PHONE", "E164", "COUNTRY", "BE");
    StandardizePhoneField second = field("PHONE2", "E164_2", "COUNTRY", "BE");
    second.setNumberTypeField("TYPE2");
    second.setIsValidNumberField("VALID2");

    StandardizePhoneNumberMeta meta = new StandardizePhoneNumberMeta();
    meta.setFields(List.of(first, second));

    RowMeta rowMeta = inputRowMeta();
    rowMeta.addValueMeta(new ValueMetaString("PHONE2"));

    List<Object[]> rows = execute(meta, rowMeta, new Object[] {"0499500158", "BE", "0470123456"});
    Object[] out = rows.get(0);
    assertEquals("+32499500158", out[3]);
    assertEquals("+32470123456", out[6]);
  }

  @Test
  void processRowReturnsFalseWhenInputFieldMissing() throws Exception {
    StandardizePhoneNumberMeta meta = createMeta("PHONE_CLEANED");
    meta.getFields().get(0).setInputField("MISSING");

    StandardizePhoneNumber transform =
        ready(meta, inputRowMeta(), new Object[] {"0499500158", "BE"});
    assertFalse(transform.processRow());
    assertEquals(1, transform.getErrors());
  }

  @Test
  void processRowThrowsWhenCountryFieldMissing() {
    StandardizePhoneNumberMeta meta = createMeta("PHONE_CLEANED");
    meta.getFields().get(0).setCountryField("ISO");

    StandardizePhoneNumber transform =
        ready(meta, inputRowMeta(), new Object[] {"0499500158", "BE"});
    HopException exception = assertThrows(HopException.class, transform::processRow);
    assertTrue(exception.getMessage().contains("ISO"));
    assertEquals(1, transform.getErrors());
  }

  @Test
  void processRowReturnsFalseWhenNoMoreRows() throws Exception {
    StandardizePhoneNumber transform = ready(createMeta("PHONE_CLEANED"), inputRowMeta());
    assertFalse(transform.processRow());
  }

  @Test
  void processRowHandlesMultipleInputRows() throws Exception {
    List<Object[]> rows =
        execute(
            createMeta("PHONE_CLEANED"),
            new Object[] {"0499500158", "BE"},
            new Object[] {"error", "BE"});

    assertEquals(2, rows.size());
    assertEquals("+32499500158", rows.get(0)[2]);
    assertEquals("error", rows.get(1)[2]);
  }

  @Test
  void getPhoneNumberFormatParsesKnownValuesAndFallsBack() {
    StandardizePhoneNumber transform = createTransform(createMeta("PHONE_CLEANED"));
    assertEquals(PhoneNumberFormat.NATIONAL, transform.getPhoneNumberFormat("NATIONAL"));
    assertEquals(PhoneNumberFormat.E164, transform.getPhoneNumberFormat("bogus"));
    assertEquals(PhoneNumberFormat.E164, transform.getPhoneNumberFormat(null));
  }

  @Test
  void initReturnsTrue() {
    StandardizePhoneNumberData data = new StandardizePhoneNumberData();
    StandardizePhoneNumber transform =
        new StandardizePhoneNumber(
            mockHelper.transformMeta,
            createMeta("PHONE_CLEANED"),
            data,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    assertTrue(transform.init());
  }

  private static StandardizePhoneNumberMeta createMeta(String outputField) {
    StandardizePhoneNumberMeta meta = new StandardizePhoneNumberMeta();
    meta.setFields(List.of(field("PHONE", outputField, "COUNTRY", "BE")));
    return meta;
  }

  private static StandardizePhoneField field(
      String input, String output, String country, String defaultCountry) {
    StandardizePhoneField result = new StandardizePhoneField();
    result.setInputField(input);
    result.setOutputField(output);
    result.setCountryField(country);
    result.setDefaultCountry(defaultCountry);
    result.setNumberFormat("E164");
    result.setNumberTypeField("TYPE");
    result.setIsValidNumberField("VALID");
    return result;
  }

  private static RowMeta inputRowMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("PHONE"));
    rowMeta.addValueMeta(new ValueMetaString("COUNTRY"));
    return rowMeta;
  }

  private StandardizePhoneNumber createTransform(StandardizePhoneNumberMeta meta) {
    StandardizePhoneNumberData data = new StandardizePhoneNumberData();
    StandardizePhoneNumber transform =
        new StandardizePhoneNumber(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    transform.init();
    return transform;
  }

  private StandardizePhoneNumber ready(
      StandardizePhoneNumberMeta meta, RowMeta rowMeta, Object[]... inputRows) {
    StandardizePhoneNumber transform = createTransform(meta);
    IRowSet rowSet = mockHelper.getMockInputRowSet(inputRows);
    when(rowSet.getRowMeta()).thenReturn(rowMeta);
    transform.addRowSetToInputRowSets(rowSet);
    transform.setInputRowMeta(rowMeta);
    return transform;
  }

  private List<Object[]> execute(StandardizePhoneNumberMeta meta, Object[]... inputRows)
      throws Exception {
    return execute(meta, inputRowMeta(), inputRows);
  }

  private List<Object[]> execute(
      StandardizePhoneNumberMeta meta, RowMeta rowMeta, Object[]... inputRows) throws Exception {
    return PipelineTestingUtil.execute(ready(meta, rowMeta, inputRows), inputRows.length, false);
  }
}
