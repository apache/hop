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

package org.apache.hop.pipeline.transforms.constant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class ConstantTest {

  private TransformMockHelper<ConstantMeta, ConstantData> mockHelper;
  private RowMetaAndData rowMetaAndData = mock(RowMetaAndData.class);
  private Constant constantSpy;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopPluginException {
    ValueMetaPluginType.getInstance().searchPlugins();
  }

  @BeforeEach
  void setUp() {

    mockHelper = new TransformMockHelper<>("Add Constants", ConstantMeta.class, ConstantData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);

    doReturn(rowMetaAndData).when(mockHelper.iTransformData).getConstants();
    constantSpy =
        Mockito.spy(
            new Constant(
                mockHelper.transformMeta,
                mockHelper.iTransformMeta,
                mockHelper.iTransformData,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void testProcessRowSuccess() throws Exception {

    doReturn(new Object[1]).when(constantSpy).getRow();
    doReturn(new RowMeta()).when(constantSpy).getInputRowMeta();
    doReturn(new Object[1]).when(rowMetaAndData).getData();

    boolean success = constantSpy.processRow();
    assertTrue(success);
  }

  @Test
  void testProcessRowFail() throws Exception {

    doReturn(null).when(constantSpy).getRow();
    doReturn(null).when(constantSpy).getInputRowMeta();

    boolean success = constantSpy.processRow();
    assertFalse(success);
  }

  /**
   * The dialog offers every registered value type, so buildRow() has to cope with types it has no
   * dedicated case for. Types the value meta plugin can build from a string have to work.
   */
  @Test
  void testBuildRowSupportsJsonType() {
    List<ICheckResult> remarks = new ArrayList<>();

    RowMetaAndData row = buildRow(remarks, new ConstantField("json", "JSON", "{\"a\":1}"));

    assertTrue(remarks.isEmpty(), textOf(remarks));
    assertEquals(IValueMeta.TYPE_JSON, row.getRowMeta().getValueMeta(0).getType());
    JsonNode json = assertInstanceOf(JsonNode.class, row.getData()[0]);
    assertEquals(1, json.get("a").asInt());
  }

  /** A value that isn't valid for the selected type is reported against that type. */
  @Test
  void testBuildRowReportsUnparsableValueForJsonType() {
    List<ICheckResult> remarks = new ArrayList<>();

    buildRow(remarks, new ConstantField("json", "JSON", "this is not json"));

    assertEquals(1, remarks.size());
    assertTrue(textOf(remarks).contains("JSON"), textOf(remarks));
  }

  /**
   * Types that can't be built from text at all have to say so, instead of claiming that no type was
   * selected (issue #2239).
   */
  @Test
  void testBuildRowReportsUnsupportedTypeByName() {
    List<ICheckResult> remarks = new ArrayList<>();

    buildRow(remarks, new ConstantField("avro", "Avro Record", "some value"));

    assertEquals(1, remarks.size());
    assertTrue(textOf(remarks).contains("Avro Record"), textOf(remarks));
    assertFalse(textOf(remarks).contains("specify the value type"), textOf(remarks));
  }

  /**
   * The dialog's Type column is populated from {@code ValueMetaFactory.getValueMetaNames()}, so
   * every name it offers has to reach buildRow() as a real type selection. Answering "please
   * specify the value type" for a type the user did pick is the bug behind issue #2239, and it
   * comes back the moment a new value meta plugin is registered - hence driving this off the
   * factory rather than a hardcoded list.
   */
  @Test
  void testNoTypeOfferedByTheDialogIsTreatedAsUnset() {
    for (String typeName : ValueMetaFactory.getValueMetaNames()) {
      List<ICheckResult> remarks = new ArrayList<>();

      buildRow(remarks, new ConstantField("field", typeName, "1"));

      assertFalse(
          textOf(remarks).contains("specify the value type"),
          "the dialog offers type '"
              + typeName
              + "' but buildRow() reports it as if no type was selected");
    }
  }

  /** An actually missing type still asks the user to pick one. */
  @Test
  void testBuildRowWithoutTypeAsksForType() {
    List<ICheckResult> remarks = new ArrayList<>();

    RowMetaAndData row = buildRow(remarks, new ConstantField("notyped", "", "some value"));

    assertEquals(1, remarks.size());
    assertTrue(textOf(remarks).contains("specify the value type"), textOf(remarks));
    assertNull(row.getData()[0]);
  }

  /** The same, for a field that has neither a type nor a value. */
  @Test
  void testBuildRowWithoutTypeOrValueAsksForType() {
    List<ICheckResult> remarks = new ArrayList<>();

    buildRow(remarks, new ConstantField("notyped", "", ""));

    assertEquals(1, remarks.size());
    assertTrue(textOf(remarks).contains("specify the value type"), textOf(remarks));
  }

  /** The types that have a dedicated case in buildRow() keep working. */
  @Test
  void testBuildRowSupportsBuiltInTypes() {
    List<ICheckResult> remarks = new ArrayList<>();

    RowMetaAndData row =
        buildRow(
            remarks,
            new ConstantField("string", "String", "a value"),
            new ConstantField("integer", "Integer", "42"),
            new ConstantField("boolean", "Boolean", "Y"),
            new ConstantField("empty", "String", ""));

    assertTrue(remarks.isEmpty(), textOf(remarks));
    assertEquals("a value", row.getData()[0]);
    assertEquals(42L, row.getData()[1]);
    assertEquals(Boolean.TRUE, row.getData()[2]);
    assertNull(row.getData()[3]);
  }

  @Test
  void testBuildRowParsesNumberWithoutFormat() {
    List<ICheckResult> remarks = new ArrayList<>();

    // No format/decimal/group/currency set, so buildRow parses with the plain NumberFormat.
    // "42" is locale independent, unlike anything with a decimal or grouping separator.
    RowMetaAndData row = buildRow(remarks, new ConstantField("number", "Number", "42"));

    assertTrue(remarks.isEmpty(), textOf(remarks));
    assertEquals(42.0d, row.getData()[0]);
  }

  @Test
  void testBuildRowParsesNumberWithFormatDecimalGroupAndCurrency() {
    ConstantField number = new ConstantField("number", "Number", "1.234,56");
    number.setFieldFormat("#,##0.00");
    number.setDecimal(",");
    number.setGroup(".");
    number.setCurrency("EUR");
    List<ICheckResult> remarks = new ArrayList<>();

    RowMetaAndData row = buildRow(remarks, number);

    assertTrue(remarks.isEmpty(), textOf(remarks));
    assertEquals(1234.56d, row.getData()[0]);
  }

  @Test
  void testBuildRowReportsUnparsableNumber() {
    List<ICheckResult> remarks = new ArrayList<>();

    buildRow(remarks, new ConstantField("number", "Number", "not a number"));

    assertEquals(1, remarks.size());
    assertTrue(textOf(remarks).contains("Number"), textOf(remarks));
  }

  @Test
  void testBuildRowParsesDateWithFormat() throws Exception {
    ConstantField date = new ConstantField("date", "Date", "2026/08/06");
    date.setFieldFormat("yyyy/MM/dd");
    List<ICheckResult> remarks = new ArrayList<>();

    RowMetaAndData row = buildRow(remarks, date);

    assertTrue(remarks.isEmpty(), textOf(remarks));
    Date parsed = assertInstanceOf(Date.class, row.getData()[0]);
    // Compare on the formatted value: the parsed Date is midnight in the default time zone, so
    // asserting an absolute epoch would make the test depend on where it runs.
    assertEquals("2026/08/06", new SimpleDateFormat("yyyy/MM/dd").format(parsed));
  }

  @Test
  void testBuildRowReportsUnparsableDate() {
    ConstantField date = new ConstantField("date", "Date", "not a date");
    date.setFieldFormat("yyyy/MM/dd");
    List<ICheckResult> remarks = new ArrayList<>();

    buildRow(remarks, date);

    assertEquals(1, remarks.size());
    assertTrue(textOf(remarks).contains("Date"), textOf(remarks));
  }

  @Test
  void testBuildRowReportsUnparsableInteger() {
    List<ICheckResult> remarks = new ArrayList<>();

    buildRow(remarks, new ConstantField("integer", "Integer", "4.2"));

    assertEquals(1, remarks.size());
    assertTrue(textOf(remarks).contains("Integer"), textOf(remarks));
  }

  @Test
  void testBuildRowParsesBigNumber() {
    List<ICheckResult> remarks = new ArrayList<>();

    RowMetaAndData row =
        buildRow(remarks, new ConstantField("big", "BigNumber", "123456789.123456789"));

    assertTrue(remarks.isEmpty(), textOf(remarks));
    assertEquals(new BigDecimal("123456789.123456789"), row.getData()[0]);
  }

  @Test
  void testBuildRowReportsUnparsableBigNumber() {
    List<ICheckResult> remarks = new ArrayList<>();

    buildRow(remarks, new ConstantField("big", "BigNumber", "not a number"));

    assertEquals(1, remarks.size());
    assertTrue(textOf(remarks).contains("BigNumber"), textOf(remarks));
  }

  @Test
  void testBuildRowParsesBooleanValues() {
    List<ICheckResult> remarks = new ArrayList<>();

    RowMetaAndData row =
        buildRow(
            remarks,
            new ConstantField("yes", "Boolean", "Y"),
            new ConstantField("true", "Boolean", "true"),
            new ConstantField("no", "Boolean", "N"),
            new ConstantField("other", "Boolean", "whatever"));

    assertTrue(remarks.isEmpty(), textOf(remarks));
    assertEquals(Boolean.TRUE, row.getData()[0]);
    assertEquals(Boolean.TRUE, row.getData()[1], "TRUE is accepted next to Y");
    assertEquals(Boolean.FALSE, row.getData()[2]);
    assertEquals(Boolean.FALSE, row.getData()[3], "anything that isn't Y/TRUE is false");
  }

  @Test
  void testBuildRowParsesBinary() {
    List<ICheckResult> remarks = new ArrayList<>();

    RowMetaAndData row = buildRow(remarks, new ConstantField("binary", "Binary", "hop"));

    assertTrue(remarks.isEmpty(), textOf(remarks));
    assertArrayEquals("hop".getBytes(), (byte[]) row.getData()[0]);
  }

  @Test
  void testBuildRowParsesTimestamp() {
    List<ICheckResult> remarks = new ArrayList<>();

    RowMetaAndData row =
        buildRow(remarks, new ConstantField("ts", "Timestamp", "2026-08-06 10:11:12.0"));

    assertTrue(remarks.isEmpty(), textOf(remarks));
    assertEquals(Timestamp.valueOf("2026-08-06 10:11:12.0"), row.getData()[0]);
  }

  @Test
  void testBuildRowReportsUnparsableTimestamp() {
    List<ICheckResult> remarks = new ArrayList<>();

    buildRow(remarks, new ConstantField("ts", "Timestamp", "not a timestamp"));

    assertEquals(1, remarks.size());
    assertTrue(textOf(remarks).contains("Timestamp"), textOf(remarks));
  }

  @Test
  void testBuildRowParsesInternetAddress() throws Exception {
    List<ICheckResult> remarks = new ArrayList<>();

    // A literal address, so resolving it never touches DNS and the test stays hermetic.
    RowMetaAndData row =
        buildRow(remarks, new ConstantField("ip", "Internet Address", "127.0.0.1"));

    assertTrue(remarks.isEmpty(), textOf(remarks));
    assertEquals(InetAddress.getByName("127.0.0.1"), row.getData()[0]);
  }

  /** "Set empty string?" wins over the value, for every type. */
  @Test
  void testBuildRowSetEmptyStringOverridesValue() {
    List<ICheckResult> remarks = new ArrayList<>();

    RowMetaAndData row =
        buildRow(
            remarks,
            new ConstantField("empty", "String", true),
            new ConstantField("ignored", "Integer", true));

    assertTrue(remarks.isEmpty(), textOf(remarks));
    assertEquals("", row.getData()[0]);
    assertEquals("", row.getData()[1], "the empty-string flag is applied before the type switch");
  }

  /** Fields carry their length and precision into the generated row meta. */
  @Test
  void testBuildRowAppliesLengthAndPrecision() {
    ConstantField field = new ConstantField("number", "Number", "42");
    field.setFieldLength(12);
    field.setFieldPrecision(3);
    List<ICheckResult> remarks = new ArrayList<>();

    RowMetaAndData row = buildRow(remarks, field);

    assertEquals(12, row.getRowMeta().getValueMeta(0).getLength());
    assertEquals(3, row.getRowMeta().getValueMeta(0).getPrecision());
  }

  /** A field without a name contributes nothing to the generated row meta, but is reported. */
  @Test
  void testBuildRowSkipsFieldWithoutName() {
    List<ICheckResult> remarks = new ArrayList<>();

    RowMetaAndData row =
        buildRow(remarks, new ConstantField(null, "String", "orphan value"), namedField());

    assertEquals(1, row.getRowMeta().size(), "only the named field makes it into the row meta");
    assertEquals("named", row.getRowMeta().getValueMeta(0).getName());
    assertEquals(1, row.getData().length, "the skipped field must not leave a hole in the data");
    assertEquals("value", row.getData()[0]);

    assertEquals(1, remarks.size(), "the dropped field has to be reported");
    assertEquals(ICheckResult.TYPE_RESULT_WARNING, remarks.get(0).getType());
    assertTrue(textOf(remarks).contains("Constant 1"), textOf(remarks));
  }

  /**
   * A field that was filled in but never named can't become a column, and quietly dropping it hides
   * what is nearly always a forgotten name. It is reported as a warning rather than an error so an
   * existing pipeline carrying one still runs.
   */
  @Test
  void testBuildRowWarnsAboutAFilledInFieldWithoutAName() {
    List<ICheckResult> remarks = new ArrayList<>();

    buildRow(remarks, namedField(), new ConstantField("", "String", "forgot the name"));

    assertEquals(1, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_WARNING, remarks.get(0).getType());
    assertTrue(
        textOf(remarks).contains("Constant 2"), "the warning names the row: " + textOf(remarks));
  }

  /** A blank leftover row carries nothing, so there is no mistake to report. */
  @Test
  void testBuildRowIsSilentAboutAnEntirelyEmptyField() {
    List<ICheckResult> remarks = new ArrayList<>();

    RowMetaAndData row = buildRow(remarks, new ConstantField("", "", ""), namedField());

    assertTrue(remarks.isEmpty(), textOf(remarks));
    assertEquals(1, row.getRowMeta().size());
  }

  /** The warning is logged, but an unnamed field does not stop the transform from starting. */
  @Test
  void testInitSucceedsButReportsAFieldWithoutAName() {
    ConstantMeta meta = new ConstantMeta();
    meta.getFields().add(new ConstantField("", "String", "forgot the name"));
    meta.getFields().add(new ConstantField("kept", "String", "kept value"));
    ConstantData data = new ConstantData();

    assertTrue(newConstant(meta, data).init(), "a warning must not stop the transform");

    assertEquals(1, data.getConstants().getRowMeta().size());
    assertEquals("kept value", data.getConstants().getData()[0]);
  }

  /**
   * A blank (but non-null) name has to be skipped exactly like a null one. The transform's output
   * row meta comes from ConstantMeta.getFields(), which drops null <em>and</em> empty names, so a
   * constants row that keeps blank-named fields is one value longer than the meta describing it and
   * every later constant lands in the wrong column.
   */
  @Test
  void testBuildRowSkipsFieldWithBlankNameLikeTheOutputRowMetaDoes() throws Exception {
    ConstantMeta meta = new ConstantMeta();
    meta.getFields().add(new ConstantField("", "String", "blank name"));
    meta.getFields().add(new ConstantField("kept", "String", "kept value"));

    RowMetaAndData constants = Constant.buildRow(meta, new ConstantData(), new ArrayList<>());
    IRowMeta outputRowMeta = new RowMeta();
    meta.getFields(outputRowMeta, "constant", null, null, new Variables(), null);

    assertEquals(outputRowMeta.size(), constants.getRowMeta().size());
    assertEquals(
        outputRowMeta.size(),
        constants.getData().length,
        "the constants row must be exactly as wide as the row meta describing it");
    assertEquals("kept value", constants.getData()[0]);
  }

  /** The end-to-end symptom of the above: a constant landing in a different field's column. */
  @Test
  void testProcessRowPutsEachConstantInItsOwnColumn() throws Exception {
    ConstantMeta meta = new ConstantMeta();
    meta.getFields().add(new ConstantField("", "String", "blank name"));
    meta.getFields().add(new ConstantField("kept", "String", "kept value"));
    ConstantData data = new ConstantData();

    Constant transform = Mockito.spy(newConstant(meta, data));
    assertTrue(transform.init());

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("in"));
    doReturn(new Object[] {"input"}).when(transform).getRow();
    doReturn(inputRowMeta).when(transform).getInputRowMeta();

    ArgumentCaptor<IRowMeta> rowMetaCaptor = ArgumentCaptor.forClass(IRowMeta.class);
    ArgumentCaptor<Object[]> rowCaptor = ArgumentCaptor.forClass(Object[].class);
    doNothing().when(transform).putRow(rowMetaCaptor.capture(), rowCaptor.capture());

    assertTrue(transform.processRow());

    IRowMeta outputRowMeta = rowMetaCaptor.getValue();
    Object[] outputRow = rowCaptor.getValue();
    assertEquals("input", outputRowMeta.getString(outputRow, "in", null));
    assertEquals("kept value", outputRowMeta.getString(outputRow, "kept", null));
  }

  @Test
  void testInitBuildsTheConstantsRow() {
    ConstantMeta meta = new ConstantMeta();
    meta.getFields().add(new ConstantField("string", "String", "a value"));
    ConstantData data = new ConstantData();

    assertTrue(newConstant(meta, data).init());

    assertEquals("a value", data.getConstants().getData()[0]);
  }

  /** A field that can't be built fails init() rather than starting with a broken row. */
  @Test
  void testInitFailsWhenAFieldCannotBeBuilt() {
    ConstantMeta meta = new ConstantMeta();
    meta.getFields().add(new ConstantField("integer", "Integer", "not a number"));

    assertFalse(newConstant(meta, new ConstantData()).init());
  }

  @Test
  void testProcessRowAppendsConstantsToTheInputRow() throws Exception {
    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("in"));

    mockHelper.iTransformData.firstRow = true;
    doReturn(new Object[] {"input"}).when(constantSpy).getRow();
    doReturn(inputRowMeta).when(constantSpy).getInputRowMeta();
    doReturn(new Object[] {"constant"}).when(rowMetaAndData).getData();
    doReturn(true).when(constantSpy).isRowLevel();

    assertTrue(constantSpy.processRow());

    // firstRow is cleared after the output meta has been derived from the input row meta.
    assertFalse(mockHelper.iTransformData.firstRow);
    assertNotNull(mockHelper.iTransformData.outputMeta);
  }

  private static ConstantField namedField() {
    return new ConstantField("named", "String", "value");
  }

  private Constant newConstant(ConstantMeta meta, ConstantData data) {
    return new Constant(
        mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
  }

  private static RowMetaAndData buildRow(List<ICheckResult> remarks, ConstantField... fields) {
    ConstantMeta meta = new ConstantMeta();
    meta.getFields().addAll(Arrays.asList(fields));
    return Constant.buildRow(meta, new ConstantData(), remarks);
  }

  private static String textOf(List<ICheckResult> remarks) {
    return remarks.stream().map(ICheckResult::getText).reduce("", (a, b) -> a + b + "\n");
  }
}
