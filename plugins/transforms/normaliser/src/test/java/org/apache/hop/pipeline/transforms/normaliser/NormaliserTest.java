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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.stubbing.Stubber;

class NormaliserTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<NormaliserMeta, NormaliserData> helper;

  @BeforeAll
  static void before() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>("Row normaliser", NormaliserMeta.class, NormaliserData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  /**
   * The example the transform was written for:
   *
   * <pre>
   * DATE      PR1_NR  PR_SL  PR2_NR  PR2_SL  PR3_NR  PR3_SL
   * 20030101  5       100    10      250     4       150
   * </pre>
   *
   * becomes one row per product, with its sales and number.
   */
  @Test
  void productsAreNormalisedIntoOneRowEach() throws Exception {
    Date date = new Date(103, 0, 1);
    IRowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaDate("DATE"));
    input.addValueMeta(new ValueMetaInteger("PR1_NR"));
    input.addValueMeta(new ValueMetaInteger("PR_SL"));
    input.addValueMeta(new ValueMetaInteger("PR2_NR"));
    input.addValueMeta(new ValueMetaInteger("PR2_SL"));
    input.addValueMeta(new ValueMetaInteger("PR3_NR"));
    input.addValueMeta(new ValueMetaInteger("PR3_SL"));

    NormaliserMeta meta =
        meta(
            "Type",
            field("PR_SL", "Product1", "Product Sales"),
            field("PR1_NR", "Product1", "Product Number"),
            field("PR2_SL", "Product2", "Product Sales"),
            field("PR2_NR", "Product2", "Product Number"),
            field("PR3_SL", "Product3", "Product Sales"),
            field("PR3_NR", "Product3", "Product Number"));

    Output output = normalise(meta, input, date, 5L, 100L, 10L, 250L, 4L, 150L);

    assertArrayEquals(
        new String[] {"DATE", "Type", "Product Sales", "Product Number"},
        output.rowMeta.getFieldNames());
    assertArrayEquals(new Object[] {date, "Product1", 100L, 5L}, output.rows.get(0));
    assertArrayEquals(new Object[] {date, "Product2", 250L, 10L}, output.rows.get(1));
    assertArrayEquals(new Object[] {date, "Product3", 150L, 4L}, output.rows.get(2));
  }

  /** When every field filling a normalised field has one type, the values go through untouched. */
  @Test
  void fieldsOfOneTypeKeepThatTypeAndTheirValues() throws Exception {
    IRowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("a"));
    input.addValueMeta(new ValueMetaString("b"));
    String a = "first";
    String b = "second";

    Output output =
        normalise(meta("type", field("a", "A", "value"), field("b", "B", "value")), input, a, b);

    assertEquals(IValueMeta.TYPE_STRING, output.rowMeta.getValueMeta(1).getType());
    assertSame(a, output.rows.get(0)[1]);
    assertSame(b, output.rows.get(1)[1]);
  }

  /**
   * The example from issue #3636: a String, an Integer and a Number normalised into one field. That
   * field used to be declared a String and carry a Long and a Double, which is fine until something
   * writes the row out: a sort that spills to disk, for one.
   */
  @Test
  void fieldsOfDifferentTypesFillAStringWithTheTextOfEachValue() throws Exception {
    IRowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("text"));
    input.addValueMeta(new ValueMetaInteger("integer"));
    ValueMetaNumber number = new ValueMetaNumber("number");
    number.setConversionMask("0.0");
    number.setDecimalSymbol(".");
    input.addValueMeta(number);

    NormaliserMeta meta =
        meta(
            "typefield",
            field("text", "text", "value"),
            field("integer", "integer", "value"),
            field("number", "number", "value"));

    Output output = normalise(meta, input, "a", 1L, 1.2);

    IValueMeta value = output.rowMeta.getValueMeta(1);
    assertEquals(IValueMeta.TYPE_STRING, value.getType());
    assertEquals(List.of("a", "1", "1.2"), output.rows.stream().map(row -> row[1]).toList());

    // What the sort's temporary file does with every row.
    DataOutputStream out = new DataOutputStream(new ByteArrayOutputStream());
    for (Object[] row : output.rows) {
      output.rowMeta.writeData(out, row);
    }
  }

  /** The same holds when the first field is not the String: the field used to be an Integer. */
  @Test
  void theFirstFieldNoLongerDecidesTheTypeOfTheOthers() throws Exception {
    IRowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaInteger("integer"));
    input.addValueMeta(new ValueMetaString("text"));

    Output output =
        normalise(
            meta("type", field("integer", "integer", "value"), field("text", "text", "value")),
            input,
            7L,
            "seven");

    assertEquals(IValueMeta.TYPE_STRING, output.rowMeta.getValueMeta(1).getType());
    assertEquals(List.of("7", "seven"), output.rows.stream().map(row -> row[1]).toList());
  }

  /** Nulls stay null, whether they are copied or converted. */
  @Test
  void nullsStayNull() throws Exception {
    IRowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("text"));
    input.addValueMeta(new ValueMetaInteger("integer"));

    Output output =
        normalise(
            meta("type", field("text", "text", "value"), field("integer", "integer", "value")),
            input,
            null,
            null);

    assertNull(output.rows.get(0)[1]);
    assertNull(output.rows.get(1)[1]);
  }

  /**
   * A value goes into the normalised field it names, whatever order the fields are listed in. Type
   * B's fields are listed Y before X, the other way round from type A's, and used to land in each
   * other's place.
   */
  @Test
  void valuesGoIntoTheFieldTheyNameWhateverTheOrderTheyAreListedIn() throws Exception {
    IRowMeta input = new RowMeta();
    for (String name : new String[] {"a1", "a2", "b1", "b2"}) {
      input.addValueMeta(new ValueMetaString(name));
    }

    NormaliserMeta meta =
        meta(
            "type",
            field("a1", "A", "X"),
            field("b2", "B", "Y"),
            field("b1", "B", "X"),
            field("a2", "A", "Y"));

    Output output = normalise(meta, input, "a1", "a2", "b1", "b2");

    assertArrayEquals(new String[] {"type", "X", "Y"}, output.rowMeta.getFieldNames());
    assertArrayEquals(new Object[] {"A", "a1", "a2"}, output.rows.get(0));
    assertArrayEquals(new Object[] {"B", "b1", "b2"}, output.rows.get(1));
  }

  /** A type that has no field for one of the normalised fields leaves it empty. */
  @Test
  void aTypeWithoutAFieldForANormalisedFieldLeavesItNull() throws Exception {
    IRowMeta input = new RowMeta();
    for (String name : new String[] {"a1", "a2", "b2"}) {
      input.addValueMeta(new ValueMetaString(name));
    }

    NormaliserMeta meta =
        meta("type", field("a1", "A", "X"), field("a2", "A", "Y"), field("b2", "B", "Y"));

    Output output = normalise(meta, input, "a1", "a2", "b2");

    assertArrayEquals(new Object[] {"A", "a1", "a2"}, output.rows.get(0));
    assertArrayEquals(new Object[] {"B", null, "b2"}, output.rows.get(1));
  }

  /** Two fields of one type filling the same normalised field cannot both fit on its row. */
  @Test
  void twoFieldsFillingTheSameNormalisedFieldOfOneTypeAreRefused() throws Exception {
    IRowMeta input = new RowMeta();
    for (String name : new String[] {"a1", "a2"}) {
      input.addValueMeta(new ValueMetaString(name));
    }

    NormaliserMeta meta = meta("type", field("a1", "A", "X"), field("a2", "A", "X"));

    HopException e = assertThrows(HopException.class, () -> normalise(meta, input, "a1", "a2"));
    assertTrue(e.getMessage().contains("a1"), e.getMessage());
    assertTrue(e.getMessage().contains("a2"), e.getMessage());
  }

  /** Lazy conversion hands over binary strings; a field of both storages gets normal ones. */
  @Test
  void fieldsOfOneTypeInDifferentStoragesFillAFieldOfNormalStorage() throws Exception {
    IRowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("plain"));
    ValueMetaString lazy = new ValueMetaString("lazy");
    lazy.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    ValueMetaString storage = new ValueMetaString("lazy");
    lazy.setStorageMetadata(storage);
    input.addValueMeta(lazy);

    Output output =
        normalise(
            meta("type", field("plain", "plain", "value"), field("lazy", "lazy", "value")),
            input,
            "plain value",
            "lazy value".getBytes(StandardCharsets.UTF_8));

    IValueMeta value = output.rowMeta.getValueMeta(1);
    assertEquals(IValueMeta.TYPE_STRING, value.getType());
    assertEquals(IValueMeta.STORAGE_TYPE_NORMAL, value.getStorageType());
    assertEquals(
        List.of("plain value", "lazy value"), output.rows.stream().map(row -> row[1]).toList());
  }

  /** The placements worked out on the first row serve every row after it. */
  @Test
  void everyRowIsNormalisedTheSameWay() throws Exception {
    IRowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("text"));
    input.addValueMeta(new ValueMetaInteger("integer"));

    Output output =
        normaliseRows(
            meta("type", field("text", "text", "value"), field("integer", "integer", "value")),
            input,
            new Object[] {"a", 1L},
            new Object[] {"b", 2L});

    assertEquals(List.of("a", "1", "b", "2"), output.rows.stream().map(row -> row[1]).toList());
  }

  /** A field that is not in the input stops the transform with an error. */
  @Test
  void aFieldMissingFromTheInputStopsTheTransform() throws Exception {
    IRowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("text"));

    NormaliserMeta meta =
        meta("type", field("text", "text", "value"), field("missing", "missing", "value"));
    when(helper.transformMeta.getTransform()).thenReturn(meta);
    Normaliser transform =
        new Normaliser(
            helper.transformMeta,
            meta,
            new NormaliserData(),
            0,
            helper.pipelineMeta,
            helper.pipeline);
    transform.init();
    transform.setInputRowMeta(input);
    transform.setOutputRowSets(Collections.singletonList(new BlockingRowSet(10)));
    Normaliser spied = spy(transform);
    doReturn(new Object[] {"a"}).when(spied).getRow();

    assertFalse(spied.processRow());
    assertEquals(1, spied.getErrors());
  }

  private static NormaliserField field(String name, String type, String norm) {
    NormaliserField field = new NormaliserField();
    field.setName(name);
    field.setValue(type);
    field.setNorm(norm);
    return field;
  }

  private static NormaliserMeta meta(String typeField, NormaliserField... fields) {
    NormaliserMeta meta = new NormaliserMeta();
    meta.setTypeField(typeField);
    meta.setNormaliserFields(new ArrayList<>(List.of(fields)));
    return meta;
  }

  private record Output(IRowMeta rowMeta, List<Object[]> rows) {}

  /** Runs one input row through the transform and collects what it writes. */
  private Output normalise(NormaliserMeta meta, IRowMeta input, Object... row) throws Exception {
    return normaliseRows(meta, input, row);
  }

  /** Runs input rows through the transform and collects what it writes. */
  private Output normaliseRows(NormaliserMeta meta, IRowMeta input, Object[]... rows)
      throws Exception {
    when(helper.transformMeta.getTransform()).thenReturn(meta);
    Normaliser transform =
        new Normaliser(
            helper.transformMeta,
            meta,
            new NormaliserData(),
            0,
            helper.pipelineMeta,
            helper.pipeline);
    transform.init();
    transform.setInputRowMeta(input);
    BlockingRowSet rowSet = new BlockingRowSet(100);
    transform.setOutputRowSets(Collections.singletonList(rowSet));

    Normaliser spied = spy(transform);
    Stubber stubber = doReturn(rows[0]);
    for (int i = 1; i < rows.length; i++) {
      stubber = stubber.doReturn(rows[i]);
    }
    stubber.doReturn(null).when(spied).getRow();

    for (int i = 0; i < rows.length; i++) {
      assertTrue(spied.processRow());
    }
    assertFalse(spied.processRow());

    List<Object[]> output = new ArrayList<>();
    Object[] written;
    while ((written = rowSet.getRowImmediate()) != null) {
      output.add(written);
    }
    return new Output(rowSet.getRowMeta(), output);
  }
}
