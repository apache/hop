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
package org.apache.hop.ai.transforms.structuredextract;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ExtractionParserTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void readsEachTypeIntoItsHopValue() throws Exception {
    List<StructuredExtractField> fields =
        List.of(
            field("name", "String"),
            field("count", "Integer"),
            field("amount", "Number"),
            field("precise", "BigNumber"),
            field("agreed", "Boolean"),
            field("signed", "Date"));

    Object[] values =
        ExtractionParser.parse(
            "{\"name\":\"ACME\",\"count\":3,\"amount\":12.5,\"precise\":1.5,"
                + "\"agreed\":true,\"signed\":\"2026-03-01\"}",
            fields);

    assertEquals("ACME", values[0]);
    assertEquals(3L, values[1]);
    assertEquals(12.5d, values[2]);
    assertEquals(0, new BigDecimal("1.5").compareTo((BigDecimal) values[3]));
    assertEquals(Boolean.TRUE, values[4]);
    assertEquals("2026-03-01", new SimpleDateFormat("yyyy-MM-dd").format(values[5]));
  }

  @Test
  void keepsEveryDigitOfALongDecimal() throws Exception {
    // Jackson parses decimals through a double unless told otherwise, which silently drops digits.
    // BigNumber is the type people pick when that is unacceptable.
    Object[] values =
        ExtractionParser.parse(
            "{\"precise\":0.12345678901234567890123}", List.of(field("precise", "BigNumber")));

    assertEquals(new BigDecimal("0.12345678901234567890123"), values[0]);
  }

  @Test
  void anOmittedFieldIsNullRatherThanAnError() throws Exception {
    // An optional field the model could not find is a legitimate answer.
    Object[] values =
        ExtractionParser.parse(
            "{\"name\":\"ACME\"}", List.of(field("name", "String"), field("missing", "String")));

    assertEquals("ACME", values[0]);
    assertNull(values[1]);
  }

  @Test
  void anExplicitJsonNullIsAlsoNull() throws Exception {
    Object[] values = ExtractionParser.parse("{\"name\":null}", List.of(field("name", "String")));

    assertNull(values[0]);
  }

  @Test
  void stripsTheMarkdownFenceModelsKeepAdding() throws Exception {
    Object[] values =
        ExtractionParser.parse(
            "```json\n{\"name\":\"ACME\"}\n```", List.of(field("name", "String")));

    assertEquals("ACME", values[0]);
  }

  @Test
  void namesTheFieldWhenAValueWillNotCoerce() {
    // The whole point: a bad value is reported, never written as a silent null, because downstream
    // an absent value and a misread one would look the same.
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                ExtractionParser.parse(
                    "{\"signed\":\"next March\"}", List.of(field("signed", "Date"))));

    assertTrue(e.getMessage().contains("signed"), e.getMessage());
    assertTrue(e.getMessage().contains("next March"), e.getMessage());
  }

  @Test
  void acceptsTheWordyBooleansModelsReturn() throws Exception {
    assertEquals(
        Boolean.TRUE,
        ExtractionParser.parse("{\"agreed\":\"yes\"}", List.of(field("agreed", "Boolean")))[0]);
    assertEquals(
        Boolean.FALSE,
        ExtractionParser.parse("{\"agreed\":\"No\"}", List.of(field("agreed", "Boolean")))[0]);
  }

  @Test
  void rejectsABooleanThatIsNeither() {
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                ExtractionParser.parse(
                    "{\"agreed\":\"maybe\"}", List.of(field("agreed", "Boolean"))));

    assertTrue(e.getMessage().contains("yes or no"), e.getMessage());
  }

  @Test
  void acceptsADateThatCameBackWithATime() throws Exception {
    Object[] values =
        ExtractionParser.parse(
            "{\"signed\":\"2026-03-01T10:30:00\"}", List.of(field("signed", "Date")));

    assertEquals("2026-03-01", new SimpleDateFormat("yyyy-MM-dd").format(values[0]));
  }

  @Test
  void aTimestampIsAnSqlTimestampAndKeepsItsTime() throws Exception {
    // ValueMetaTimestamp casts its native value to java.sql.Timestamp.
    Object[] values =
        ExtractionParser.parse(
            "{\"opened\":\"2026-03-01T10:30:15\"}", List.of(field("opened", "Timestamp")));

    Timestamp opened = assertInstanceOf(Timestamp.class, values[0]);
    assertEquals("2026-03-01 10:30:15", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(opened));
  }

  @Test
  void acceptsATimeSeparatedByASpaceOrWithAnOffset() throws Exception {
    Object[] values =
        ExtractionParser.parse(
            "{\"a\":\"2026-03-01 10:30:00\",\"b\":\"2026-03-01T10:30:00Z\"}",
            List.of(field("a", "Timestamp"), field("b", "Timestamp")));

    assertEquals(
        "2026-03-01 10:30:00", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(values[0]));
    assertEquals(
        java.time.Instant.parse("2026-03-01T10:30:00Z"), ((Timestamp) values[1]).toInstant());
  }

  @Test
  void aTimestampGivenOnlyADateIsMidnight() throws Exception {
    Object[] values =
        ExtractionParser.parse(
            "{\"opened\":\"2026-03-01\"}", List.of(field("opened", "Timestamp")));

    assertEquals(
        "2026-03-01 00:00:00", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(values[0]));
  }

  @Test
  void rejectsADateWithTrailingText() {
    // Parsing only the start would make this midnight on the first of March, a valid-looking lie.
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                ExtractionParser.parse(
                    "{\"signed\":\"2026-03-01 oops\"}", List.of(field("signed", "Date"))));

    assertTrue(e.getMessage().contains("2026-03-01 oops"), e.getMessage());
  }

  @Test
  void rejectsADateThatDoesNotExist() {
    assertThrows(
        HopException.class,
        () ->
            ExtractionParser.parse(
                "{\"signed\":\"2026-02-30\"}", List.of(field("signed", "Date"))));
  }

  @Test
  void readsAFieldWhoseNameHasSurroundingSpaces() throws Exception {
    // The schema asks for "severity", so the answer has to be read back under that name too.
    Object[] values =
        ExtractionParser.parse("{\"severity\":\"high\"}", List.of(field(" severity ", "String")));

    assertEquals("high", values[0]);
  }

  @Test
  void rejectsAValueOutsideTheAllowedList() {
    // On the prompt-only path nothing stops the model from answering outside the enum.
    StructuredExtractField severity = field("severity", "String");
    severity.setAllowedValues("low, medium, high");

    HopException e =
        assertThrows(
            HopException.class,
            () -> ExtractionParser.parse("{\"severity\":\"critical\"}", List.of(severity)));

    assertTrue(e.getMessage().contains("severity"), e.getMessage());
    assertTrue(e.getMessage().contains("critical"), e.getMessage());
    assertTrue(e.getMessage().contains("low, medium, high"), e.getMessage());
  }

  @Test
  void rejectsAnEmptyAnswerForARequiredFieldWithAllowedValues() {
    // Coercion turns an empty string into null, so without this check it would pass silently.
    StructuredExtractField severity = field("severity", "String");
    severity.setAllowedValues("low, medium, high");

    HopException e =
        assertThrows(
            HopException.class,
            () -> ExtractionParser.parse("{\"severity\":\"\"}", List.of(severity)));

    assertTrue(e.getMessage().contains("not one of"), e.getMessage());
  }

  @Test
  void anEmptyAnswerForAnOptionalFieldWithAllowedValuesIsNull() throws Exception {
    StructuredExtractField severity =
        new StructuredExtractField("severity", "String", "how urgent", false);
    severity.setAllowedValues("low, medium, high");

    assertNull(ExtractionParser.parse("{\"severity\":\"\"}", List.of(severity))[0]);
  }

  @Test
  void acceptsAnAllowedValueAndAnOptionalNull() throws Exception {
    StructuredExtractField severity = field("severity", "String");
    severity.setAllowedValues("low, medium, high");
    StructuredExtractField priority = new StructuredExtractField("priority", "Integer", "p", false);
    priority.setAllowedValues("1,2,3");

    Object[] values =
        ExtractionParser.parse(
            "{\"severity\":\"medium\",\"priority\":null}", List.of(severity, priority));

    assertEquals("medium", values[0]);
    assertNull(values[1]);
    assertEquals(
        2L,
        ExtractionParser.parse(
            "{\"severity\":\"low\",\"priority\":2}", List.of(severity, priority))[1]);
  }

  @Test
  void reportsAnAnswerThatIsNotAnObject() {
    HopException e =
        assertThrows(
            HopException.class,
            () -> ExtractionParser.parse("\"just a string\"", List.of(field("name", "String"))));

    assertTrue(e.getMessage().contains("JSON object"), e.getMessage());
  }

  @Test
  void reportsAnAnswerThatIsNotJsonAtAll() {
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                ExtractionParser.parse(
                    "I could not find that information.", List.of(field("name", "String"))));

    assertTrue(e.getMessage().contains("readable JSON"), e.getMessage());
  }

  @Test
  void reportsAnEmptyAnswer() {
    HopException e =
        assertThrows(
            HopException.class,
            () -> ExtractionParser.parse("  ", List.of(field("name", "String"))));

    assertTrue(e.getMessage().contains("returned nothing"), e.getMessage());
  }

  private static StructuredExtractField field(String name, String type) {
    return new StructuredExtractField(name, type, "the " + name, true);
  }
}
