/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.calculator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.stream.Stream;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.DateToJdeJulian;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.JdeJulian;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.JdeJulianToDate;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class JdeJulianTest {
  private static final SimpleDateFormat ISO_DATE = new SimpleDateFormat("yyyy-MM-dd");

  private TransformMockHelper<CalculatorMeta, CalculatorData> smh;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void init() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    smh = new TransformMockHelper<>("Calculator", CalculatorMeta.class, CalculatorData.class);
    when(smh.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(smh.iLogChannel);
    when(smh.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void cleanUp() {
    smh.cleanUp();
  }

  static Stream<Arguments> sampleConversions() {
    return Stream.of(
        Arguments.of("1995-01-01", 95001L),
        Arguments.of("1997-11-04", 97308L),
        Arguments.of("1997-11-14", 97318L),
        Arguments.of("2000-08-23", 100236L),
        Arguments.of("2000-12-31", 100366L),
        Arguments.of("1999-12-31", 99365L),
        Arguments.of("2009-01-01", 109001L),
        Arguments.of("2011-01-13", 111013L),
        Arguments.of("1900-01-01", 1L),
        Arguments.of("2000-02-29", 100060L));
  }

  @ParameterizedTest
  @MethodSource("sampleConversions")
  void convertsDateToJulianAndBack(String isoDate, long julian) throws Exception {
    Date date = ISO_DATE.parse(isoDate);
    assertEquals(julian, JdeJulian.fromDate(date));
    assertEquals(midnight(date), JdeJulian.toDate(julian));
  }

  @Test
  void fromDateUsesDayOfYearAndIgnoresTime() throws Exception {
    Date afternoon = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2009-01-01 15:30:45");
    assertEquals(109001L, JdeJulian.fromDate(afternoon));
    assertEquals(ISO_DATE.parse("2009-01-01"), JdeJulian.toDate(109001L));
  }

  @Test
  void nullInputsStayNull() throws Exception {
    assertNull(JdeJulian.fromDate(null));
    assertNull(JdeJulian.toDate(null));

    DateToJdeJulian dateToJulian = new DateToJdeJulian();
    JdeJulianToDate julianToDate = new JdeJulianToDate();
    ValueMetaDate dateMeta = new ValueMetaDate("isoDate");
    ValueMetaInteger julianMeta = new ValueMetaInteger("julian");

    assertNull(
        dateToJulian.calculate(
                new CalculationInput(
                    dateMeta, null, null, null, null, null, null, null, null, false))
            .value);
    assertNull(
        julianToDate.calculate(
                new CalculationInput(
                    julianMeta, null, null, null, null, null, null, null, null, false))
            .value);
  }

  @Test
  void rejectsDatesBefore1900() throws Exception {
    Date date = ISO_DATE.parse("1899-12-31");
    HopValueException exception =
        assertThrows(HopValueException.class, () -> JdeJulian.fromDate(date));
    assertTrue(exception.getMessage().contains("1899-12-31"));
  }

  @Test
  void rejectsInvalidJulianValues() {
    assertThrows(HopValueException.class, () -> JdeJulian.toDate(0L));
    assertThrows(HopValueException.class, () -> JdeJulian.toDate(-1L));
    assertThrows(HopValueException.class, () -> JdeJulian.toDate(100000L));
    assertThrows(HopValueException.class, () -> JdeJulian.toDate(99366L));
    assertThrows(HopValueException.class, () -> JdeJulian.toDate(100367L));
  }

  @Test
  void julianToDateAcceptsIntegerNumberAndString() throws Exception {
    JdeJulianToDate calc = new JdeJulianToDate();
    Date expected = ISO_DATE.parse("1997-11-04");

    assertEquals(
        expected,
        calc.calculate(
                new CalculationInput(
                    new ValueMetaInteger("julian"),
                    null,
                    null,
                    97308L,
                    null,
                    null,
                    null,
                    null,
                    null,
                    false))
            .value);
    assertEquals(
        expected,
        calc.calculate(
                new CalculationInput(
                    new ValueMetaNumber("julian"),
                    null,
                    null,
                    97308.0,
                    null,
                    null,
                    null,
                    null,
                    null,
                    false))
            .value);
    assertEquals(
        expected,
        calc.calculate(
                new CalculationInput(
                    new ValueMetaString("julian"),
                    null,
                    null,
                    "097308",
                    null,
                    null,
                    null,
                    null,
                    null,
                    false))
            .value);
    assertEquals(
        ISO_DATE.parse("2009-01-01"),
        calc.calculate(
                new CalculationInput(
                    new ValueMetaString("julian"),
                    null,
                    null,
                    "109001",
                    null,
                    null,
                    null,
                    null,
                    null,
                    false))
            .value);
  }

  @Test
  void calculatorTransformRoundTrip() throws Exception {
    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaDate("isoDate"));
    inputRowMeta.addValueMeta(new ValueMetaInteger("julianIn"));

    Date isoDate = ISO_DATE.parse("2011-01-13");
    IRowSet inputRowSet = smh.getMockInputRowSet(new Object[][] {{isoDate, 111013L}});
    inputRowSet.setRowMeta(inputRowMeta);

    CalculatorMeta meta = new CalculatorMeta();
    meta.getFunctions()
        .add(
            new CalculatorMetaFunction(
                "julianOut",
                CalculationType.DATE_TO_JDE_JULIAN,
                "isoDate",
                null,
                null,
                "Integer",
                0,
                0,
                "",
                "",
                "",
                "",
                false));
    meta.getFunctions()
        .add(
            new CalculatorMetaFunction(
                "isoOut",
                CalculationType.JDE_JULIAN_TO_DATE,
                "julianIn",
                null,
                null,
                "Date",
                0,
                0,
                "yyyy-MM-dd",
                "",
                "",
                "",
                false));

    Calculator calculator =
        new Calculator(
            smh.transformMeta, meta, new CalculatorData(), 0, smh.pipelineMeta, smh.pipeline);
    calculator.addRowSetToInputRowSets(inputRowSet);
    calculator.setInputRowMeta(inputRowMeta);
    calculator.init();
    calculator.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
            assertEquals(111013L, row[2]);
            assertEquals(midnight(isoDate), row[3]);
          }
        });

    assertTrue(calculator.processRow());
  }

  @Test
  void calculationTypeCodesMatchMetadata() {
    assertEquals("DATE_TO_JDE_JULIAN", CalculationType.DATE_TO_JDE_JULIAN.getCode());
    assertEquals("JDE_JULIAN_TO_DATE", CalculationType.JDE_JULIAN_TO_DATE.getCode());
    assertEquals(
        CalculationType.DATE_TO_JDE_JULIAN,
        CalculationType.findByDescription(CalculationType.DATE_TO_JDE_JULIAN.getDescription()));
    assertEquals(
        CalculationType.JDE_JULIAN_TO_DATE,
        CalculationType.findByDescription(CalculationType.JDE_JULIAN_TO_DATE.getDescription()));
  }

  @Test
  void dateToJulianResultTypeIsInteger() throws Exception {
    DateToJdeJulian calc = new DateToJdeJulian();
    Date date = ISO_DATE.parse("2000-08-23");
    CalculationOutput output =
        calc.calculate(
            new CalculationInput(
                new ValueMetaDate("isoDate"),
                null,
                null,
                date,
                null,
                null,
                null,
                null,
                null,
                false));
    assertEquals(100236L, output.value);
    assertEquals(IValueMeta.TYPE_INTEGER, output.resultType);
  }

  private static Date midnight(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    return calendar.getTime();
  }
}
