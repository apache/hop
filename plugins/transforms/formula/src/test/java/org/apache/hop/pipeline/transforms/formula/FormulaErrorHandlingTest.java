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

package org.apache.hop.pipeline.transforms.formula;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for the way {@link Formula} reports formulas that can not be calculated. Excel returns
 * an error value instead of throwing, which used to leave the output field silently blank.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class FormulaErrorHandlingTest {
  private TransformMockHelper<FormulaMeta, FormulaData> transformMockHelper;

  @BeforeEach
  void setUp() {
    transformMockHelper =
        new TransformMockHelper<>("Formula", FormulaMeta.class, FormulaData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);
    when(transformMockHelper.transformMeta.getName()).thenReturn("Formula");
  }

  @AfterEach
  void tearDown() {
    transformMockHelper.cleanUp();
  }

  @Test
  void naIsReturnedAsNull() throws Exception {
    // #N/A means "no value available" and is produced on purpose by the "Set Null to #N/A"
    // option, so it has to stay a null rather than fail the row.
    Formula formula = createTransform("NA()", IValueMeta.TYPE_NUMBER);
    ArgumentCaptor<Object[]> rows = ArgumentCaptor.forClass(Object[].class);
    doNothing().when(formula).putRow(any(IRowMeta.class), rows.capture());

    assertTrue(formula.processRow());

    assertNull(rows.getValue()[1]);
    verify(formula, never()).putError(any(), any(), anyLong(), anyString(), any(), anyString());
    formula.dispose();
  }

  @Test
  void divisionByZeroStopsThePipelineWithoutErrorHandling() throws Exception {
    Formula formula = createTransform("1/0", IValueMeta.TYPE_NUMBER);
    when(transformMockHelper.transformMeta.isDoingErrorHandling()).thenReturn(false);

    assertFalse(formula.processRow());

    assertEquals(1, formula.getErrors());
    verify(formula, never()).putRow(any(), any());
    verify(formula, never()).putError(any(), any(), anyLong(), anyString(), any(), anyString());
    formula.dispose();
  }

  @Test
  void divisionByZeroGoesToTheErrorStreamWithErrorHandling() throws Exception {
    Formula formula = createTransform("1/0", IValueMeta.TYPE_NUMBER);
    when(transformMockHelper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    doNothing().when(formula).putError(any(), any(), anyLong(), anyString(), any(), anyString());

    // the row is diverted, the transform keeps going
    assertTrue(formula.processRow());

    ArgumentCaptor<String> description = ArgumentCaptor.forClass(String.class);
    verify(formula, times(1))
        .putError(any(), any(), anyLong(), description.capture(), any(), anyString());
    assertTrue(description.getValue().contains("#DIV/0!"), description.getValue());
    verify(formula, never()).putRow(any(), any());
    formula.dispose();
  }

  @Test
  void dateBeforeTheExcelEpochGoesToTheErrorStream() throws Exception {
    // Before this was reported, the pre-1900 date silently became POI's BAD_DATE and the field
    // came out blank. See https://github.com/apache/hop/issues/3572
    Date tooOld =
        Date.from(LocalDate.of(1800, 1, 1).atStartOfDay(ZoneId.systemDefault()).toInstant());
    Formula formula =
        createTransform(
            "IF(1=2, DATE(2000,1,1), [start])",
            IValueMeta.TYPE_DATE,
            new ValueMetaDate("start"),
            tooOld);
    when(transformMockHelper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    doNothing().when(formula).putError(any(), any(), anyLong(), anyString(), any(), anyString());

    assertTrue(formula.processRow());

    ArgumentCaptor<String> description = ArgumentCaptor.forClass(String.class);
    verify(formula, times(1))
        .putError(any(), any(), anyLong(), description.capture(), any(), anyString());
    assertTrue(description.getValue().contains("1899-12-31"), description.getValue());
    verify(formula, never()).putRow(any(), any());
    formula.dispose();
  }

  private Formula createTransform(String expression, int valueType) throws HopException {
    return createTransform(expression, valueType, new ValueMetaInteger("amount"), 10L);
  }

  /** Builds a Formula transform with a single formula, fed a single row holding one field. */
  private Formula createTransform(
      String expression, int valueType, IValueMeta inputMeta, Object inputValue)
      throws HopException {
    FormulaMeta meta = new FormulaMeta();
    meta.getFormulas()
        .add(new FormulaMetaFunction("result", expression, valueType, -1, -1, "", false));

    Formula formula =
        spy(
            new Formula(
                transformMockHelper.transformMeta,
                meta,
                new FormulaData(),
                0,
                transformMockHelper.pipelineMeta,
                transformMockHelper.pipeline));
    assertTrue(formula.init());

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(inputMeta);
    formula.setInputRowMeta(inputRowMeta);

    doReturn(new Object[] {inputValue}).when(formula).getRow();
    return formula;
  }
}
