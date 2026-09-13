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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.pipeline.transforms.formula.fast.FastFormulaCompiler;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

/**
 * Ensures the fast path produces exactly the same output rows as the regular POI path for every
 * formula the fast path supports. Each scenario is run once with the fast path enabled and once
 * with it disabled, and the captured output rows must be identical.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class FormulaFastPathParityTest {
  private TransformMockHelper<FormulaMeta, FormulaData> transformMockHelper;

  @BeforeEach
  void setUp() {
    FastFormulaCompiler.clear();
    transformMockHelper =
        new TransformMockHelper<>("Formula", FormulaMeta.class, FormulaData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);
    when(transformMockHelper.transformMeta.getName()).thenReturn("Formula");
  }

  @AfterEach
  void tearDown() {
    FastFormulaCompiler.clear();
    FastFormulaCompiler.setEnabled(true);
    transformMockHelper.cleanUp();
  }

  private record Field(IValueMeta meta, Object value) {}

  private record Scenario(
      String formula, int targetType, String outputField, List<Field> inputFields, boolean setNa) {}

  @Test
  void fastAndPoiProduceTheSameRowsForSupportedFormulas() throws Exception {
    List<Scenario> scenarios =
        List.of(
            new Scenario(
                "IF([score] >= 60, \"Pass\", \"Fail\")",
                IValueMeta.TYPE_STRING,
                "grade",
                List.of(new Field(new ValueMetaInteger("score"), 85L)),
                false),
            new Scenario(
                "IF([score] >= 60, \"Pass\", \"Fail\")",
                IValueMeta.TYPE_STRING,
                "grade",
                List.of(new Field(new ValueMetaInteger("score"), 40L)),
                false),
            new Scenario(
                "[left] + [right]",
                IValueMeta.TYPE_NUMBER,
                "sum",
                List.of(
                    new Field(new ValueMetaInteger("left"), 10L),
                    new Field(new ValueMetaInteger("right"), 20L)),
                false),
            new Scenario(
                "[left] + [right]",
                IValueMeta.TYPE_INTEGER,
                "sumInt",
                List.of(
                    new Field(new ValueMetaInteger("left"), 10L),
                    new Field(new ValueMetaInteger("right"), 20L)),
                false),
            new Scenario(
                "[qty] * [factor]",
                IValueMeta.TYPE_BIGNUMBER,
                "product",
                List.of(
                    new Field(new ValueMetaBigNumber("qty"), new BigDecimal("10.5")),
                    new Field(new ValueMetaNumber("factor"), 2.0)),
                false),
            new Scenario(
                "[qty] > 100",
                IValueMeta.TYPE_BOOLEAN,
                "big",
                List.of(new Field(new ValueMetaBigNumber("qty"), new BigDecimal("200.5"))),
                false),
            new Scenario(
                "[qty] > 100",
                IValueMeta.TYPE_BOOLEAN,
                "big",
                List.of(new Field(new ValueMetaBigNumber("qty"), new BigDecimal("20.5"))),
                false),
            new Scenario(
                "[label] = \"active\"",
                IValueMeta.TYPE_BOOLEAN,
                "match",
                List.of(new Field(new ValueMetaString("label"), "active")),
                false),
            new Scenario(
                "TRIM([name])",
                IValueMeta.TYPE_STRING,
                "clean",
                List.of(new Field(new ValueMetaString("name"), "  a   b  ")),
                false),
            new Scenario(
                "ISBLANK([comment])",
                IValueMeta.TYPE_BOOLEAN,
                "blank",
                List.of(new Field(new ValueMetaString("comment"), null)),
                false),
            new Scenario(
                "ISNA([comment])",
                IValueMeta.TYPE_BOOLEAN,
                "na",
                List.of(new Field(new ValueMetaString("comment"), null)),
                false),
            new Scenario(
                "ISNA([comment])",
                IValueMeta.TYPE_BOOLEAN,
                "naWithOption",
                List.of(new Field(new ValueMetaString("comment"), null)),
                true),
            new Scenario(
                "ABS([delta])",
                IValueMeta.TYPE_NUMBER,
                "abs",
                List.of(new Field(new ValueMetaNumber("delta"), -5.0)),
                false),
            new Scenario(
                "LEN([code])",
                IValueMeta.TYPE_NUMBER,
                "len",
                List.of(new Field(new ValueMetaString("code"), "Hop ")),
                false),
            new Scenario(
                "[prefix] & \"-\" & [num]",
                IValueMeta.TYPE_STRING,
                "id",
                List.of(
                    new Field(new ValueMetaString("prefix"), "id"),
                    new Field(new ValueMetaInteger("num"), 7L)),
                false),
            new Scenario(
                "[amount] * [factor]",
                IValueMeta.TYPE_NUMBER,
                "scaled",
                List.of(
                    new Field(new ValueMetaInteger("amount"), null),
                    new Field(new ValueMetaNumber("factor"), 2.0)),
                false),
            new Scenario(
                "IF([score] >= 60, \"Pass\")",
                IValueMeta.TYPE_STRING,
                "grade2",
                List.of(new Field(new ValueMetaInteger("score"), 40L)),
                false));

    for (Scenario scenario : scenarios) {
      List<Object[]> fastRows = run(scenario, true);
      List<Object[]> poiRows = run(scenario, false);
      assertRowsEqual(scenario, fastRows, poiRows);
    }
  }

  private void assertRowsEqual(Scenario scenario, List<Object[]> fast, List<Object[]> poi) {
    assertEquals(
        fast.size(),
        poi.size(),
        "row count differs for " + scenario.formula() + ": fast=" + fast + " poi=" + poi);
    for (int i = 0; i < fast.size(); i++) {
      assertEquals(
          Arrays.asList(fast.get(i)),
          Arrays.asList(poi.get(i)),
          "row " + i + " differs for " + scenario.formula());
    }
  }

  private List<Object[]> run(Scenario scenario, boolean enabled) throws HopException {
    FastFormulaCompiler.setEnabled(enabled);
    try {
      FastFormulaCompiler.clear();

      RowMeta inputMeta = new RowMeta();
      for (Field field : scenario.inputFields()) {
        inputMeta.addValueMeta(field.meta());
      }

      FormulaMeta meta = new FormulaMeta();
      meta.getFormulas()
          .add(
              new FormulaMetaFunction(
                  scenario.outputField(),
                  scenario.formula(),
                  scenario.targetType(),
                  -1,
                  -1,
                  "",
                  scenario.setNa()));

      Formula formula =
          spy(
              new Formula(
                  transformMockHelper.transformMeta,
                  meta,
                  new FormulaData(),
                  0,
                  transformMockHelper.pipelineMeta,
                  transformMockHelper.pipeline));
      when(transformMockHelper.transformMeta.isDoingErrorHandling()).thenReturn(false);
      org.junit.jupiter.api.Assertions.assertTrue(
          formula.init(), "init failed for " + scenario.formula());
      formula.setInputRowMeta(inputMeta);

      Object[] inputRow = new Object[scenario.inputFields().size()];
      for (int i = 0; i < scenario.inputFields().size(); i++) {
        inputRow[i] = scenario.inputFields().get(i).value();
      }
      doReturn(inputRow).doReturn(null).when(formula).getRow();

      ArgumentCaptor<Object[]> rows = ArgumentCaptor.forClass(Object[].class);
      doNothing().when(formula).putRow(any(IRowMeta.class), rows.capture());

      org.junit.jupiter.api.Assertions.assertTrue(formula.processRow());
      formula.processRow();
      formula.dispose();

      List<Object[]> captured = new ArrayList<>(rows.getAllValues());
      return captured;
    } finally {
      FastFormulaCompiler.clear();
    }
  }
}
