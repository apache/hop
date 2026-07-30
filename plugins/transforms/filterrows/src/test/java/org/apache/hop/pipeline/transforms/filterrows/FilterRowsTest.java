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

package org.apache.hop.pipeline.transforms.filterrows;

import static org.apache.hop.core.Condition.Function.EQUAL;
import static org.apache.hop.core.Condition.Function.REGEXP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.stream.Stream;
import org.apache.hop.core.Condition;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FilterRowsTest {

  @BeforeAll
  static void setUp() throws Exception {
    HopEnvironment.init();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("variableValueTypes")
  void resolvesAndConvertsVariableForSupportedScalarTypes(
      String type,
      IValueMeta valueMeta,
      Object matchingValue,
      Object differentValue,
      String variable)
      throws Exception {
    Condition.CValue constant =
        new Condition.CValue(new ValueMetaAndData(valueMeta, matchingValue));
    constant.setText("${FILTER_VALUE}");
    Condition metadataCondition = new Condition();
    metadataCondition.setLeftValueName("value");
    metadataCondition.setFunction(EQUAL);
    metadataCondition.setRightValue(constant);

    FilterRows transform = createTransform(metadataCondition);
    transform.setVariable("FILTER_VALUE", variable);
    Condition runtimeCondition = metadataCondition.clone();
    transform.resolveVariables(runtimeCondition);

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(valueMeta.clone());
    assertEquals(variable, runtimeCondition.getRightValueString());
    assertEquals("${FILTER_VALUE}", metadataCondition.getRightValueString());
    assertTrue(runtimeCondition.evaluate(rowMeta, new Object[] {matchingValue}));
    assertFalse(runtimeCondition.evaluate(rowMeta, new Object[] {differentValue}));
  }

  @Test
  void resolvesVariablesInNestedConditionValuesWithoutChangingMetadata() throws Exception {
    Condition stringCondition =
        new Condition("name", EQUAL, null, new ValueMetaAndData("constant", "${NAME}"));
    Condition regexCondition =
        new Condition("code", REGEXP, null, new ValueMetaAndData("constant", "${CODE_REGEX}"));
    Condition condition = new Condition();
    condition.addCondition(stringCondition);
    condition.addCondition(regexCondition);

    FilterRows transform = createTransform(condition);
    transform.setVariable("NAME", "Alice");
    transform.setVariable("CODE_REGEX", "A-[0-9]+");

    Condition runtimeCondition = condition.clone();
    transform.resolveVariables(runtimeCondition);

    assertEquals("Alice", runtimeCondition.getCondition(0).getRightValueString());
    assertEquals("A-[0-9]+", runtimeCondition.getCondition(1).getRightValueString());
    assertEquals("${NAME}", condition.getCondition(0).getRightValueString());
    assertEquals("${CODE_REGEX}", condition.getCondition(1).getRightValueString());

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaString("code"));
    assertTrue(runtimeCondition.evaluate(rowMeta, new Object[] {"Alice", "A-123"}));
    assertFalse(runtimeCondition.evaluate(rowMeta, new Object[] {"Alice", "B-123"}));
  }

  private static Stream<Arguments> variableValueTypes() throws Exception {
    ValueMetaDate dateMeta = new ValueMetaDate("value");
    dateMeta.setConversionMask("yyyy-MM-dd");
    Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2026-07-29");
    Date otherDate = new SimpleDateFormat("yyyy-MM-dd").parse("2026-07-30");

    ValueMetaTimestamp timestampMeta = new ValueMetaTimestamp("value");
    timestampMeta.setConversionMask("yyyy-MM-dd HH:mm:ss.SSS");
    Timestamp timestamp = Timestamp.valueOf("2026-07-29 12:34:56.789");
    Timestamp otherTimestamp = Timestamp.valueOf("2026-07-29 12:34:57.789");

    return Stream.of(
        Arguments.of("String", new ValueMetaString("value"), "Alice", "Bob", "Alice"),
        Arguments.of("Integer", new ValueMetaInteger("value"), 42L, 43L, "42"),
        Arguments.of("Number", new ValueMetaNumber("value"), 12.5D, 13.5D, "12.5"),
        Arguments.of(
            "BigNumber",
            new ValueMetaBigNumber("value"),
            new BigDecimal("123456789.0123"),
            new BigDecimal("123456789.0124"),
            "123456789.0123"),
        Arguments.of("Boolean", new ValueMetaBoolean("value"), true, false, "Y"),
        Arguments.of("Date", dateMeta, date, otherDate, "2026-07-29"),
        Arguments.of(
            "Timestamp", timestampMeta, timestamp, otherTimestamp, "2026-07-29 12:34:56.789"));
  }

  private static FilterRows createTransform(Condition condition) {
    FilterRowsMeta meta = new FilterRowsMeta();
    meta.setCondition(condition);
    TransformMeta transformMeta = new TransformMeta("Filter rows", meta);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(transformMeta);
    return new FilterRows(
        transformMeta, meta, new FilterRowsData(), 0, pipelineMeta, new LocalPipelineEngine());
  }
}
