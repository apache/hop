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

package org.apache.hop.testing.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.VariableValue;
import org.junit.jupiter.api.Test;

class UnitTestGraphVariablesTest {

  @Test
  void applySetsUnitTestVariablesOnTargetSpace() {
    Variables variables = new Variables();
    Map<String, Object> stateMap = new HashMap<>();
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.getVariableValues().add(new VariableValue("UNIT_VAR", "unit-value"));
    unitTest.getVariableValues().add(new VariableValue("OTHER", "other-value"));

    UnitTestGraphVariables.apply(variables, unitTest, stateMap);

    assertEquals("unit-value", variables.getVariable("UNIT_VAR"));
    assertEquals("other-value", variables.getVariable("OTHER"));
    assertTrue(stateMap.containsKey(DataSetConst.STATE_KEY_APPLIED_UNIT_TEST_VARIABLES));
  }

  @Test
  void clearRemovesAppliedVariables() {
    Variables variables = new Variables();
    Map<String, Object> stateMap = new HashMap<>();
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.getVariableValues().add(new VariableValue("UNIT_VAR", "unit-value"));

    UnitTestGraphVariables.apply(variables, unitTest, stateMap);
    UnitTestGraphVariables.clear(variables, stateMap);

    assertNull(variables.getVariable("UNIT_VAR"));
    assertNull(stateMap.get(DataSetConst.STATE_KEY_APPLIED_UNIT_TEST_VARIABLES));
  }

  @Test
  void applyReplacesPreviousUnitTestVariables() {
    Variables variables = new Variables();
    Map<String, Object> stateMap = new HashMap<>();

    PipelineUnitTest testA = new PipelineUnitTest();
    testA.getVariableValues().add(new VariableValue("ONLY_A", "a"));
    testA.getVariableValues().add(new VariableValue("SHARED", "from-a"));

    PipelineUnitTest testB = new PipelineUnitTest();
    testB.getVariableValues().add(new VariableValue("ONLY_B", "b"));
    testB.getVariableValues().add(new VariableValue("SHARED", "from-b"));

    UnitTestGraphVariables.apply(variables, testA, stateMap);
    UnitTestGraphVariables.apply(variables, testB, stateMap);

    assertNull(variables.getVariable("ONLY_A"));
    assertEquals("b", variables.getVariable("ONLY_B"));
    assertEquals("from-b", variables.getVariable("SHARED"));
  }

  @Test
  void applySwitchingSameKeyUpdatesValue() {
    // Mirrors two unit tests for one pipeline that both set UNIT_TEST_VAR
    Variables variables = new Variables();
    Map<String, Object> stateMap = new HashMap<>();

    PipelineUnitTest test1 = new PipelineUnitTest();
    test1.setName("test UNIT");
    test1.getVariableValues().add(new VariableValue("UNIT_TEST_VAR", "value1"));

    PipelineUnitTest test2 = new PipelineUnitTest();
    test2.setName("test UNIT 2");
    test2.getVariableValues().add(new VariableValue("UNIT_TEST_VAR", "value2"));

    UnitTestGraphVariables.apply(variables, test1, stateMap);
    assertEquals("value1", variables.getVariable("UNIT_TEST_VAR"));

    UnitTestGraphVariables.apply(variables, test2, stateMap);
    assertEquals("value2", variables.getVariable("UNIT_TEST_VAR"));

    UnitTestGraphVariables.apply(variables, test1, stateMap);
    assertEquals("value1", variables.getVariable("UNIT_TEST_VAR"));
  }

  @Test
  void applyResolvesKeyAndValue() {
    Variables variables = new Variables();
    variables.setVariable("PREFIX", "env");
    variables.setVariable("SUFFIX", "prod");
    Map<String, Object> stateMap = new HashMap<>();

    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.getVariableValues().add(new VariableValue("${PREFIX}_MODE", "${SUFFIX}"));

    UnitTestGraphVariables.apply(variables, unitTest, stateMap);

    assertEquals("prod", variables.getVariable("env_MODE"));
  }

  @Test
  void applySkipsEmptyKeys() {
    Variables variables = new Variables();
    Map<String, Object> stateMap = new HashMap<>();
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.getVariableValues().add(null);
    unitTest.getVariableValues().add(new VariableValue("", "ignored"));
    unitTest.getVariableValues().add(new VariableValue(null, "ignored-too"));
    unitTest.getVariableValues().add(new VariableValue("OK", null));

    UnitTestGraphVariables.apply(variables, unitTest, stateMap);

    assertEquals("", variables.getVariable("OK"));
    assertNull(variables.getVariable(""));
  }

  @Test
  void applyNoopsSafelyWithNulls() {
    UnitTestGraphVariables.apply(null, null, null);
    UnitTestGraphVariables.clear(null, null);

    Variables variables = new Variables();
    variables.setVariable("KEEP", "yes");
    UnitTestGraphVariables.apply(variables, new PipelineUnitTest(), new HashMap<>());
    assertEquals("yes", variables.getVariable("KEEP"));
  }
}
