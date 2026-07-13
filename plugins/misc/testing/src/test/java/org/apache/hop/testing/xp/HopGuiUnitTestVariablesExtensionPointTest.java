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

package org.apache.hop.testing.xp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.VariableValue;
import org.junit.jupiter.api.Test;

class HopGuiUnitTestVariablesExtensionPointTest {

  @Test
  void applyUnitTestVariablesPutsEntriesInVariablesMap() {
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.getVariableValues().add(new VariableValue("UNIT_VAR", "unit-value"));
    unitTest.getVariableValues().add(new VariableValue("OTHER", "other-value"));

    PipelineExecutionConfiguration config = new PipelineExecutionConfiguration();
    config.getVariablesMap().put("EXISTING", "keep-me");

    HopGuiUnitTestVariablesExtensionPoint.applyUnitTestVariables(
        config, unitTest, new PipelineMeta(), new Variables());

    assertEquals("unit-value", config.getVariablesMap().get("UNIT_VAR"));
    assertEquals("other-value", config.getVariablesMap().get("OTHER"));
    assertEquals("keep-me", config.getVariablesMap().get("EXISTING"));
  }

  @Test
  void applyUnitTestVariablesOverridesExistingVariableWithSameName() {
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.getVariableValues().add(new VariableValue("SHARED", "from-unit-test"));

    PipelineExecutionConfiguration config = new PipelineExecutionConfiguration();
    config.getVariablesMap().put("SHARED", "from-previous-run");

    HopGuiUnitTestVariablesExtensionPoint.applyUnitTestVariables(
        config, unitTest, new PipelineMeta(), new Variables());

    assertEquals("from-unit-test", config.getVariablesMap().get("SHARED"));
  }

  @Test
  void applyUnitTestVariablesPutsParameterNamesInParametersMap() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addParameterDefinition("INPUT_PARAM", "default", "description");

    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.getVariableValues().add(new VariableValue("INPUT_PARAM", "param-value"));
    unitTest.getVariableValues().add(new VariableValue("PLAIN_VAR", "var-value"));

    PipelineExecutionConfiguration config = new PipelineExecutionConfiguration();

    HopGuiUnitTestVariablesExtensionPoint.applyUnitTestVariables(
        config, unitTest, pipelineMeta, new Variables());

    assertEquals("param-value", config.getParametersMap().get("INPUT_PARAM"));
    assertFalse(config.getVariablesMap().containsKey("INPUT_PARAM"));
    assertEquals("var-value", config.getVariablesMap().get("PLAIN_VAR"));
  }

  @Test
  void applyUnitTestVariablesSkipsEmptyKeysAndNullEntries() {
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.getVariableValues().add(null);
    unitTest.getVariableValues().add(new VariableValue("", "ignored"));
    unitTest.getVariableValues().add(new VariableValue(null, "ignored-too"));
    unitTest.getVariableValues().add(new VariableValue("OK", null));

    PipelineExecutionConfiguration config = new PipelineExecutionConfiguration();

    HopGuiUnitTestVariablesExtensionPoint.applyUnitTestVariables(
        config, unitTest, new PipelineMeta(), new Variables());

    assertEquals(1, config.getVariablesMap().size());
    assertTrue(config.getVariablesMap().containsKey("OK"));
    assertEquals("", config.getVariablesMap().get("OK"));
  }

  @Test
  void applyUnitTestVariablesResolvesKeyAndValueAgainstVariables() {
    Variables variables = new Variables();
    variables.setVariable("PREFIX", "env");
    variables.setVariable("SUFFIX", "prod");

    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.getVariableValues().add(new VariableValue("${PREFIX}_MODE", "${SUFFIX}"));

    PipelineExecutionConfiguration config = new PipelineExecutionConfiguration();

    HopGuiUnitTestVariablesExtensionPoint.applyUnitTestVariables(
        config, unitTest, new PipelineMeta(), variables);

    assertEquals("prod", config.getVariablesMap().get("env_MODE"));
  }

  @Test
  void applyUnitTestVariablesNoopsWhenNoUnitTestVariables() {
    PipelineUnitTest unitTest = new PipelineUnitTest();
    PipelineExecutionConfiguration config = new PipelineExecutionConfiguration();
    config.getVariablesMap().put("KEEP", "yes");

    HopGuiUnitTestVariablesExtensionPoint.applyUnitTestVariables(
        config, unitTest, new PipelineMeta(), new Variables());

    assertEquals(1, config.getVariablesMap().size());
    assertEquals("yes", config.getVariablesMap().get("KEEP"));
  }
}
