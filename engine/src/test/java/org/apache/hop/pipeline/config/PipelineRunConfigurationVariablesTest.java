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

package org.apache.hop.pipeline.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

/**
 * Confirms configuration variable <em>values</em> are resolved while names stay literal (#7604).
 */
class PipelineRunConfigurationVariablesTest {

  @Test
  void applyToVariablesResolvesValueExpressions() {
    PipelineRunConfiguration config = new PipelineRunConfiguration();
    config
        .getConfigurationVariables()
        .add(new DescribedVariable("RUN_PATH", "${PROJECT_HOME}/data", "path"));

    Variables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "/tmp/project");
    config.applyToVariables(variables);

    assertEquals("/tmp/project/data", variables.getVariable("RUN_PATH"));
  }

  @Test
  void applyToVariablesDoesNotResolveVariableName() {
    PipelineRunConfiguration config = new PipelineRunConfiguration();
    config
        .getConfigurationVariables()
        .add(new DescribedVariable("${NAME_VAR}", "literal-value", null));

    Variables variables = new Variables();
    variables.setVariable("NAME_VAR", "RESOLVED_NAME");
    config.applyToVariables(variables);

    assertEquals("literal-value", variables.getVariable("${NAME_VAR}"));
    assertNull(variables.getVariable("RESOLVED_NAME"));
  }
}
