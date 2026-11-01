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
 *
 */

package org.apache.hop.run;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.parameters.NamedParameters;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/** A declared parameter gets the -p value or its default, never a same-named variable (#9003). */
class HopRunParametersTest {

  private static final String PARAMETER = "P_2517";

  @Test
  void sameNamedVariableDoesNotReplaceTheDefault() throws Exception {
    IVariables variables = run(PARAMETER, "from-project");

    assertEquals("from-default", variables.getVariable(PARAMETER));
  }

  @Test
  void commandLineValueReplacesTheDefault() throws Exception {
    IVariables variables = run(PARAMETER, "from-project", "-p", PARAMETER + "=from-command-line");

    assertEquals("from-command-line", variables.getVariable(PARAMETER));
  }

  @Test
  void undeclaredNameStaysAVariable() throws Exception {
    IVariables variables = run("OTHER", "from-project", "-p", "OTHER=from-command-line");

    assertEquals("from-command-line", variables.getVariable("OTHER"));
    assertEquals("from-default", variables.getVariable(PARAMETER));
  }

  /**
   * Runs the hop-run parameter handling for a pipeline or workflow that declares {@link #PARAMETER}
   * with a default, where the project already sets variable {@code name}.
   */
  private static IVariables run(String name, String value, String... args) throws Exception {
    HopRun hopRun = new HopRun();
    CommandLine cmd = new CommandLine(hopRun);
    cmd.parseArgs(args);

    NamedParameters namedParams = new NamedParameters();
    namedParams.addParameterDefinition(PARAMETER, "from-default", "");

    // Like the engine created by hop-run: initialized from hop-run's own variables
    //
    IVariables variables = new Variables();
    variables.initializeFrom(null);
    variables.setVariable(name, value);

    PipelineExecutionConfiguration configuration = new PipelineExecutionConfiguration();
    hopRun.parseParametersAndVariables(cmd, configuration, namedParams);
    hopRun.configureParametersAndVariables(cmd, configuration, variables, namedParams);
    namedParams.activateParameters(variables);
    return variables;
  }
}
