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

package org.apache.hop.workflow.actions.setvariables;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.workflow.actions.setvariables.ActionSetVariables.VariableDefinition;
import org.apache.hop.workflow.actions.setvariables.ActionSetVariables.VariableType;
import org.junit.jupiter.api.Test;

/** Variables are listed on the action or read from a properties file. Either source is valid. */
class ActionSetVariablesCheckTest {

  @Test
  void aPropertiesFileDoesNotRequireListedVariables() {
    ActionSetVariables action = new ActionSetVariables();
    action.setFilename("${PROJECT_HOME}/retail.properties");

    assertTrue(errors(action).isEmpty());
  }

  @Test
  void listedVariablesDoNotRequireAPropertiesFile() {
    ActionSetVariables action = new ActionSetVariables();
    action
        .getVariableDefinitions()
        .add(new VariableDefinition("RETAIL_CSV_WAVE", "1", VariableType.CURRENT_WORKFLOW));

    assertTrue(errors(action).isEmpty());
  }

  @Test
  void anEmptyVariableRowIsIgnoredWhenAPropertiesFileIsSet() {
    ActionSetVariables action = new ActionSetVariables();
    action.setFilename("${PROJECT_HOME}/retail.properties");
    action.getVariableDefinitions().add(new VariableDefinition("", "", VariableType.JVM));

    assertTrue(errors(action).isEmpty());
  }

  @Test
  void nothingToSetIsReported() {
    ActionSetVariables action = new ActionSetVariables();

    List<String> errors = errors(action);
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("properties file"));
    assertTrue(errors.stream().noneMatch(text -> text.contains("variableName")));
  }

  private static List<String> errors(ActionSetVariables action) {
    List<ICheckResult> remarks = new ArrayList<>();
    action.check(remarks, null, null, null);
    List<String> errors = new ArrayList<>();
    for (ICheckResult remark : remarks) {
      if (remark.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        errors.add(remark.getText());
      }
    }
    return errors;
  }
}
