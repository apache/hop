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

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.VariableValue;
import org.apache.hop.testing.util.DataSetConst;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;

/**
 * When a pipeline unit test is selected in Hop GUI, copy its configured variables into the pipeline
 * execution configuration so they appear on the Variables tab of the run dialog.
 */
@ExtensionPoint(
    extensionPointId = "HopGuiPipelineExecutionConfiguration",
    id = "HopGuiUnitTestVariablesExtensionPoint",
    description =
        "Add variables defined in the active pipeline unit test to the execution configuration dialog")
public class HopGuiUnitTestVariablesExtensionPoint
    implements IExtensionPoint<PipelineExecutionConfiguration> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, PipelineExecutionConfiguration executionConfiguration)
      throws HopException {

    HopGuiPipelineGraph activePipelineGraph = HopGui.getActivePipelineGraph();
    if (activePipelineGraph == null) {
      return;
    }

    Map<String, Object> stateMap = activePipelineGraph.getStateMap();
    if (stateMap == null) {
      return;
    }

    PipelineUnitTest unitTest =
        (PipelineUnitTest) stateMap.get(DataSetConst.STATE_KEY_ACTIVE_UNIT_TEST);
    if (unitTest == null) {
      return;
    }

    applyUnitTestVariables(
        executionConfiguration, unitTest, activePipelineGraph.getPipelineMeta(), variables);
  }

  /**
   * Copy unit test variables (and parameter-named entries) into the execution configuration maps.
   * Package-private for unit testing without Hop GUI.
   */
  static void applyUnitTestVariables(
      PipelineExecutionConfiguration executionConfiguration,
      PipelineUnitTest unitTest,
      PipelineMeta pipelineMeta,
      IVariables variables) {
    if (executionConfiguration == null || unitTest == null) {
      return;
    }

    List<VariableValue> variableValues = unitTest.getVariableValues();
    if (variableValues == null || variableValues.isEmpty()) {
      return;
    }

    String[] parameters = pipelineMeta != null ? pipelineMeta.listParameters() : new String[0];

    for (VariableValue variableValue : variableValues) {
      if (variableValue == null) {
        continue;
      }

      String key = variableValue.getKey();
      String value = variableValue.getValue();
      if (variables != null) {
        key = variables.resolve(key);
        value = variables.resolve(value);
      }
      if (StringUtils.isEmpty(key)) {
        continue;
      }

      String resolvedValue = Const.NVL(value, "");
      if (Const.indexOfString(key, parameters) < 0) {
        executionConfiguration.getVariablesMap().put(key, resolvedValue);
      } else {
        executionConfiguration.getParametersMap().put(key, resolvedValue);
      }
    }
  }
}
