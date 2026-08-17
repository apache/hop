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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.VariableValue;

/**
 * Applies and clears pipeline unit test variables on the Hop GUI pipeline graph variable space so
 * design-time actions (get fields, check, transform dialogs) resolve the same sample values used
 * when the unit test is executed.
 */
public final class UnitTestGraphVariables {

  private UnitTestGraphVariables() {
    // utility
  }

  /**
   * Clear any previously applied unit-test variables from the target space, then apply the given
   * unit test's variable values. Tracks applied keys in {@code stateMap} for later cleanup.
   *
   * @param variables the pipeline graph variable space (or any target space)
   * @param unitTest the active unit test (may be null)
   * @param stateMap pipeline graph state map (may be null)
   */
  public static void apply(
      IVariables variables, PipelineUnitTest unitTest, Map<String, Object> stateMap) {
    clear(variables, stateMap);
    if (variables == null || unitTest == null || stateMap == null) {
      return;
    }

    List<VariableValue> variableValues = unitTest.getVariableValues();
    if (variableValues == null || variableValues.isEmpty()) {
      return;
    }

    Set<String> applied = new HashSet<>();
    for (VariableValue variableValue : variableValues) {
      if (variableValue == null) {
        continue;
      }
      // Resolve after clear() so a previous unit test's value for the same key cannot leak into
      // ${...} substitution of the new test's key/value expressions.
      String key = variables.resolve(Const.NVL(variableValue.getKey(), ""));
      String value = variables.resolve(Const.NVL(variableValue.getValue(), ""));
      if (StringUtils.isEmpty(key)) {
        continue;
      }
      variables.setVariable(key, value);
      applied.add(key);
    }

    if (!applied.isEmpty()) {
      stateMap.put(DataSetConst.STATE_KEY_APPLIED_UNIT_TEST_VARIABLES, applied);
    }
  }

  /**
   * Remove unit-test variables previously applied via {@link #apply} from the target space.
   *
   * @param variables the pipeline graph variable space (or any target space)
   * @param stateMap pipeline graph state map (may be null)
   */
  @SuppressWarnings("unchecked")
  public static void clear(IVariables variables, Map<String, Object> stateMap) {
    if (stateMap == null) {
      return;
    }
    Object stored = stateMap.remove(DataSetConst.STATE_KEY_APPLIED_UNIT_TEST_VARIABLES);
    if (variables == null || !(stored instanceof Set)) {
      return;
    }
    for (String key : (Set<String>) stored) {
      if (StringUtils.isNotEmpty(key)) {
        variables.setVariable(key, null);
      }
    }
  }
}
