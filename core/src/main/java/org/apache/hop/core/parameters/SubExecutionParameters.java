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

package org.apache.hop.core.parameters;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;

/**
 * Decides which values a child pipeline or workflow starts with.
 *
 * <p>Every action or transform that executes a child pipeline or workflow - the Pipeline and
 * Workflow actions, the Pipeline and Workflow Executor transforms, the Mapping transforms and
 * Metadata Injection - resolves parameters the same way:
 *
 * <ol>
 *   <li>A value on the caller's <i>Parameters</i> tab wins. That is how a child default is
 *       overridden for a single execution. An empty value there means nothing was passed, so the
 *       child's own default applies - it is never filled in from a leftover value.
 *   <li>Otherwise, when the caller passes parent values down, a parameter the child declares is
 *       seeded from the caller's value of the same name.
 *   <li>Otherwise the child's own default applies. A declared parameter isolates the child from
 *       unrelated same-named values in the caller's scope (issue #8084).
 * </ol>
 *
 * <p>Note that this governs <i>parameters</i> only. Variables are always inherited by a child
 * execution, whatever the caller decides here.
 */
public class SubExecutionParameters {

  private SubExecutionParameters() {
    // Utility class
  }

  /**
   * Apply the caller's parameter values to a child pipeline or workflow and activate them.
   *
   * @param childVariables the child's variable space
   * @param childParameters the child's named parameters
   * @param parent the caller's variable space, used to resolve values and to pass values down
   * @param childParameterNames the names the child declares as parameters
   * @param mappingNames the parameter names listed on the caller's Parameters tab
   * @param mappingValues the values for those names, positionally matched to mappingNames
   * @param passingParentValues pass the caller's value of a same-named parameter or variable down
   *     to a parameter the child declares but the Parameters tab does not list
   * @param resolveMappingValues resolve variable expressions in mappingValues against the parent.
   *     Pass false when the caller resolved the values itself, so they are not resolved twice.
   */
  public static void activate(
      IVariables childVariables,
      INamedParameters childParameters,
      IVariables parent,
      String[] childParameterNames,
      String[] mappingNames,
      String[] mappingValues,
      boolean passingParentValues,
      boolean resolveMappingValues) {

    Set<String> declaredByChild =
        new HashSet<>(
            Arrays.asList(childParameterNames == null ? new String[0] : childParameterNames));

    // Rule 1: what the caller listed on its Parameters tab. An empty cell means the caller
    // passed nothing, so the child's own default applies below. What it must never do is pick up
    // a same-named value still lying around in the caller's scope from an earlier row.
    //
    Map<String, String> values = new LinkedHashMap<>();
    if (mappingNames != null) {
      for (int i = 0; i < mappingNames.length; i++) {
        String name = mappingNames[i];
        if (Utils.isEmpty(name)) {
          continue;
        }
        String value = mappingValues == null || i >= mappingValues.length ? null : mappingValues[i];
        if (resolveMappingValues) {
          value = parent.resolve(value);
        }
        values.put(name, Const.NVL(value, ""));
      }
    }

    // Rule 2: pass the caller's own value down for the parameters it did not list.
    //
    if (passingParentValues) {
      for (String name : parent.getVariableNames()) {
        if (!values.containsKey(name) && declaredByChild.contains(name)) {
          values.put(name, Const.NVL(parent.getVariable(name), ""));
        }
      }
    }

    for (Map.Entry<String, String> entry : values.entrySet()) {
      String name = entry.getKey();
      String value = entry.getValue();
      if (!declaredByChild.contains(name)) {
        // A name the child does not declare. Declare it there so the child can use it, the same
        // way it can use any parameter it declared itself.
        //
        try {
          childParameters.addParameterDefinition(name, "", "");
        } catch (DuplicateParamException e) {
          // Already defined, just set the value below.
        }
        childVariables.setVariable(name, value);
      }
      try {
        childParameters.setParameterValue(name, value);
      } catch (UnknownParamException e) {
        // Defined right above, so this cannot happen.
      }
    }

    // Rule 3: everything the caller did not provide falls back to the child's own default.
    //
    childParameters.activateParameters(childVariables);
  }
}
