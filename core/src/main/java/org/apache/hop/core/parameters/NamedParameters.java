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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class is an implementation of INamedParameters.
 *
 * @author Sven Boden
 */
public class NamedParameters implements INamedParameters {
  /** Map to store named parameters in. */
  protected Map<String, NamedParameter> params = new HashMap<>();

  /** Default constructor. */
  public NamedParameters() {}

  @Override
  public void addParameterDefinition(String key, String defValue, String description)
      throws DuplicateParamException {

    if (params.get(key) == null) {
      NamedParameter oneParam = new NamedParameter();

      oneParam.key = key;
      oneParam.defaultValue = defValue;
      oneParam.description = description;
      oneParam.value = "";

      params.put(key, oneParam);
    } else {
      throw new DuplicateParamException("Duplicate parameter '" + key + "' detected.");
    }
  }

  @Override
  public String getParameterDescription(String key) throws UnknownParamException {
    String description = null;

    NamedParameter theParam = params.get(key);
    if (theParam != null) {
      description = theParam.description;
    }

    return description;
  }

  @Override
  public String getParameterValue(String key) throws UnknownParamException {
    String value = null;

    NamedParameter theParam = params.get(key);
    if (theParam != null) {
      value = theParam.value;
    }

    return value;
  }

  @Override
  public String getParameterDefault(String key) throws UnknownParamException {
    String value = null;

    NamedParameter theParam = params.get(key);
    if (theParam != null) {
      value = theParam.defaultValue;
    }

    return value;
  }

  @Override
  public String[] listParameters() {
    Set<String> keySet = params.keySet();

    String[] paramArray = keySet.toArray(new String[0]);
    Arrays.sort(paramArray);

    return paramArray;
  }

  @Override
  public void setParameterValue(String key, String value) {
    NamedParameter theParam = params.get(key);
    if (theParam != null) {
      theParam.value = value;
    }
  }

  @Override
  public void removeAllParameters() {
    params.clear();
  }

  @Override
  public void clearParameterValues() {
    String[] keys = listParameters();
    for (int idx = 0; idx < keys.length; idx++) {
      NamedParameter theParam = params.get(keys[idx]);
      if (theParam != null) {
        theParam.value = "";
      }
    }
  }

  @Override
  public void activateParameters(IVariables variables) {
    for (NamedParameter param : params.values()) {
      if (StringUtils.isNotEmpty(param.key)) {
        variables.setVariable(param.key, Const.NVL(param.value, Const.NVL(param.defaultValue, "")));
      }
    }
  }

  /**
   * Copy parameter definitions into these parameters with empty value. Parameters which already
   * exists are untouched.
   *
   * @param definitions The
   */
  @Override
  public void copyParametersFromDefinitions( INamedParameterDefinitions definitions) {
    for (String name : definitions.listParameters()) {
      try {
        String defaultValue = definitions.getParameterDefault(name);
        String description = definitions.getParameterDescription(name);
        addParameterDefinition(name, defaultValue, description);
      } catch (Exception e) {
        // Ignore duplicates
      }
    }
  }
}
