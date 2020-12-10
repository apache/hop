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

import org.apache.hop.core.variables.IVariables;

/**
 * Interface to implement named parameters.
 *
 * @author Sven Boden
 */
public interface INamedParameters extends INamedParameterDefinitions {

  /**
   * Set the value of a parameter.
   *
   * @param key   key to set value of
   * @param value value to set it to.
   * @throws UnknownParamException Parameter 'key' is unknown.
   */
  void setParameterValue( String key, String value ) throws UnknownParamException;

  /**
   * Get the value of a parameter.
   *
   * @param key Key to get value for.
   * @return value of parameter key.
   * @throws UnknownParamException Parameter 'key' is unknown.
   */
  String getParameterValue( String key ) throws UnknownParamException;

  /**
   * Clear the values.
   */
  void removeAllParameters();

  /**
   * Activate the currently set parameters.  Apply the values set in the parameters to the specified variables.
   * @param variables where variables to apply the parameter values to
   */
  void activateParameters( IVariables variables );

  /**
   * Clear all parameters
   */
  void clearParameterValues();

  /**
   * Copy the parameters defined in the provided definitions with null values.
   * Parameters which already exists are untouched.
   *
   * @param definitions
   */
  void copyParametersFromDefinitions( INamedParameterDefinitions definitions);
}
