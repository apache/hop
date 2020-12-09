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

/**
 * This contains only parameter definitions without the possibility to actually give values to them.
 *
 */
public interface INamedParameterDefinitions {
  /**
   * Add a parameter definition to this set.
   *
   * @param key         Name of the parameter.
   * @param defValue    default value.
   * @param description Description of the parameter.
   * @throws DuplicateParamException Upon duplicate parameter definitions
   */
  void addParameterDefinition( String key, String defValue, String description ) throws DuplicateParamException;

  /**
   * Get the description of a parameter.
   *
   * @param key Key to get value for.
   * @return description of parameter key.
   * @throws UnknownParamException Parameter 'key' is unknown.
   */
  String getParameterDescription( String key ) throws UnknownParamException;

  /**
   * Get the default value of a parameter.
   *
   * @param key Key to get value for.
   * @return default value for parameter key.
   * @throws UnknownParamException Parameter 'key' is unknown.
   */
  String getParameterDefault( String key ) throws UnknownParamException;

  /**
   * List the parameters.
   *
   * @return Array of parameters.
   */
  String[] listParameters();

  /**
   * Remove all defined parameters
   */
  void removeAllParameters();
}
