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

package org.apache.hop.core.variables.resolver;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataObject;

/** Indicates that the class can resolve variables in a specific manner. */
@HopMetadataObject(objectFactory = VariableResolverObjectFactory.class)
public interface IVariableResolver {
  void init();

  /**
   * Resolve the variables in the given String.
   *
   * @param secretPath The
   * @param variables The variables and values to use as a reference
   * @return The input string with expressions resolved.
   * @throws HopException In case something goes wrong.
   */
  String resolve(String secretPath, IVariables variables) throws HopException;

  void setPluginId();

  String getPluginId();

  void setPluginName(String pluginName);

  String getPluginName();
}
