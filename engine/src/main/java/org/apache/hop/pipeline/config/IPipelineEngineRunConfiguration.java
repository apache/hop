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

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataObject;

@HopMetadataObject(objectFactory = PipelineRunConfigurationMetadataObjectFactory.class)
public interface IPipelineEngineRunConfiguration extends Cloneable, IVariables {

  IPipelineEngineRunConfiguration clone();

  void setEnginePluginId(String pluginId);

  String getEnginePluginId();

  void setEnginePluginName(String name);

  String getEnginePluginName();

  /**
   * The class that adjusts this run configuration's widgets in the run configuration editor, for
   * example to hide the options that do not apply to the settings made so far. The class implements
   * {@code IGuiPluginCompositeWidgetsListener} and has a public constructor taking this {@link
   * IPipelineEngineRunConfiguration}. It is given as a name so that the engine can run without the
   * GUI on the classpath.
   *
   * @return the class name, or null when the widgets need no adjusting
   */
  default String getGuiWidgetsListenerClassName() {
    return null;
  }
}
