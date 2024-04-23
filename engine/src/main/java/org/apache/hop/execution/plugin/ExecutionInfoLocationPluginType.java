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

package org.apache.hop.execution.plugin;

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;
import org.apache.hop.execution.IExecutionInfoLocation;

@PluginMainClassType(IExecutionInfoLocation.class)
@PluginAnnotationType(ExecutionInfoLocationPlugin.class)
public class ExecutionInfoLocationPluginType extends BasePluginType<ExecutionInfoLocationPlugin> {

  private ExecutionInfoLocationPluginType() {
    super(
        ExecutionInfoLocationPlugin.class,
        "EXECUTION_INFO_LOCATIONS",
        "Execution Information Locations");
  }

  private static ExecutionInfoLocationPluginType pluginType;

  public static ExecutionInfoLocationPluginType getInstance() {
    if (pluginType == null) {
      pluginType = new ExecutionInfoLocationPluginType();
    }
    return pluginType;
  }

  @Override
  protected String extractDesc(ExecutionInfoLocationPlugin annotation) {
    return annotation.description();
  }

  @Override
  protected String extractID(ExecutionInfoLocationPlugin annotation) {
    return annotation.id();
  }

  @Override
  protected String extractName(ExecutionInfoLocationPlugin annotation) {
    return annotation.name();
  }
}
