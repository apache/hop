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

package org.apache.hop.execution.sampler;

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

@PluginMainClassType(IExecutionDataSampler.class)
@PluginAnnotationType(ExecutionDataSamplerPlugin.class)
public class ExecutionDataSamplerPluginType extends BasePluginType<ExecutionDataSamplerPlugin> {

  private ExecutionDataSamplerPluginType() {
    super(
        ExecutionDataSamplerPlugin.class,
        "EXECUTION_DATA_SAMPLER_LOCATIONS",
        "Execution Data Sampler Locations");
  }

  private static ExecutionDataSamplerPluginType pluginType;

  public static ExecutionDataSamplerPluginType getInstance() {
    if (pluginType == null) {
      pluginType = new ExecutionDataSamplerPluginType();
    }
    return pluginType;
  }

  @Override
  protected String extractDesc(ExecutionDataSamplerPlugin annotation) {
    return annotation.description();
  }

  @Override
  protected String extractID(ExecutionDataSamplerPlugin annotation) {
    return annotation.id();
  }

  @Override
  protected String extractName(ExecutionDataSamplerPlugin annotation) {
    return annotation.name();
  }
}
