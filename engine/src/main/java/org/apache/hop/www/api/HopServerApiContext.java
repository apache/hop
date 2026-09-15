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
 */

package org.apache.hop.www.api;

import lombok.Getter;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.www.HopServerConfig;
import org.apache.hop.www.PipelineMap;
import org.apache.hop.www.WorkflowMap;

/**
 * The server state a JSON API resource needs: the exact analogue of what {@code
 * IHopServerPlugin.setup(pipelineMap, workflowMap)} hands a servlet.
 *
 * <p>The metadata provider and variables are read back from the {@link HopServerConfig} on every
 * call rather than captured once, so a resource always sees the same metadata the servlets do -
 * including anything a project or environment switch put there after startup.
 */
public class HopServerApiContext {

  @Getter private final PipelineMap pipelineMap;
  @Getter private final WorkflowMap workflowMap;
  @Getter private final ILogChannel log;

  public HopServerApiContext(PipelineMap pipelineMap, WorkflowMap workflowMap, ILogChannel log) {
    this.pipelineMap = pipelineMap;
    this.workflowMap = workflowMap;
    this.log = log;
  }

  /** The config is read back from the map each time, so replacing it is picked up here too. */
  private HopServerConfig getServerConfig() {
    return pipelineMap.getHopServerConfig();
  }

  public MultiMetadataProvider getMetadataProvider() {
    return getServerConfig().getMetadataProvider();
  }

  public IVariables getVariables() {
    return getServerConfig().getVariables();
  }
}
