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

package org.apache.hop.projects.xp;

import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.config.IRestServicesProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.config.ProjectsOptionPlugin;

@ExtensionPoint(
    id = "HopRestServicesStartupProjectsConfig",
    extensionPointId = "HopRestServiceStart",
    description = "Configures a selected environment or project by name")
public class HopRestServicesStartupProjectsConfig
    implements IExtensionPoint<IRestServicesProvider> {

  public static final String OPTION_ENVIRONMENT_NAME = "environmentName";
  public static final String OPTION_PROJECT_NAME = "projectName";

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, IRestServicesProvider provider) throws HopException {

    Properties properties = provider.getProperties();
    IHasHopMetadataProvider hasHopMetadataProvider = provider.getHasHopMetadataProvider();
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    String environmentName = (String) properties.get(OPTION_ENVIRONMENT_NAME);
    if (StringUtils.isEmpty(environmentName)) {
      environmentName = config.getDefaultEnvironment();
    }
    String projectName = (String) properties.get(OPTION_PROJECT_NAME);
    if (StringUtils.isEmpty(projectName)) {
      projectName = config.getDefaultProject();
    }

    try {
      // Configure the project and environment if needed
      if ((StringUtils.isNotEmpty(projectName) || StringUtils.isNotEmpty(environmentName))
          && ProjectsOptionPlugin.configure(
              log, variables, hasHopMetadataProvider, projectName, environmentName)) {
        log.logBasic("Configured project or environment for the Hop REST services");
      }
    } catch (Exception e) {
      throw new HopException(
          "Error configuring project or environment for the Hop REST services", e);
    }
  }
}
