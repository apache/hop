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

package org.apache.hop.projects.config;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;

@ConfigPlugin(
    id = "ProjectsGuiOptionPlugin",
    description = "Project and Environment configuration options for hop-gui",
    category = ConfigPlugin.CATEGORY_GUI)
public class ProjectsGuiOptionPlugin extends ProjectsOptionPlugin implements IConfigOptions {

  @Getter private static volatile String requestedProjectName;
  @Getter private static volatile String requestedEnvironmentName;

  /** Clears remembered GUI startup project and environment (for tests). */
  public static void clearRequested() {
    requestedProjectName = null;
    requestedEnvironmentName = null;
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    // Do not pass the GUI command as metadata holder. configure() would enable the default
    // project and write an "open" audit event before last-used restoration in HopGuiStart.
    boolean result = super.handleOption(log, null, variables);
    // Picocli consumes -j/-e on the gui subcommand, so remember them for HopGuiStartProjectLoad.
    if (StringUtils.isNotEmpty(getProjectName())) {
      requestedProjectName = getProjectName();
    }
    if (StringUtils.isNotEmpty(getEnvironmentName())) {
      requestedEnvironmentName = getEnvironmentName();
    }
    return result;
  }
}
