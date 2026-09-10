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

package org.apache.hop.projects.xp;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;

@ExtensionPoint(
    id = "HopImportCreateProject",
    description = "Creates a new project for a project path specified in Hop Import",
    extensionPointId = "HopImportCreateProject")
public class HopImportCreateProjectIfNotExists implements IExtensionPoint<String> {

  static final String IMPORT_PROJECT_NAME = "Hop Import Project";

  @Override
  public void callExtensionPoint(ILogChannel iLogChannel, IVariables variables, String projectPath)
      throws HopException {
    createImportProject(variables, projectPath, true);
  }

  /**
   * Register a project at {@code projectPath} without applying it to {@code variables}.
   *
   * <p>Folder import must not overwrite the GUI {@code PROJECT_HOME} (issue #2865). The new project
   * is registered, not activated.
   *
   * @param persistHopConfig when false, skip writing hop-config.json (tests)
   * @return the registered project config, or {@code null} when {@code projectPath} is empty
   */
  static ProjectConfig createImportProject(
      IVariables variables, String projectPath, boolean persistHopConfig) throws HopException {
    if (StringUtil.isEmpty(projectPath)) {
      return null;
    }

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    String defaultProjectConfigFilename =
        Const.NVL(
            variables.resolve(config.getDefaultProjectConfigFile()),
            ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME);
    ProjectConfig projectConfig =
        new ProjectConfig(IMPORT_PROJECT_NAME, projectPath, defaultProjectConfigFilename);
    Project project = new Project();
    project.getDescribedVariables().clear();

    try (FileObject projectHome = HopVfs.getFileObject(projectPath)) {
      FileObject configFile = projectHome.resolveFile(defaultProjectConfigFilename);
      project.setConfigFilename(configFile.getName().getURI());
    } catch (Exception e) {
      throw new HopException(
          "Error resolving project configuration file for import folder '" + projectPath + "'", e);
    }

    config.addProjectConfig(projectConfig);
    if (persistHopConfig) {
      HopConfig.getInstance().saveToFile();
    }
    project.saveToFile();
    return projectConfig;
  }
}
