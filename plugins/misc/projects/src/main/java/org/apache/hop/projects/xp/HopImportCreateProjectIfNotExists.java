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

import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.hopgui.HopGui;

import java.util.Collections;


@ExtensionPoint(
        id = "HopImportCreateProject",
        description = "Creates a new project for a project path specified in Hop Import",
        extensionPointId = "HopImportCreateProject"
)
public class HopImportCreateProjectIfNotExists implements IExtensionPoint<String> {

    @Override
    public void callExtensionPoint(ILogChannel iLogChannel, IVariables variables, String projectPath) throws HopException {

        String projectName = "Hop Import Project";
        String envName = "Hop Import Environment";

        HopGui hopGui = HopGui.getInstance();
        ProjectsConfig config = ProjectsConfigSingleton.getConfig();

        // create new project
        if(!StringUtil.isEmpty(projectPath)){
            ProjectConfig projectConfig = new ProjectConfig(projectName, projectPath, ProjectConfig.DEFAULT_PROJECT_CONFIG_FILENAME);
            Project project = new Project();
            project.getDescribedVariables().clear();
            project.modifyVariables(variables, projectConfig, Collections.emptyList(), null);
            project.setConfigFilename(projectPath + System.getProperty("file.separator") + "project-config.json");
            config.addProjectConfig(projectConfig);
            HopConfig.getInstance().saveToFile();
            project.saveToFile();
        }
    }
}
