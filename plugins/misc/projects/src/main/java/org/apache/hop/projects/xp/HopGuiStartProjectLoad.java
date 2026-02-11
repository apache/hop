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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.history.AuditEvent;
import org.apache.hop.history.AuditManager;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.gui.ProjectsGuiPlugin;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.ProjectsUtil;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;

@ExtensionPoint(
    id = "HopGuiStartProjectLoad",
    description = "Load the previously used project",
    extensionPointId = "HopGuiStart")
/** set the debug level right before the transform starts to run */
public class HopGuiStartProjectLoad implements IExtensionPoint {

  @Override
  public void callExtensionPoint(ILogChannel logChannelInterface, IVariables variables, Object o)
      throws HopException {

    HopGui hopGui = HopGui.getInstance();

    try {
      ProjectsConfig config = ProjectsConfigSingleton.getConfig();

      // Only move forward if the "projects" system is enabled.
      //
      if (ProjectsConfigSingleton.getConfig().isEnabled()) {
        logChannelInterface.logBasic("Projects enabled");

        // Build list of candidate projects to try: last used first, then default.
        // Defensive: try multiple recent projects so one failing does not block startup.
        //
        List<String> candidateNames = new ArrayList<>();
        List<AuditEvent> auditEvents =
            AuditManager.findEvents(
                ProjectsUtil.STRING_PROJECTS_AUDIT_GROUP,
                ProjectsUtil.STRING_PROJECT_AUDIT_TYPE,
                "open",
                15,
                true);
        for (AuditEvent event : auditEvents) {
          String name = event.getName();
          if (StringUtils.isNotEmpty(name)
              && config.findProjectConfig(name) != null
              && !candidateNames.contains(name)) {
            candidateNames.add(name);
          }
        }
        if (candidateNames.isEmpty() && config.getDefaultProject() != null) {
          if (config.findProjectConfig(config.getDefaultProject()) != null) {
            candidateNames.add(config.getDefaultProject());
          }
        }

        Exception firstFailure = null;
        String firstFailedProjectName = null;
        boolean enabled = false;

        for (String lastProjectName : candidateNames) {
          try {
            ProjectConfig projectConfig = config.findProjectConfig(lastProjectName);
            if (projectConfig == null) {
              continue;
            }
            Project project = projectConfig.loadProject(variables);

            logChannelInterface.logBasic("Enabling project : '" + lastProjectName + "'");

            LifecycleEnvironment lastEnvironment = null;

            List<AuditEvent> envEvents =
                AuditManager.findEvents(
                    ProjectsUtil.STRING_PROJECTS_AUDIT_GROUP,
                    ProjectsUtil.STRING_ENVIRONMENT_AUDIT_TYPE,
                    "open",
                    100,
                    true);

            for (AuditEvent envEvent : envEvents) {
              LifecycleEnvironment environment = config.findEnvironment(envEvent.getName());
              if (environment != null && lastProjectName.equals(environment.getProjectName())) {
                lastEnvironment = environment;
                break;
              }
            }

            ProjectsGuiPlugin.enableHopGuiProject(lastProjectName, project, lastEnvironment);
            hopGui.setOpeningLastFiles(false);
            enabled = true;
            break;
          } catch (Exception e) {
            if (firstFailure == null) {
              firstFailure = e;
              firstFailedProjectName = lastProjectName;
            }
            ProjectsGuiPlugin.MissingProjectInfo missing =
                ProjectsGuiPlugin.extractMissingProjectInfo(e);
            if (missing != null) {
              logChannelInterface.logBasic(
                  "Skipping project '"
                      + lastProjectName
                      + "': project folder no longer exists at "
                      + missing.path);
            }
            // Continue to next candidate project.
          }
        }

        if (!enabled && firstFailure != null) {
          ProjectsGuiPlugin.MissingProjectInfo missing =
              ProjectsGuiPlugin.extractMissingProjectInfo(firstFailure);
          if (missing != null) {
            String displayName =
                missing.projectName != null ? missing.projectName : firstFailedProjectName;
            MessageBox box = new MessageBox(hopGui.getActiveShell(), SWT.OK | SWT.ICON_WARNING);
            box.setMessage(
                "Project '"
                    + displayName
                    + "' could not be loaded because "
                    + missing.path
                    + " no longer exists.\n\nYou can update the path in the Project configuration.");
            box.setText("Project not available");
            box.open();
          } else {
            new ErrorDialog(
                hopGui.getActiveShell(),
                "Error",
                "Error initializing the Projects system",
                firstFailure);
          }
        }
      } else {
        logChannelInterface.logBasic("No last projects history found");
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getActiveShell(), "Error", "Error initializing the Projects system", e);
    }
  }
}
