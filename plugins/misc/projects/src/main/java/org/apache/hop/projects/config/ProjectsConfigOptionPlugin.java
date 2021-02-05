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

import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.ui.core.dialog.EnterOptionsDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import picocli.CommandLine;

@ConfigPlugin(
    id = "ProjectsConfigOptionPlugin",
    description = "Configuration options for the global projects plugin")
@GuiPlugin(
    description = "Projects" // Tab label in options dialog
    )
public class ProjectsConfigOptionPlugin
    implements IConfigOptions, IGuiPluginCompositeWidgetsListener {

  private static final String WIDGET_ID_ENABLE_PROJECTS = "10000-enable-projects-plugin";
  private static final String WIDGET_ID_PROJECT_MANDATORY = "10010-project-mandatory";
  private static final String WIDGET_ID_ENVIRONMENT_MANDATORY = "10020-environment-mandatory";
  private static final String WIDGET_ID_DEFAULT_PROJECT = "10030-default-project";
  private static final String WIDGET_ID_DEFAULT_ENVIRONMENT = "10040-default-environment";
  private static final String WIDGET_ID_STANDARD_PARENT_PROJECT = "10050-standard-parent-project";
  private static final String WIDGET_ID_STANDARD_PROJECTS_FOLDER = "10060-standard-projects-folder";

  @GuiWidgetElement(
      id = WIDGET_ID_ENABLE_PROJECTS,
      parentId = EnterOptionsDialog.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "Enable the projects plugin")
  @CommandLine.Option(
      names = {"-pn", "--projects-enabled"},
      description = "Enable or disable the projects plugin")
  private Boolean projectsEnabled;

  @GuiWidgetElement(
      id = WIDGET_ID_PROJECT_MANDATORY,
      parentId = EnterOptionsDialog.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "Use of a project is mandatory")
  @CommandLine.Option(
      names = {"-py", "--project-mandatory"},
      description = "Make it mandatory to reference a project")
  private Boolean projectMandatory;

  @GuiWidgetElement(
    id = WIDGET_ID_ENVIRONMENT_MANDATORY,
    parentId = EnterOptionsDialog.GUI_WIDGETS_PARENT_ID,
    type = GuiElementType.CHECKBOX,
    label = "Use of an environment is mandatory")
  @CommandLine.Option(
    names = {"-ey", "--environment-mandatory"},
    description = "Make it mandatory to reference an environment")
  private Boolean environmentMandatory;

  @GuiWidgetElement(
      id = WIDGET_ID_DEFAULT_PROJECT,
      parentId = EnterOptionsDialog.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "The default project to use when none is specified")
  @CommandLine.Option(
      names = {"-dp", "--default-project"},
      description = "The name of the default project to use when none is specified")
  private String defaultProject;

  @GuiWidgetElement(
    id = WIDGET_ID_DEFAULT_ENVIRONMENT,
    parentId = EnterOptionsDialog.GUI_WIDGETS_PARENT_ID,
    type = GuiElementType.TEXT,
    variables = true,
    label = "The default environment to use when none is specified")
  @CommandLine.Option(
    names = {"-de", "--default-environment"},
    description = "The name of the default environment to use when none is specified")
  private String defaultEnvironment;

  @GuiWidgetElement(
      id = WIDGET_ID_STANDARD_PARENT_PROJECT,
      parentId = EnterOptionsDialog.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "The parent project to propose when creating projects")
  @CommandLine.Option(
      names = {"-sp", "--standard-parent-project"},
      description = "The name of the standard project to use as a parent when creating new projects")
  private String standardParentProject;

  @GuiWidgetElement(
    id = WIDGET_ID_STANDARD_PROJECTS_FOLDER,
    parentId = EnterOptionsDialog.GUI_WIDGETS_PARENT_ID,
    type = GuiElementType.TEXT,
    variables = true,
    label = "GUI: The standard projects folder proposed when creating projects")
  @CommandLine.Option(
    names = {"-sj", "--standard-projects-folder"},
    description = "GUI: The standard projects folder proposed when creating projects")
  private String standardProjectsFolder;

  /**
   * Gets instance
   *
   * @return value of instance
   */
  public static ProjectsConfigOptionPlugin getInstance() {
    ProjectsConfigOptionPlugin instance = new ProjectsConfigOptionPlugin();

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    instance.projectsEnabled = config.isEnabled();
    instance.defaultProject = config.getDefaultProject();
    instance.defaultEnvironment = config.getDefaultEnvironment();
    instance.projectMandatory = config.isProjectMandatory();
    instance.environmentMandatory = config.isEnvironmentMandatory();
    instance.standardParentProject = config.getStandardParentProject();
    instance.standardProjectsFolder = config.getStandardProjectsFolder();
    return instance;
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    try {
      boolean changed = false;
      if (projectsEnabled != null) {
        config.setEnabled(projectsEnabled);
        if (projectsEnabled) {
          log.logBasic("Enabled the projects system");
        } else {
          log.logBasic("Disabled the projects system");
        }
        changed = true;
      }
      if (projectMandatory != null) {
        config.setProjectMandatory(projectMandatory);
        if (projectMandatory) {
          log.logBasic("Using a project is set to be mandatory");
        } else {
          log.logBasic("Using a project is set to be optional");
        }
        changed = true;
      }
      if (environmentMandatory != null) {
        config.setEnvironmentMandatory(environmentMandatory);
        if (environmentMandatory) {
          log.logBasic("Using an environment is set to be mandatory");
        } else {
          log.logBasic("Using an environment is set to be optional");
        }
        changed = true;
      }
      if (defaultProject != null) {
        config.setDefaultProject(defaultProject);
        log.logBasic("The default project is set to '" + defaultProject + "'");
        changed = true;
      }
      if (defaultEnvironment != null) {
        config.setDefaultEnvironment(defaultEnvironment);
        log.logBasic("The default environment is set to '" + defaultEnvironment + "'");
        changed = true;
      }
      if (standardParentProject != null) {
        config.setStandardParentProject(standardParentProject);
        log.logBasic(
            "The standard project to inherit from when creating a project is set to '"
                + standardParentProject
                + "'");
        changed = true;
      }
      if (standardProjectsFolder != null) {
        config.setStandardProjectsFolder( standardProjectsFolder );
        log.logBasic(
          "The standard projects folder to browse to in the GUI is set to '"
            + standardProjectsFolder
            + "'");
        changed = true;
      }
      // Save to file if anything changed
      //
      if (changed) {
        ProjectsConfigSingleton.saveConfig();
      }
      return changed;
    } catch (Exception e) {
      throw new HopException("Error handling projects plugin configuration options", e);
    }
  }

  @Override
  public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {}

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {}

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {}

  @Override
  public void persistContents(GuiCompositeWidgets compositeWidgets) {
    for (String widgetId : compositeWidgets.getWidgetsMap().keySet()) {
      Control control = compositeWidgets.getWidgetsMap().get(widgetId);
      switch (widgetId) {
        case WIDGET_ID_ENABLE_PROJECTS:
          projectsEnabled = ((Button) control).getSelection();
          ProjectsConfigSingleton.getConfig().setEnabled(projectsEnabled);
          break;
        case WIDGET_ID_PROJECT_MANDATORY:
          projectMandatory = ((Button) control).getSelection();
          ProjectsConfigSingleton.getConfig().setProjectMandatory(projectMandatory);
          break;
        case WIDGET_ID_ENVIRONMENT_MANDATORY:
          environmentMandatory = ((Button) control).getSelection();
          ProjectsConfigSingleton.getConfig().setEnvironmentMandatory(environmentMandatory);
          break;
        case WIDGET_ID_DEFAULT_PROJECT:
          defaultProject = ((TextVar) control).getText();
          ProjectsConfigSingleton.getConfig().setDefaultProject(defaultProject);
          break;
        case WIDGET_ID_DEFAULT_ENVIRONMENT:
          defaultEnvironment = ((TextVar) control).getText();
          ProjectsConfigSingleton.getConfig().setDefaultEnvironment(defaultEnvironment);
          break;
        case WIDGET_ID_STANDARD_PARENT_PROJECT:
          standardParentProject = ((TextVar) control).getText();
          ProjectsConfigSingleton.getConfig().setStandardParentProject(standardParentProject);
          break;
        case WIDGET_ID_STANDARD_PROJECTS_FOLDER:
          standardProjectsFolder = ((TextVar) control).getText();
          ProjectsConfigSingleton.getConfig().setStandardProjectsFolder(standardProjectsFolder);
          break;
      }
    }
    // Save the project...
    //
    try {
      ProjectsConfigSingleton.saveConfig();
    } catch (Exception e) {
      new ErrorDialog(HopGui.getInstance().getShell(), "Error", "Error saving option", e);
    }
  }

  /**
   * Gets projectsEnabled
   *
   * @return value of projectsEnabled
   */
  public Boolean getProjectsEnabled() {
    return projectsEnabled;
  }

  /** @param projectsEnabled The projectsEnabled to set */
  public void setProjectsEnabled(Boolean projectsEnabled) {
    this.projectsEnabled = projectsEnabled;
  }

  /**
   * Gets projectMandatory
   *
   * @return value of projectMandatory
   */
  public Boolean getProjectMandatory() {
    return projectMandatory;
  }

  /** @param projectMandatory The projectMandatory to set */
  public void setProjectMandatory(Boolean projectMandatory) {
    this.projectMandatory = projectMandatory;
  }

  /**
   * Gets defaultProject
   *
   * @return value of defaultProject
   */
  public String getDefaultProject() {
    return defaultProject;
  }

  /** @param defaultProject The defaultProject to set */
  public void setDefaultProject(String defaultProject) {
    this.defaultProject = defaultProject;
  }

  /**
   * Gets standardParentProject
   *
   * @return value of standardParentProject
   */
  public String getStandardParentProject() {
    return standardParentProject;
  }

  /** @param standardParentProject The standardParentProject to set */
  public void setStandardParentProject(String standardParentProject) {
    this.standardParentProject = standardParentProject;
  }

  /**
   * Gets environmentMandatory
   *
   * @return value of environmentMandatory
   */
  public Boolean getEnvironmentMandatory() {
    return environmentMandatory;
  }

  /**
   * @param environmentMandatory The environmentMandatory to set
   */
  public void setEnvironmentMandatory( Boolean environmentMandatory ) {
    this.environmentMandatory = environmentMandatory;
  }

  /**
   * Gets defaultEnvironment
   *
   * @return value of defaultEnvironment
   */
  public String getDefaultEnvironment() {
    return defaultEnvironment;
  }

  /**
   * @param defaultEnvironment The defaultEnvironment to set
   */
  public void setDefaultEnvironment( String defaultEnvironment ) {
    this.defaultEnvironment = defaultEnvironment;
  }

  /**
   * Gets standardProjectsFolder
   *
   * @return value of standardProjectsFolder
   */
  public String getStandardProjectsFolder() {
    return standardProjectsFolder;
  }

  /**
   * @param standardProjectsFolder The standardProjectsFolder to set
   */
  public void setStandardProjectsFolder( String standardProjectsFolder ) {
    this.standardProjectsFolder = standardProjectsFolder;
  }
}
