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

package org.apache.hop.projects.gui;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.VariableValueDescription;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.history.AuditEvent;
import org.apache.hop.history.AuditManager;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.environment.LifecycleEnvironmentDialog;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.project.ProjectDialog;
import org.apache.hop.projects.util.ProjectsUtil;
import org.apache.hop.ui.core.bus.HopGuiEvents;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.vfs.HopVfsFileDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.dialog.PipelineExecutionConfigurationDialog;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engines.local.LocalWorkflowRunConfiguration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.MessageBox;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@GuiPlugin
public class ProjectsGuiPlugin {

  public static final String ID_TOOLBAR_PROJECT_LABEL = "toolbar-40000-project-label";
  public static final String ID_TOOLBAR_PROJECT_COMBO = "toolbar-40010-project-list";
  public static final String ID_TOOLBAR_PROJECT_EDIT = "toolbar-40020-project-edit";
  public static final String ID_TOOLBAR_PROJECT_ADD = "toolbar-40030-project-add";
  public static final String ID_TOOLBAR_PROJECT_DELETE = "toolbar-40040-project-delete";

  public static final String ID_TOOLBAR_ENVIRONMENT_LABEL = "toolbar-50000-environment-label";
  public static final String ID_TOOLBAR_ENVIRONMENT_COMBO = "toolbar-50010-environment-list";
  public static final String ID_TOOLBAR_ENVIRONMENT_EDIT = "toolbar-50020-environment-edit";
  public static final String ID_TOOLBAR_ENVIRONMENT_ADD = "toolbar-50030-environment-add";
  public static final String ID_TOOLBAR_ENVIRONMENT_DELETE = "toolbar-50040-environment-delete";

  public static final String NAVIGATE_TOOLBAR_PARENT_ID = "HopVfsFileDialog-NavigateToolbar";
  private static final String NAVIGATE_ITEM_ID_NAVIGATE_PROJECT_HOME =
      "0005-navigate-project-home"; // right next to Home button

  /** Automatically instantiated when the toolbar widgets etc need it */
  public ProjectsGuiPlugin() {}

  @GuiToolbarElement(
      root = HopGui.ID_MAIN_TOOLBAR,
      id = ID_TOOLBAR_PROJECT_LABEL,
      type = GuiToolbarElementType.LABEL,
      label = "i18n::HopGui.Toolbar.Project.Label",
      toolTip = "i18n::HopGui.Toolbar.Project.Tooltip",
      separator = true)
  public void editProject() {
    HopGui hopGui = HopGui.getInstance();
    Combo combo = getProjectsCombo();
    if (combo == null) {
      return;
    }
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    String projectName = combo.getText();

    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    if (projectConfig == null) {
      return;
    }

    try {
      Project project = projectConfig.loadProject(hopGui.getVariables());
      ProjectDialog projectDialog =
          new ProjectDialog(hopGui.getShell(), project, projectConfig, hopGui.getVariables());
      if (projectDialog.open() != null) {
        config.addProjectConfig(projectConfig);
        project.saveToFile();

        refreshProjectsList();
        selectProjectInList(projectName);

        if (projectDialog.isNeedingProjectRefresh()) {
          if (askAboutProjectRefresh(hopGui)){
            // Try to stick to the same environment if we have one selected...
            //
            LifecycleEnvironment environment = null;
            Combo environmentsCombo = getEnvironmentsCombo();
            if (environmentsCombo != null) {
              environment = config.findEnvironment(environmentsCombo.getText());
            }
            enableHopGuiProject(projectConfig.getProjectName(), project, environment);
          }
        }
      }
    } catch (Exception e) {
      new ErrorDialog(hopGui.getShell(), "Error", "Error editing project '" + projectName, e);
    }
  }

  private boolean askAboutProjectRefresh(HopGui hopGui) {
    MessageBox box = new MessageBox(hopGui.getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO);
    box.setText("Reload project?");
    box.setMessage("To apply all changes you made a reload of this project is required. Do you want to do this now?");
    int answer = box.open();
    return (answer & SWT.YES) != 0;
  }

  @GuiToolbarElement(
      root = HopGui.ID_MAIN_TOOLBAR,
      id = ID_TOOLBAR_PROJECT_COMBO,
      type = GuiToolbarElementType.COMBO,
      comboValuesMethod = "getProjectsList",
      extraWidth = 200,
      toolTip = "i18n::HopGui.Toolbar.ProjectsList.Tooltip")
  public void selectProject() {
    HopGui hopGui = HopGui.getInstance();
    Combo combo = getProjectsCombo();
    if (combo == null) {
      return;
    }
    String projectName = combo.getText();
    if (StringUtils.isEmpty(projectName)) {
      return;
    }
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    ProjectConfig projectConfig = config.findProjectConfig(projectName);

    LifecycleEnvironment environment = null;
    List<LifecycleEnvironment> environments = config.findEnvironmentsOfProject(projectName);
    if (!environments.isEmpty()) {
      environment = environments.get(0);
    }

    try {
      Project project = projectConfig.loadProject(hopGui.getVariables());
      if (project != null) {
        enableHopGuiProject(projectName, project, environment);
      } else {
        hopGui.getLog().logError("Unable to find project '" + projectName + "'");
      }
    } catch (Exception e) {
      new ErrorDialog(hopGui.getShell(), "Error", "Error changing project to '" + projectName, e);
    }
  }

  @GuiToolbarElement(
      root = HopGui.ID_MAIN_TOOLBAR,
      id = ID_TOOLBAR_PROJECT_EDIT,
      toolTip = "i18n::HopGui.Toolbar.Project.Edit.Tooltip",
      image = "project-edit.svg")
  public void editSelectedProject() {
    editProject();
  }

  @GuiToolbarElement(
      root = HopGui.ID_MAIN_TOOLBAR,
      id = ID_TOOLBAR_PROJECT_ADD,
      toolTip = "i18n::HopGui.Toolbar.Project.Add.Tooltip",
      image = "project-add.svg")
  public void addNewProject() {
    HopGui hopGui = HopGui.getInstance();
    try {
      ProjectsConfig config = ProjectsConfigSingleton.getConfig();

      String standardProjectsFolder = hopGui.getVariables().resolve( config.getStandardProjectsFolder() );
      ProjectConfig projectConfig =
          new ProjectConfig("", standardProjectsFolder, ProjectConfig.DEFAULT_PROJECT_CONFIG_FILENAME);

      Project project = new Project();
      project.setParentProjectName(config.getStandardParentProject());
      ProjectDialog projectDialog =
          new ProjectDialog(hopGui.getShell(), project, projectConfig, hopGui.getVariables());
      String projectName = projectDialog.open();
      if (projectName != null) {
        config.addProjectConfig(projectConfig);
        HopConfig.getInstance().saveToFile();

        // Save the project-config.json file as well in the project itself
        //
        project.saveToFile();

        refreshProjectsList();
        selectProjectInList(projectName);

        enableHopGuiProject(projectName, project, null);

        // Now see if these project contains any local run configurations.
        // If not we can add those automatically
        //
        // First pipeline
        //
        IHopMetadataSerializer<PipelineRunConfiguration> prcSerializer =
            hopGui.getMetadataProvider().getSerializer(PipelineRunConfiguration.class);
        List<PipelineRunConfiguration> pipelineRunConfigs = prcSerializer.loadAll();
        boolean localFound = false;
        for (PipelineRunConfiguration pipelineRunConfig : pipelineRunConfigs) {
          if (pipelineRunConfig.getEngineRunConfiguration()
              instanceof LocalPipelineRunConfiguration) {
            localFound = true;
          }
        }
        if (!localFound) {
          PipelineExecutionConfigurationDialog.createLocalPipelineConfiguration(
              hopGui.getShell(), prcSerializer);
        }

        // Then the local workflow runconfig
        //
        IHopMetadataSerializer<WorkflowRunConfiguration> wrcSerializer =
            hopGui.getMetadataProvider().getSerializer(WorkflowRunConfiguration.class);
        localFound = false;
        List<WorkflowRunConfiguration> workflowRunConfigs = wrcSerializer.loadAll();
        for (WorkflowRunConfiguration workflowRunConfig : workflowRunConfigs) {
          if (workflowRunConfig.getEngineRunConfiguration()
              instanceof LocalWorkflowRunConfiguration) {
            localFound = true;
          }
        }
        if (!localFound) {
          MessageBox box =
              new MessageBox(HopGui.getInstance().getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
          box.setText("Create local workflow run configuration?");
          box.setMessage(
              "Do you want to have a local workflow run configuration for this project?");
          int anwser = box.open();
          if ((anwser & SWT.YES) != 0) {
            LocalWorkflowRunConfiguration localWorkflowRunConfiguration =
                new LocalWorkflowRunConfiguration();
            localWorkflowRunConfiguration.setEnginePluginId("Local");
            WorkflowRunConfiguration local =
                new WorkflowRunConfiguration(
                    "local",
                    "Runs your workflows locally with the standard local Hop workflow engine",
                    localWorkflowRunConfiguration);
            wrcSerializer.save(local);
          }
        }

        // Ask to put the project in a lifecycle environment
        //
        MessageBox box =
            new MessageBox(HopGui.getInstance().getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
        box.setText("Create project lifecycle environment?");
        box.setMessage(
            "If this project is part of a lifecyle then perhaps you want to add it to a lifecycle environment?"
                + Const.CR
                + "With it you can manage the specific settings like hostnames and paths for the environment");
        int anwser = box.open();
        if ((anwser & SWT.YES) != 0) {

          addNewEnvironment();
        }
      }

    } catch (Exception e) {
      new ErrorDialog(hopGui.getShell(), "Error", "Error adding project", e);
    }
  }

  @GuiToolbarElement(
      root = HopGui.ID_MAIN_TOOLBAR,
      id = ID_TOOLBAR_PROJECT_DELETE,
      toolTip = "i18n::HopGui.Toolbar.Project.Delete.Tooltip",
      image = "project-delete.svg",
      separator = true)
  public void deleteSelectedProject() {
    Combo combo = getProjectsCombo();
    if (combo == null) {
      return;
    }
    String projectName = combo.getText();
    if (StringUtils.isEmpty(projectName)) {
      return;
    }

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    if (projectConfig == null) {
      return;
    }
    String projectHome = projectConfig.getProjectHome();
    String configFilename = projectConfig.getConfigFilename();

    MessageBox box =
        new MessageBox(HopGui.getInstance().getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    box.setText("Delete?");
    box.setMessage(
        "Do you want to delete project '"
            + projectName
            + "' from the Hop configuration?"
            + Const.CR
            + "Please note that folder '"
            + projectHome
            + "' or the project configuration file "
            + configFilename
            + " in it are NOT removed or otherwise altered in any way.");
    int anwser = box.open();
    if ((anwser & SWT.YES) != 0) {
      try {
        config.removeProjectConfig(projectName);
        ProjectsConfigSingleton.saveConfig();

        refreshProjectsList();
        if (StringUtils.isEmpty(config.getDefaultProject())) {
          selectProjectInList(null);
        } else {
          selectProjectInList(config.getDefaultProject());
        }
      } catch (Exception e) {
        new ErrorDialog(
            HopGui.getInstance().getShell(),
            "Error",
            "Error removing project '" + projectName + "'",
            e);
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Environment toolbar items...
  //
  //////////////////////////////////////////////////////////////////////////////////

  @GuiToolbarElement(
      root = HopGui.ID_MAIN_TOOLBAR,
      id = ID_TOOLBAR_ENVIRONMENT_LABEL,
      type = GuiToolbarElementType.LABEL,
      label = "i18n::HopGui.Toolbar.Environment.Label",
      toolTip = "i18n::HopGui.Toolbar.Environment.Tooltip",
      separator = true)
  public void editEnvironment() {
    HopGui hopGui = HopGui.getInstance();
    Combo combo = getEnvironmentsCombo();
    if (combo == null) {
      return;
    }
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    String environmentName = combo.getText();
    if (StringUtils.isEmpty(environmentName)) {
      return;
    }
    LifecycleEnvironment environment = config.findEnvironment(environmentName);
    if (environment == null) {
      return;
    }

    try {
      LifecycleEnvironmentDialog dialog =
          new LifecycleEnvironmentDialog(hopGui.getShell(), environment, hopGui.getVariables());
      if (dialog.open() != null) {
        config.addEnvironment(environment);
        ProjectsConfigSingleton.saveConfig();

        refreshEnvironmentsList();

        selectEnvironmentInList(environmentName);
      }

      // A refresh of the project and environment is likely needed
      // Ask the user if this is needed now...
      //
      if (dialog.isNeedingEnvironmentRefresh()) {
        if (askAboutProjectRefresh(hopGui)) {
          // Refresh the loaded environment
          //
          selectEnvironment();
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(), "Error", "Error editing environment '" + environmentName, e);
    }
  }

  @GuiToolbarElement(
      root = HopGui.ID_MAIN_TOOLBAR,
      id = ID_TOOLBAR_ENVIRONMENT_COMBO,
      type = GuiToolbarElementType.COMBO,
      comboValuesMethod = "getEnvironmentsList",
      extraWidth = 200,
      toolTip = "Select the active environment")
  public void selectEnvironment() {
    HopGui hopGui = HopGui.getInstance();
    Combo envCombo = getEnvironmentsCombo();
    if (envCombo == null) {
      return;
    }

    String environmentName = envCombo.getText();
    if (StringUtils.isEmpty(environmentName)) {
      return;
    }
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    LifecycleEnvironment environment = config.findEnvironment(environmentName);
    if (environment == null) {
      return;
    }
    if (StringUtils.isEmpty(environment.getProjectName())) {
      return;
    }
    ProjectConfig projectConfig = config.findProjectConfig(environment.getProjectName());
    if (projectConfig == null) {
      return;
    }

    try {
      Project project = projectConfig.loadProject(hopGui.getVariables());
      if (project != null) {
        enableHopGuiProject(projectConfig.getProjectName(), project, environment);
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(), "Error", "Error changing project to '" + environmentName, e);
    }
  }

  @GuiToolbarElement(
      root = HopGui.ID_MAIN_TOOLBAR,
      id = ID_TOOLBAR_ENVIRONMENT_EDIT,
      toolTip = "i18n::HopGui.Toolbar.Environment.Edit.Tooltip",
      image = "environment-edit.svg")
  public void editSelectedEnvironment() {
    editEnvironment();
  }

  @GuiToolbarElement(
      root = HopGui.ID_MAIN_TOOLBAR,
      id = ID_TOOLBAR_ENVIRONMENT_ADD,
      toolTip = "i18n::HopGui.Toolbar.Environment.Add.Tooltip",
      image = "environment-add.svg")
  public void addNewEnvironment() {
    HopGui hopGui = HopGui.getInstance();
    try {
      ProjectsConfig config = ProjectsConfigSingleton.getConfig();
      String projectName = getProjectsCombo().getText(); // The default is the active project

      LifecycleEnvironment environment =
          new LifecycleEnvironment(null, "", projectName, new ArrayList<>());
      LifecycleEnvironmentDialog dialog =
          new LifecycleEnvironmentDialog(hopGui.getShell(), environment, hopGui.getVariables());
      String environmentName = dialog.open();
      if (environmentName != null) {
        config.addEnvironment(environment);
        ProjectsConfigSingleton.saveConfig();

        refreshEnvironmentsList();
        selectEnvironmentInList(environmentName);

        ProjectConfig projectConfig = config.findProjectConfig(projectName);
        if (projectConfig != null) {
          Project project = projectConfig.loadProject(hopGui.getVariables());
          enableHopGuiProject(projectName, project, environment);
        }
      }
    } catch (Exception e) {
      new ErrorDialog(hopGui.getShell(), "Error", "Error adding lifecycle environment", e);
    }
  }

  @GuiToolbarElement(
      root = HopGui.ID_MAIN_TOOLBAR,
      id = ID_TOOLBAR_ENVIRONMENT_DELETE,
      toolTip = "i18n::HopGui.Toolbar.Environment.Delete.Tooltip",
      image = "environment-delete.svg",
      separator = true)
  public void deleteSelectedEnvironment() {
    Combo combo = getEnvironmentsCombo();
    if (combo == null) {
      return;
    }
    String environmentName = combo.getText();
    if (StringUtils.isEmpty(environmentName)) {
      return;
    }

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    LifecycleEnvironment environment = config.findEnvironment(environmentName);
    if (environment == null) {
      return;
    }

    MessageBox box =
        new MessageBox(HopGui.getInstance().getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    box.setText("Delete?");
    box.setMessage(
        "Do you want to delete environment '"
            + environmentName
            + "' from the Hop configuration?"
            + Const.CR
            + "Please note that project '"
            + environment.getProjectName()
            + "' or any file or folder are NOT removed or otherwise altered in any way.");
    int anwser = box.open();
    if ((anwser & SWT.YES) != 0) {
      try {
        config.removeEnvironment(environmentName);
        ProjectsConfigSingleton.saveConfig();

        refreshEnvironmentsList();
        selectEnvironmentInList(null);
      } catch (Exception e) {
        new ErrorDialog(
            HopGui.getInstance().getShell(),
            "Error",
            "Error removing environment " + environmentName + "'",
            e);
      }
    }
  }

  public static void enableHopGuiProject(
      String projectName, Project project, LifecycleEnvironment environment) throws HopException {
    try {
      HopGui hopGui = HopGui.getInstance();

      // Before we switch the namespace in HopGui, save the state of the perspectives
      //
      hopGui.auditDelegate.writeLastOpenFiles();

      // Now we can close all files if they're all saved (or changes are ignored)
      //
      if (!hopGui.fileDelegate.saveGuardAllFiles()) {
        // Abort the project change
        return;
      }

      // Close 'm all
      //
      hopGui.fileDelegate.closeAllFiles();

      // This is called only in HopGui so we want to start with a new set of variables
      // It avoids variables from one project showing up in another
      //
      IVariables variables = Variables.getADefaultVariableSpace();

      // See if there's an environment associated with the current project
      // In that case, apply the variables in those files
      //
      List<String> configurationFiles = new ArrayList<>();
      if (environment != null) {
        configurationFiles.addAll(environment.getConfigurationFiles());
      }

      // Set the variables and give HopGui the new metadata provider(s) for the project.
      //
      String environmentName = environment == null ? null : environment.getName();
      ProjectsUtil.enableProject(
          hopGui.getLog(),
          projectName,
          project,
          variables,
          configurationFiles,
          environmentName,
          hopGui);

      // HopGui now has a new metadata provider set.
      // Only now we can get the variables from the defined run configs.
      // Set them with default values just to make them show up.
      //
      IHopMetadataSerializer<PipelineRunConfiguration> runConfigSerializer =
          hopGui.getMetadataProvider().getSerializer(PipelineRunConfiguration.class);
      for (PipelineRunConfiguration runConfig : runConfigSerializer.loadAll()) {
        for (VariableValueDescription variableValueDescription :
            runConfig.getConfigurationVariables()) {
          variables.setVariable(
              variableValueDescription.getName(), "");
        }
      }

      // We need to change the currently set variables in the newly loaded files
      //
      hopGui.setVariables(variables);

      // Re-open last open files for the namespace
      //
      hopGui.auditDelegate.openLastFiles();

      // Clear last used, fill it with something useful.
      //
      IVariables hopGuiVariables = Variables.getADefaultVariableSpace();
      hopGui.setVariables(hopGuiVariables);
      for (String variable : variables.getVariableNames()) {
        String value = variables.getVariable(variable);
        if (!variable.startsWith(Const.INTERNAL_VARIABLE_PREFIX)) {
          hopGuiVariables.setVariable(variable, value);
        }
      }

      // Refresh the currently active file
      //
      hopGui.getActivePerspective().getActiveFileTypeHandler().updateGui();

      // Update the toolbar combos
      //
      ProjectsGuiPlugin.selectProjectInList(projectName);
      ProjectsGuiPlugin.selectEnvironmentInList(environment == null ? null : environment.getName());

      // Also add this as an event so we know what the project usage history is
      //
      AuditEvent prjUsedEvent =
          new AuditEvent(
              ProjectsUtil.STRING_PROJECTS_AUDIT_GROUP,
              ProjectsUtil.STRING_PROJECT_AUDIT_TYPE,
              projectName,
              "open",
              new Date());
      AuditManager.getActive().storeEvent(prjUsedEvent);

      // Now use that event to refresh the list...
      //
      refreshProjectsList();
      ProjectsGuiPlugin.selectProjectInList(projectName);

      if (environment != null) {
        // Also add this as an event so we know what the project usage history is
        //
        AuditEvent envUsedEvent =
            new AuditEvent(
                ProjectsUtil.STRING_PROJECTS_AUDIT_GROUP,
                ProjectsUtil.STRING_ENVIRONMENT_AUDIT_TYPE,
                environment.getName(),
                "open",
                new Date());
        AuditManager.getActive().storeEvent(envUsedEvent);
      }

      // Send out an event notifying that a new project is activated...
      // The metadata has changed so fire those events as well
      //
      hopGui.getEventsHandler().fire(projectName, HopGuiEvents.ProjectActivated.name());
      hopGui.getEventsHandler().fire(projectName, HopGuiEvents.MetadataChanged.name());

      // Inform the outside world that we're enabled an other project
      //
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL,
          hopGuiVariables,
          HopExtensionPoint.HopGuiProjectAfterEnabled.name(),
          project);

    } catch (Exception e) {
      throw new HopException("Error enabling project '" + projectName + "' in HopGui", e);
    }
  }

  private static Combo getProjectsCombo() {
    Control control =
        HopGui.getInstance()
            .getMainToolbarWidgets()
            .getWidgetsMap()
            .get(ProjectsGuiPlugin.ID_TOOLBAR_PROJECT_COMBO);
    if ((control != null) && (control instanceof Combo)) {
      Combo combo = (Combo) control;
      return combo;
    }
    return null;
  }

  private static Combo getEnvironmentsCombo() {
    Control control =
        HopGui.getInstance()
            .getMainToolbarWidgets()
            .getWidgetsMap()
            .get(ProjectsGuiPlugin.ID_TOOLBAR_ENVIRONMENT_COMBO);
    if ((control != null) && (control instanceof Combo)) {
      Combo combo = (Combo) control;
      return combo;
    }
    return null;
  }

  /**
   * Called by the Combo in the toolbar
   *
   * @param log
   * @param metadataProvider
   * @return
   * @throws Exception
   */
  public List<String> getProjectsList(ILogChannel log, IHopMetadataProvider metadataProvider)
      throws Exception {
    List<String> names = ProjectsConfigSingleton.getConfig().listProjectConfigNames();
    Map<String, Date> lastUsedMap = new HashMap<>();
    names.stream()
        .forEach(
            name ->
                lastUsedMap.put(name, new GregorianCalendar(1900, Calendar.JANUARY, 1).getTime()));

    // Get the list of events from the Audit Manager...
    //
    List<AuditEvent> projectOpenEvents = AuditManager.findEvents(
      ProjectsUtil.STRING_PROJECTS_AUDIT_GROUP,
      ProjectsUtil.STRING_PROJECT_AUDIT_TYPE,
      "open",
      100,
      true );

    for (AuditEvent projectOpenEvent : projectOpenEvents) {
      lastUsedMap.put(projectOpenEvent.getName(), projectOpenEvent.getDate());
    }

    // Reverse sort by last used date of a project...
    //
    Collections.sort( names, new Comparator<String>() {
      @Override public int compare( String name1, String name2 ) {
        int cmp = -lastUsedMap.get( name1 ).compareTo( lastUsedMap.get( name2 ) );
        if (cmp==0) {
          cmp = name1.compareToIgnoreCase( name2 );
        }
        return cmp;
      }
    } );

    return names;
  }

  public static void refreshProjectsList() {
    HopGui.getInstance().getMainToolbarWidgets().refreshComboItemList(ID_TOOLBAR_PROJECT_COMBO);
  }

  public static void selectProjectInList(String name) {
    GuiToolbarWidgets toolbarWidgets = HopGui.getInstance().getMainToolbarWidgets();

    toolbarWidgets.selectComboItem(ID_TOOLBAR_PROJECT_COMBO, name);
    Combo combo = getProjectsCombo();
    if (combo != null) {
      ProjectsConfig config = ProjectsConfigSingleton.getConfig();
      ProjectConfig projectConfig = config.findProjectConfig(name);
      if (projectConfig != null) {
        String projectHome = projectConfig.getProjectHome();
        if (StringUtils.isNotEmpty(projectHome)) {
          combo.setToolTipText(
              "Project "
                  + name
                  + " in '"
                  + projectHome
                  + "' configured in '"
                  + projectConfig.getConfigFilename()
                  + "'");
        }
      }
    }
  }

  /**
   * Called by the environments Combo in the toolbar
   *
   * @param log
   * @param metadataProvider
   * @return
   * @throws Exception
   */
  public List<String> getEnvironmentsList(ILogChannel log, IHopMetadataProvider metadataProvider)
      throws Exception {
    List<String> names = ProjectsConfigSingleton.getConfig().listEnvironmentNames();
    return names;
  }

  public static void refreshEnvironmentsList() {
    HopGui.getInstance().getMainToolbarWidgets().refreshComboItemList(ID_TOOLBAR_ENVIRONMENT_COMBO);
  }

  public static void selectEnvironmentInList(String name) {
    GuiToolbarWidgets toolbarWidgets = HopGui.getInstance().getMainToolbarWidgets();

    toolbarWidgets.selectComboItem(ID_TOOLBAR_ENVIRONMENT_COMBO, name);
    Combo combo = getEnvironmentsCombo();
    if (combo != null) {
      ProjectsConfig config = ProjectsConfigSingleton.getConfig();
      LifecycleEnvironment environment = config.findEnvironment(name);
      if (environment != null) {
        combo.setToolTipText(
            "Environment "
                + name
                + " for project '"
                + environment.getProjectName()
                + "' has purpose: "
                + environment.getPurpose());
      }
    }
  }

  // Add an e button to the file dialog browser toolbar
  //
  @GuiToolbarElement(
      root = NAVIGATE_TOOLBAR_PARENT_ID,
      id = NAVIGATE_ITEM_ID_NAVIGATE_PROJECT_HOME,
      toolTip = "i18n::FileDialog.Browse.Project.Home",
      image = "project.svg")
  public void fileDialogBrowserProjectHome() {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    ProjectConfig projectConfig = config.findProjectConfig(HopNamespace.getNamespace());
    if (projectConfig == null) {
      return;
    }
    String homeFolder = projectConfig.getProjectHome();
    if (StringUtils.isNotEmpty(homeFolder)) {
      // Navigate to the home folder
      //
      HopVfsFileDialog instance = HopVfsFileDialog.getInstance();
      if (instance != null) {
        instance.navigateTo(homeFolder, true);
      }
    }
  }
}
