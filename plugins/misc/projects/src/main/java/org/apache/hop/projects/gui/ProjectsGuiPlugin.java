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

package org.apache.hop.projects.gui;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRunnableWithProgress;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.history.AuditEvent;
import org.apache.hop.history.AuditManager;
import org.apache.hop.i18n.BaseMessages;
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
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.bus.HopGuiEvents;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.core.gui.GuiMenuWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.vfs.HopVfsFileDialog;
import org.apache.hop.ui.core.widget.FileTree;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.dialog.PipelineExecutionConfigurationDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engines.local.LocalWorkflowRunConfiguration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolItem;

@GuiPlugin
public class ProjectsGuiPlugin {

  public static final Class<?> PKG = ProjectsGuiPlugin.class; // i18n

  public static final String ID_TOOLBAR_ITEM_PROJECT = "toolbar-item-10000-project";
  public static final String ID_CONTEXT_MENU_PROJECT = "context-menu-project";
  public static final String ID_CONTEXT_MENU_PROJECT_ADD = "context-menu-project-40010-add";
  public static final String ID_CONTEXT_MENU_PROJECT_EDIT = "context-menu-project-40020-edit";
  public static final String ID_CONTEXT_MENU_PROJECT_DELETE = "context-menu-project-40030-delete";

  public static final String ID_TOOLBAR_ITEM_ENVIRONMENT = "toolbar-item-20000-environment";
  public static final String ID_CONTEXT_MENU_ENVIRONMENT = "context-menu-environment";
  public static final String ID_CONTEXT_MENU_ENVIRONMENT_ADD = "context-menu-environment-50010-add";
  public static final String ID_CONTEXT_MENU_ENVIRONMENT_EDIT =
      "context-menu-environment-50020-edit";
  public static final String ID_CONTEXT_MENU_ENVIRONMENT_DUPLICATE =
      "context-menu-environment-50030-duplicate";

  public static final String ID_CONTEXT_MENU_ENVIRONMENT_DELETE =
      "context-menu-environment-50040-delete";

  public static final String ID_MAIN_MENU_PROJECT_EXPORT = "10055-menu-file-export-to-svg";

  public static final String NAVIGATE_TOOLBAR_PARENT_ID = "HopVfsFileDialog-NavigateToolbar";
  private static final String NAVIGATE_ITEM_ID_NAVIGATE_PROJECT_HOME =
      "0005-navigate-project-home"; // right next to Home button

  FileTree tree;

  /** Automatically instantiated when the toolbar widgets need it */
  public ProjectsGuiPlugin() {
    // Do nothing
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

      // Close's all

      hopGui.fileDelegate.closeAllFiles();

      // This is called only in Hop GUI so we want to start with a new set of variables
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
        for (DescribedVariable variableValueDescription : runConfig.getConfigurationVariables()) {
          variables.setVariable(variableValueDescription.getName(), "");
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

      // Update the toolbar items
      //
      updateProjectToolItem(projectName);
      updateEnvironmentToolItem(environmentName);

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

      if (environment != null) {
        // Also add this as an event so we know what the project usage history is
        //
        AuditEvent envUsedEvent =
            new AuditEvent(
                ProjectsUtil.STRING_PROJECTS_AUDIT_GROUP,
                ProjectsUtil.STRING_ENVIRONMENT_AUDIT_TYPE,
                environmentName,
                "open",
                new Date());
        AuditManager.getActive().storeEvent(envUsedEvent);
      }

      // Send out an event notifying that a new project is activated...
      // The metadata has changed so fire those events as well
      //
      hopGui.getEventsHandler().fire(projectName, HopGuiEvents.ProjectActivated.name());
      hopGui.getEventsHandler().fire(projectName, HopGuiEvents.MetadataChanged.name());

      // Inform the outside world that we're enabled another project
      //
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL,
          hopGuiVariables,
          HopExtensionPoint.HopGuiProjectAfterEnabled.name(),
          project);

      // Reset VFS filesystem to load additional configurations
      HopVfs.reset();
    } catch (Exception e) {
      throw new HopException("Error enabling project '" + projectName + "' in HopGui", e);
    }
  }

  private static ToolItem getProjectToolItem() {
    return HopGui.getInstance().getStatusToolbarWidgets().findToolItem(ID_TOOLBAR_ITEM_PROJECT);
  }

  private static ToolItem getEnvironmentToolItem() {
    return HopGui.getInstance().getStatusToolbarWidgets().findToolItem(ID_TOOLBAR_ITEM_ENVIRONMENT);
  }

  private static void updateProjectToolItem(String projectName) {
    ToolItem item = getProjectToolItem();
    if (item != null && !item.isDisposed()) {
      ProjectsConfig config = ProjectsConfigSingleton.getConfig();
      ProjectConfig projectConfig = config.findProjectConfig(projectName);
      if (projectConfig != null) {
        String projectHome = projectConfig.getProjectHome();
        if (StringUtils.isNotEmpty(projectHome)) {
          item.setText(projectName);
          item.setToolTipText(
              BaseMessages.getString(
                  PKG,
                  "HopGui.Toolbar.Project.Tooltip",
                  projectName,
                  projectHome,
                  projectConfig.getConfigFilename()));
        }
      }
    }
  }

  private static void updateEnvironmentToolItem(String environmentName) {
    ToolItem item = getEnvironmentToolItem();
    if (item != null && !item.isDisposed()) {
      if (Utils.isEmpty(environmentName)) {
        item.setText("");
        item.setToolTipText(
            BaseMessages.getString(PKG, "HopGui.Toolbar.Environment.Select.Tooltip"));
        return;
      }

      ProjectsConfig config = ProjectsConfigSingleton.getConfig();
      LifecycleEnvironment environment = config.findEnvironment(environmentName);
      if (environment != null) {
        item.setText(environmentName);
        item.setToolTipText(
            BaseMessages.getString(
                PKG,
                "HopGui.Toolbar.Environment.Tooltip",
                environmentName,
                environment.getProjectName(),
                environment.getPurpose()));
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Environment toolbar items...
  //
  //////////////////////////////////////////////////////////////////////////////////

  /**
   * used by the welcome dialog to switch to the samples project.
   *
   * @param projectName The name of the project to switch to.
   * @throws HopException when the project cannot be found
   */
  public static void enableProject(String projectName) throws HopException {

    HopGui hopGui = HopGui.getInstance();

    IVariables variables = hopGui.getVariables();
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    if (projectConfig == null) {
      throw new HopException("The project with name '" + projectName + "' could not be found");
    }
    Project project = projectConfig.loadProject(variables);

    enableHopGuiProject(projectName, project, null);
  }

  @GuiMenuElement(
      root = ID_CONTEXT_MENU_PROJECT,
      parentId = ID_CONTEXT_MENU_PROJECT,
      id = ID_CONTEXT_MENU_PROJECT_EDIT,
      label = "i18n::HopGui.Toolbar.Project.Edit.Label",
      toolTip = "i18n::HopGui.Toolbar.Project.Edit.Tooltip",
      image = "ui/images/edit.svg")
  public void editProject() {
    HopGui hopGui = HopGui.getInstance();
    String projectName = getProjectToolItem().getText();

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    if (projectConfig == null) {
      return;
    }

    try {
      Project project = projectConfig.loadProject(hopGui.getVariables());
      String projectFolder = projectConfig.getProjectHome();

      ProjectDialog projectDialog =
          new ProjectDialog(
              hopGui.getActiveShell(), project, projectConfig, hopGui.getVariables(), true);
      if (projectDialog.open() != null) {
        config.addProjectConfig(projectConfig);

        // Project's home changed. Update reference in hop-config.json
        if (!projectFolder.equals(projectConfig.getProjectHome())) {
          // Refresh project reference in hop-config.json
          HopConfig.getInstance().saveToFile();
        }

        if (!projectName.equals(projectConfig.getProjectName())) {
          // Project got renamed
          projectName = projectConfig.getProjectName();
          // Refresh project reference in hop-config.json
          HopConfig.getInstance().saveToFile();
        }

        project.saveToFile();
        updateProjectToolItem(projectName);

        if (projectDialog.isNeedingProjectRefresh()) {
          if (askAboutProjectRefresh(hopGui)) {
            // Try to stick to the same environment if we have one selected...
            //
            LifecycleEnvironment environment = null;
            ToolItem environmentItem = getEnvironmentToolItem();
            if (environmentItem != null) {
              environment = config.findEnvironment(environmentItem.getText());
            }
            enableHopGuiProject(projectConfig.getProjectName(), project, environment);
          }

          // refresh automatically when the projects change
          //
          hopGui.getEventsHandler().fire(projectName, HopGuiEvents.ProjectUpdated.name());
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getActiveShell(),
          BaseMessages.getString(PKG, "ProjectGuiPlugin.EditProject.Error.Dialog.Header"),
          BaseMessages.getString(
              PKG, "ProjectGuiPlugin.EditProject.Error.Dialog.Message", projectName),
          e);
    }
  }

  private boolean askAboutProjectRefresh(HopGui hopGui) {
    MessageBox box = new MessageBox(hopGui.getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO);
    box.setText(BaseMessages.getString(PKG, "ProjectGuiPlugin.ReloadProject.Dialog.Header"));
    box.setMessage(BaseMessages.getString(PKG, "ProjectGuiPlugin.ReloadProject.Dialog.Message"));
    int answer = box.open();
    return (answer & SWT.YES) != 0;
  }

  @GuiToolbarElement(
      root = HopGui.ID_STATUS_TOOLBAR,
      id = ID_TOOLBAR_ITEM_PROJECT,
      type = GuiToolbarElementType.BUTTON,
      image = "project.svg",
      toolTip = "i18n::HopGui.Toolbar.Project.Tooltip")
  public void showProjectContextMenu() {
    ToolItem item = getProjectToolItem();
    if (item != null) {
      Rectangle rect = item.getBounds();
      Point location = item.getParent().toDisplay(new Point(rect.x, rect.y + rect.height));
      Menu menu = createProjectContextMenu();
      menu.setLocation(location);
      menu.setVisible(true);
    }
  }

  private Menu createProjectContextMenu() {
    Shell shell = HopGui.getInstance().getActiveShell();
    Menu menu = new Menu(shell, SWT.POP_UP);

    // Create context menu...
    //
    GuiMenuWidgets menuWidgets = new GuiMenuWidgets();
    menuWidgets.registerGuiPluginObject(this);
    menuWidgets.createMenuWidgets(ID_CONTEXT_MENU_PROJECT, shell, menu);

    // Enable menus if a project is active.
    //
    boolean enabled = !"".equals(getProjectToolItem().getText());
    menuWidgets.enableMenuItem(ID_CONTEXT_MENU_PROJECT_EDIT, enabled);
    menuWidgets.enableMenuItem(ID_CONTEXT_MENU_PROJECT_DELETE, enabled);

    new MenuItem(menu, SWT.SEPARATOR);

    String currentProjectName = HopNamespace.getNamespace();
    List<String> names = ProjectsConfigSingleton.getConfig().listProjectConfigNames();
    int count = 0;
    for (String name : names) {
      MenuItem item = new MenuItem(menu, SWT.CHECK);
      item.setText(name);
      item.setSelection(currentProjectName.equals(name));
      item.addListener(SWT.Selection, e -> selectProject(name));
      //    if (++count == 10) break;
    }
    // TODO: display only the last 10 used projects
    //    if (count == 5) {
    //      new MenuItem(menu, SWT.SEPARATOR);
    //      MenuItem item = new MenuItem(menu, SWT.PUSH);
    //      item.setText("More...");
    //      item.addListener(SWT.Selection, e -> selectProject());
    //    }

    return menu;
  }

  private Menu createEnvironmentContextMenu() {
    Shell shell = HopGui.getInstance().getActiveShell();
    Menu menu = new Menu(shell, SWT.POP_UP);

    // Create context menu...
    //
    GuiMenuWidgets menuWidgets = new GuiMenuWidgets();
    menuWidgets.registerGuiPluginObject(this);
    menuWidgets.createMenuWidgets(ID_CONTEXT_MENU_ENVIRONMENT, shell, menu);

    // Enable menus if an environment is active.
    //
    boolean enabled = !"".equals(getEnvironmentToolItem().getText());
    menuWidgets.enableMenuItem(ID_CONTEXT_MENU_ENVIRONMENT_EDIT, enabled);
    menuWidgets.enableMenuItem(ID_CONTEXT_MENU_ENVIRONMENT_DUPLICATE, enabled);
    menuWidgets.enableMenuItem(ID_CONTEXT_MENU_ENVIRONMENT_DELETE, enabled);

    new MenuItem(menu, SWT.SEPARATOR);

    String currentProjectName = getProjectToolItem().getText();
    String currentEnvironmentName = getEnvironmentToolItem().getText();

    // List of first 10 projects
    //
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    List<String> names = config.listEnvironmentNames();
    int count = 0;
    for (String name : names) {

      // Exclude environment linked to another project
      LifecycleEnvironment environment = config.findEnvironment(name);
      if (Utils.isEmpty(environment.getProjectName())
          || currentProjectName.equals(environment.getProjectName())) {
        MenuItem item = new MenuItem(menu, SWT.CHECK);
        item.setText(name);
        item.setSelection(currentEnvironmentName.equals(name));
        item.addListener(SWT.Selection, e -> selectEnvironment(name));
        // if (++count == 10) break;
      }
    }

    // TODO: display only the last 10 used environments
    // If more projects, open a dialog to select it
    //    if (count == 10) {
    //      new MenuItem(menu, SWT.SEPARATOR);
    //      MenuItem item = new MenuItem(menu, SWT.PUSH);
    //      item.setText("More...");
    //      item.addListener(SWT.Selection, e -> selectEnvironment());
    //    }

    return menu;
  }

  @GuiToolbarElement(
      root = HopGui.ID_STATUS_TOOLBAR,
      id = ID_TOOLBAR_ITEM_ENVIRONMENT,
      type = GuiToolbarElementType.BUTTON,
      image = "environment.svg",
      toolTip = "i18n::HopGui.Toolbar.Environment.Tooltip")
  public void showEnvironmentContextMenu() {
    ToolItem item = getEnvironmentToolItem();
    if (item != null) {
      Rectangle rect = item.getBounds();
      Point location = item.getParent().toDisplay(new Point(rect.x, rect.y + rect.height));
      Menu menu = createEnvironmentContextMenu();
      menu.setLocation(location);
      menu.setVisible(true);
    }
  }

  public void selectProject() {
    List<String> projectNames = ProjectsConfigSingleton.getConfig().listProjectConfigNames();
    EnterSelectionDialog dialog =
        new EnterSelectionDialog(
            HopGui.getInstance().getActiveShell(),
            projectNames.toArray(new String[0]),
            BaseMessages.getString(PKG, "ProjectGuiPlugin.Dialog.AvailableProjects.Title"),
            BaseMessages.getString(PKG, "ProjectGuiPlugin.Dialog.AvailableProjects.Message"));
    dialog.setCurrentValue(HopNamespace.getNamespace());

    String name = dialog.open();
    if (name != null) {
      selectProject(name);
    }
  }

  public void selectProject(String projectName) {
    HopGui hopGui = HopGui.getInstance();

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    if (config.isEnvironmentsForActiveProject() && StringUtils.isEmpty(projectName)) {
      // list all environments and select the first one if we don't have a project selected
      List<String> allEnvironments = config.listEnvironmentNames();
      updateEnvironmentToolItem(allEnvironments.get(0));
      return;
    }

    ProjectConfig projectConfig = config.findProjectConfig(projectName);

    // What is the last used environment?
    //
    LifecycleEnvironment environment = null;

    // See in the audit logs if there was a recent environment opened.
    // We limit ourselves to 1 event, the last one.
    //
    try {
      List<AuditEvent> environmentAuditEvents =
          AuditManager.findEvents(
              ProjectsUtil.STRING_PROJECTS_AUDIT_GROUP,
              ProjectsUtil.STRING_ENVIRONMENT_AUDIT_TYPE,
              "open",
              100,
              true);
      for (AuditEvent auditEvent : environmentAuditEvents) {
        String environmentName = auditEvent.getName();
        if (StringUtils.isNotEmpty(environmentName)) {
          environment = config.findEnvironment(environmentName);
          if (environment != null) {
            // See that the project belongs to the environment
            //
            if (projectName.equals(environment.getProjectName())) {
              // We found what we've been looking for
              break;
            } else {
              // The project doesn't to the last selected environment
              // Since we selected the project it is the driver of the selection.
              // Keep looking.
              //
              environment = null;
            }
          }
        }
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error reading the last used environment from the audit logs", e);
    }

    // If there is no recent usage, select the first environment.
    //
    if (environment == null) {
      List<LifecycleEnvironment> environments = config.findEnvironmentsOfProject(projectName);
      if (!environments.isEmpty()) {
        environment = environments.get(0);
      }
    }

    try {
      Project project = projectConfig.loadProject(hopGui.getVariables());
      if (project != null) {
        enableHopGuiProject(projectName, project, environment);
      } else {
        hopGui.getLog().logError("Unable to find project '" + projectName + "'");
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getActiveShell(),
          BaseMessages.getString(PKG, "ProjectGuiPlugin.ChangeProject.Error.Dialog.Header"),
          BaseMessages.getString(
              PKG, "ProjectGuiPlugin.ChangeProject.Error.Dialog.Message", projectName),
          e);
    }
  }

  @GuiMenuElement(
      root = ID_CONTEXT_MENU_PROJECT,
      parentId = ID_CONTEXT_MENU_PROJECT,
      id = ID_CONTEXT_MENU_PROJECT_ADD,
      label = "i18n::HopGui.Toolbar.Project.Add.Label",
      toolTip = "i18n::HopGui.Toolbar.Project.Add.Tooltip",
      image = "ui/images/add.svg")
  public void addNewProject() {
    HopGui hopGui = HopGui.getInstance();
    IVariables variables = hopGui.getVariables();

    try {
      ProjectsConfig config = ProjectsConfigSingleton.getConfig();
      String standardProjectsFolder = variables.resolve(config.getStandardProjectsFolder());
      String defaultProjectConfigFilename = variables.resolve(config.getDefaultProjectConfigFile());
      ProjectConfig projectConfig =
          new ProjectConfig("", standardProjectsFolder, defaultProjectConfigFilename);

      Project project = new Project();
      project.setParentProjectName(config.getStandardParentProject());

      ProjectDialog projectDialog =
          new ProjectDialog(hopGui.getActiveShell(), project, projectConfig, variables, false);
      String projectName = projectDialog.open();
      if (projectName != null) {
        config.addProjectConfig(projectConfig);
        HopConfig.getInstance().saveToFile();

        // Save the project-config.json file as well in the project itself
        //
        FileObject configFile =
            HopVfs.getFileObject(projectConfig.getActualProjectConfigFilename(variables));
        if (!configFile.exists()) {
          // Create the empty configuration file if it does not exists
          project.saveToFile();
        } else {
          // If projects exists load configuration
          MessageBox box = new MessageBox(hopGui.getActiveShell(), SWT.ICON_QUESTION | SWT.OK);
          box.setText(BaseMessages.getString(PKG, "ProjectGuiPlugin.ProjectExists.Dialog.Header"));
          box.setMessage(
              BaseMessages.getString(PKG, "ProjectGuiPlugin.ProjectExists.Dialog.Message"));
          box.open();

          project.readFromFile();
        }

        updateProjectToolItem(projectName);

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
            break;
          }
        }
        if (!localFound) {
          PipelineExecutionConfigurationDialog.createLocalPipelineConfiguration(
              hopGui.getShell(), prcSerializer);
        }

        // Then the local workflow run configuration
        //
        IHopMetadataSerializer<WorkflowRunConfiguration> wrcSerializer =
            hopGui.getMetadataProvider().getSerializer(WorkflowRunConfiguration.class);
        localFound = false;
        List<WorkflowRunConfiguration> workflowRunConfigs = wrcSerializer.loadAll();
        for (WorkflowRunConfiguration workflowRunConfig : workflowRunConfigs) {
          if (workflowRunConfig.getEngineRunConfiguration()
              instanceof LocalWorkflowRunConfiguration) {
            localFound = true;
            break;
          }
        }
        if (!localFound) {
          MessageBox box =
              new MessageBox(HopGui.getInstance().getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
          box.setText(
              BaseMessages.getString(PKG, "ProjectGuiPlugin.LocalWFRunConfig.Dialog.Header"));
          box.setMessage(
              BaseMessages.getString(PKG, "ProjectGuiPlugin.LocalWFRunConfig.Dialog.Message"));
          int answer = box.open();
          if ((answer & SWT.YES) != 0) {
            LocalWorkflowRunConfiguration localWorkflowRunConfiguration =
                new LocalWorkflowRunConfiguration();
            localWorkflowRunConfiguration.setEnginePluginId("Local");
            WorkflowRunConfiguration local =
                new WorkflowRunConfiguration(
                    "local",
                    BaseMessages.getString(
                        PKG, "ProjectGuiPlugin.LocalWFRunConfigDescription.Text"),
                    null,
                    localWorkflowRunConfiguration,
                    true);
            wrcSerializer.save(local);
          }
        }

        // Ask to put the project in a lifecycle environment
        //
        MessageBox box =
            new MessageBox(HopGui.getInstance().getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
        box.setText(BaseMessages.getString(PKG, "ProjectGuiPlugin.Lifecycle.Dialog.Header"));
        box.setMessage(
            BaseMessages.getString(PKG, "ProjectGuiPlugin.Lifecycle.Dialog.Message1")
                + Const.CR
                + BaseMessages.getString(PKG, "ProjectGuiPlugin.Lifecycle.Dialog.Message2"));
        int answer = box.open();
        if ((answer & SWT.YES) != 0) {

          addNewEnvironment();
        }
      }

    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getActiveShell(),
          BaseMessages.getString(PKG, "ProjectGuiPlugin.AddProject.Error.Dialog.Header"),
          BaseMessages.getString(PKG, "ProjectGuiPlugin.AddProject.Error.Dialog.Message"),
          e);
    }
  }

  @GuiMenuElement(
      root = ID_CONTEXT_MENU_PROJECT,
      parentId = ID_CONTEXT_MENU_PROJECT,
      id = ID_CONTEXT_MENU_PROJECT_DELETE,
      label = "i18n::HopGui.Toolbar.Project.Delete.Label",
      toolTip = "i18n::HopGui.Toolbar.Project.Delete.Tooltip",
      image = "ui/images/delete.svg")
  public void deleteProject() {
    String projectName = getProjectToolItem().getText();
    if (StringUtils.isEmpty(projectName)) {
      return;
    }
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    ProjectConfig currentProjectConfig = config.findProjectConfig(projectName);
    if (currentProjectConfig == null) {
      return;
    }

    String projectHome = currentProjectConfig.getProjectHome();
    String configFilename = currentProjectConfig.getConfigFilename();

    try {

      List<String> refs = ProjectsUtil.getParentProjectReferences(projectName);

      if (refs.isEmpty()) {
        performProjectDeletion(projectName, config, projectHome, configFilename);
      } else {
        String prjReferences = String.join(",", refs);
        MessageBox box =
            new MessageBox(HopGui.getInstance().getShell(), SWT.OK | SWT.ICON_INFORMATION);
        box.setText(
            BaseMessages.getString(
                PKG, "ProjectGuiPlugin.DeleteProject.ProjectReferencedAsParent.Header"));
        box.setMessage(
            BaseMessages.getString(
                    PKG, "ProjectGuiPlugin.DeleteProject.ProjectReferencedAsParent.Message1")
                + Const.CR
                + prjReferences
                + Const.CR
                + BaseMessages.getString(
                    PKG, "ProjectGuiPlugin.DeleteProject.ProjectReferencedAsParent.Message2"));
        box.open();
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "ProjectGuiPlugin.DeleteProject.Dialog.Header"),
          BaseMessages.getString(
              PKG, "ProjectGuiPlugin.DeleteProject.Error.Dialog.Message", projectName),
          e);
    }
  }

  private void performProjectDeletion(
      String projectName, ProjectsConfig config, String projectHome, String configFilename) {
    MessageBox box =
        new MessageBox(HopGui.getInstance().getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    box.setText(BaseMessages.getString(PKG, "ProjectGuiPlugin.DeleteProject.Dialog.Header"));
    box.setMessage(
        BaseMessages.getString(PKG, "ProjectGuiPlugin.DeleteProject.Dialog.Message1", projectName)
            + Const.CR
            + BaseMessages.getString(
                PKG,
                "ProjectGuiPlugin.DeleteProject.Dialog.Message2",
                projectHome,
                configFilename));
    int answer = box.open();
    if ((answer & SWT.YES) != 0) {
      try {
        config.removeProjectConfig(projectName);
        ProjectsConfigSingleton.saveConfig();

        if (StringUtils.isEmpty(config.getDefaultProject())) {
          updateProjectToolItem(null);
        } else {
          updateProjectToolItem(config.getDefaultProject());
        }
      } catch (Exception e) {
        new ErrorDialog(
            HopGui.getInstance().getShell(),
            BaseMessages.getString(PKG, "ProjectGuiPlugin.DeleteProject.Error.Dialog.Header"),
            BaseMessages.getString(
                PKG, "ProjectGuiPlugin.DeleteProject.Error.Dialog.Message", projectName),
            e);
      }
    }
  }

  @GuiMenuElement(
      root = ID_CONTEXT_MENU_ENVIRONMENT,
      parentId = ID_CONTEXT_MENU_ENVIRONMENT,
      id = ID_CONTEXT_MENU_ENVIRONMENT_EDIT,
      label = "i18n::HopGui.Toolbar.Environment.Edit.Label",
      toolTip = "i18n::HopGui.Toolbar.Environment.Edit.Tooltip",
      image = "ui/images/edit.svg")
  public void editEnvironment() {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    LifecycleEnvironment environment = config.findEnvironment(getEnvironmentToolItem().getText());
    if (environment != null) {
      this.editEnvironment(environment);
    }
  }

  public void editEnvironment(LifecycleEnvironment environment) {
    HopGui hopGui = HopGui.getInstance();
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    try {
      LifecycleEnvironmentDialog dialog =
          new LifecycleEnvironmentDialog(
              hopGui.getActiveShell(), environment, hopGui.getVariables());
      if (dialog.open() != null) {
        config.addEnvironment(environment);
        ProjectsConfigSingleton.saveConfig();

        updateEnvironmentToolItem(environment.getName());
      }

      // A refresh of the project and environment is likely needed
      // Ask the user if this is needed now...
      //
      if (dialog.isNeedingEnvironmentRefresh() && askAboutProjectRefresh(hopGui)) {
        // Refresh the loaded environment
        //
        selectEnvironment(environment.getName());
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getActiveShell(),
          BaseMessages.getString(PKG, "ProjectGuiPlugin.EditEnvironment.Error.Dialog.Header"),
          BaseMessages.getString(
              PKG, "ProjectGuiPlugin.EditEnvironment.Error.Dialog.Message", environment.getName()),
          e);
    }
  }

  public void selectEnvironment(String environmentName) {
    if (StringUtils.isEmpty(environmentName)) {
      return;
    }
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    LifecycleEnvironment environment = config.findEnvironment(environmentName);
    if (environment == null) {
      return;
    }
    String projectName = environment.getProjectName();
    if (StringUtils.isEmpty(projectName)) {
      projectName = getProjectToolItem().getText();
    }
    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    if (projectConfig == null) {
      return;
    }

    try {
      Project project = projectConfig.loadProject(HopGui.getInstance().getVariables());
      if (project != null) {
        enableHopGuiProject(projectConfig.getProjectName(), project, environment);
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getActiveShell(),
          BaseMessages.getString(PKG, "ProjectGuiPlugin.ChangeEnvironment.Error.Dialog.Header"),
          BaseMessages.getString(
              PKG, "ProjectGuiPlugin.ChangeEnvironment.Error.Dialog.Message", environmentName),
          e);
    }
  }

  @GuiMenuElement(
      root = ID_CONTEXT_MENU_ENVIRONMENT,
      parentId = ID_CONTEXT_MENU_ENVIRONMENT,
      id = ID_CONTEXT_MENU_ENVIRONMENT_ADD,
      label = "i18n::HopGui.Toolbar.Environment.Add.Label",
      toolTip = "i18n::HopGui.Toolbar.Environment.Add.Tooltip",
      image = "ui/images/add.svg")
  public void addNewEnvironment() {
    HopGui hopGui = HopGui.getInstance();
    try {
      ProjectsConfig config = ProjectsConfigSingleton.getConfig();
      String projectName = getProjectToolItem().getText(); // The default is the active project

      LifecycleEnvironment environment =
          new LifecycleEnvironment(null, "", projectName, new ArrayList<>());
      LifecycleEnvironmentDialog dialog =
          new LifecycleEnvironmentDialog(
              hopGui.getActiveShell(), environment, hopGui.getVariables());
      String environmentName = dialog.open();
      if (environmentName != null) {
        config.addEnvironment(environment);
        ProjectsConfigSingleton.saveConfig();

        updateEnvironmentToolItem(environmentName);

        ProjectConfig projectConfig = config.findProjectConfig(projectName);
        if (projectConfig != null) {
          Project project = projectConfig.loadProject(hopGui.getVariables());
          enableHopGuiProject(projectName, project, environment);
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getActiveShell(),
          BaseMessages.getString(PKG, "ProjectGuiPlugin.AddEnvironment.Error.Dialog.Header"),
          BaseMessages.getString(PKG, "ProjectGuiPlugin.AddEnvironment.Error.Dialog.Message"),
          e);
    }
  }

  @GuiMenuElement(
      root = ID_CONTEXT_MENU_ENVIRONMENT,
      parentId = ID_CONTEXT_MENU_ENVIRONMENT,
      id = ID_CONTEXT_MENU_ENVIRONMENT_DUPLICATE,
      label = "i18n::HopGui.Toolbar.Environment.Duplicate.Label",
      toolTip = "i18n::HopGui.Toolbar.Environment.Duplicate.Tooltip",
      image = "ui/images/duplicate.svg")
  public void duplicateEnvironment() {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    LifecycleEnvironment environment = config.findEnvironment(getEnvironmentToolItem().getText());
    if (environment == null) {
      return;
    }

    LifecycleEnvironment newEnvironment = new LifecycleEnvironment(environment);
    newEnvironment.setName(null);
    this.editEnvironment(newEnvironment);
  }

  @GuiMenuElement(
      root = ID_CONTEXT_MENU_ENVIRONMENT,
      parentId = ID_CONTEXT_MENU_ENVIRONMENT,
      id = ID_CONTEXT_MENU_ENVIRONMENT_DELETE,
      label = "i18n::HopGui.Toolbar.Environment.Delete.Label",
      toolTip = "i18n::HopGui.Toolbar.Environment.Delete.Tooltip",
      image = "ui/images/delete.svg")
  public void deleteEnvironment() {
    HopGui hopGui = HopGui.getInstance();
    String environmentName = getEnvironmentToolItem().getText();
    if (StringUtils.isEmpty(environmentName)) {
      return;
    }

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    LifecycleEnvironment environment = config.findEnvironment(environmentName);
    if (environment == null) {
      return;
    }

    MessageBox box = new MessageBox(hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    box.setText(BaseMessages.getString(PKG, "ProjectGuiPlugin.DeleteEnvironment.Dialog.Header"));
    box.setMessage(
        BaseMessages.getString(
                PKG, "ProjectGuiPlugin.DeleteEnvironment.Dialog.Message1", environmentName)
            + Const.CR
            + BaseMessages.getString(
                PKG,
                "ProjectGuiPlugin.DeleteEnvironment.Dialog.Message2",
                environment.getProjectName()));
    int answer = box.open();
    if ((answer & SWT.YES) != 0) {
      try {
        String projectName = getProjectToolItem().getText(); // The default is the active project
        ProjectConfig projectConfig = config.findProjectConfig(projectName);
        Project project = projectConfig.loadProject(hopGui.getVariables());

        config.removeEnvironment(environmentName);
        ProjectsConfigSingleton.saveConfig();

        updateEnvironmentToolItem(null);

        // Reload the project to clear current variables
        enableHopGuiProject(projectName, project, null);
      } catch (Exception e) {
        new ErrorDialog(
            hopGui.getShell(),
            BaseMessages.getString(PKG, "ProjectGuiPlugin.DeleteEnvironment.Error.Dialog.Header"),
            BaseMessages.getString(
                PKG, "ProjectGuiPlugin.DeleteEnvironment.Error.Dialog.Message", environmentName),
            e);
      }
    }
  }

  /**
   * Called by the menu in the toolbar
   *
   * @param log the current logchannel
   * @param metadataProvider
   * @return a List of project names
   * @throws Exception
   */
  public List<String> getProjectsList(ILogChannel log, IHopMetadataProvider metadataProvider)
      throws Exception {
    List<String> names = ProjectsConfigSingleton.getConfig().listProjectConfigNames();
    Map<String, Date> lastUsedMap = new HashMap<>();
    names.forEach(
        name -> lastUsedMap.put(name, new GregorianCalendar(1900, Calendar.JANUARY, 1).getTime()));

    // Get the list of events from the Audit Manager...
    //
    List<AuditEvent> projectOpenEvents =
        AuditManager.findEvents(
            ProjectsUtil.STRING_PROJECTS_AUDIT_GROUP,
            ProjectsUtil.STRING_PROJECT_AUDIT_TYPE,
            "open",
            100,
            true);

    for (AuditEvent projectOpenEvent : projectOpenEvents) {
      lastUsedMap.put(projectOpenEvent.getName(), projectOpenEvent.getDate());
    }

    // Reverse sort by last used date of a project...
    //
    names.sort(
        (name1, name2) -> {
          int cmp = -lastUsedMap.get(name1).compareTo(lastUsedMap.get(name2));
          if (cmp == 0) {
            cmp = name1.compareToIgnoreCase(name2);
          }
          return cmp;
        });

    return names;
  }

  /**
   * Called by the environment menu in the toolbar
   *
   * @param log
   * @param metadataProvider
   */
  public List<String> getEnvironmentsList(ILogChannel log, IHopMetadataProvider metadataProvider) {
    return ProjectsConfigSingleton.getConfig().listEnvironmentNames();
  }

  // Add a "Navigate to project home" button to the file dialog browser toolbar
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

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_PROJECT_EXPORT,
      label = "i18n::HopGui.FileMenu.Project.Export.Label",
      image = "export.svg",
      parentId = HopGui.ID_MAIN_MENU_FILE,
      separator = false)
  public void menuProjectExport() {
    HopGui hopGui = HopGui.getInstance();
    Shell shell = hopGui.getShell();
    IVariables variables = hopGui.getVariables();

    // Resolve variables in filepath
    String zipFilename =
        variables.resolve(
            BaseDialog.presentFileDialog(
                true,
                shell,
                new String[] {"*.zip", "*.*"},
                new String[] {"Zip files (*.zip)", "All Files (*.*)"},
                true));
    if (zipFilename == null) {
      return;
    }
    ToolItem projectItem = getProjectToolItem();
    if (projectItem == null) {
      return;
    }
    String projectName = projectItem.getText();
    if (StringUtils.isEmpty(projectName)) {
      return;
    }
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    String projectHome = projectConfig.getProjectHome();

    AtomicBoolean includeVariables = new AtomicBoolean(true);
    AtomicBoolean includeMetadata = new AtomicBoolean(true);
    AtomicBoolean cancel = new AtomicBoolean(true);

    // After getting the filename we create a tree dialog to select the items you wish to include in
    // the export.
    try {
      Shell treeShell =
          new Shell(
              shell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX | SWT.APPLICATION_MODAL);
      treeShell.setText(BaseMessages.getString(PKG, "HopGui.FileMenu.Project.Export.Label"));
      treeShell.setImage(GuiResource.getInstance().getImageHopUi());
      treeShell.setMinimumSize(500, 300);
      PropsUi.setLook(treeShell);

      FormLayout formLayout = new FormLayout();
      formLayout.marginWidth = PropsUi.getFormMargin();
      formLayout.marginHeight = PropsUi.getFormMargin();
      treeShell.setLayout(formLayout);

      // Some buttons at the bottom
      Button okButton = new Button(treeShell, SWT.PUSH);
      okButton.setText(BaseMessages.getString(PKG, "System.Button.OK"));
      okButton.addListener(
          SWT.Selection,
          event -> {
            cancel.set(false);
            treeShell.dispose();
          });

      Button cancelButton = new Button(treeShell, SWT.PUSH);
      cancelButton.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
      cancelButton.addListener(SWT.Selection, event -> treeShell.dispose());
      BaseTransformDialog.positionBottomButtons(
          shell, new Button[] {okButton, cancelButton}, PropsUi.getMargin(), null);

      Label separator = new Label(treeShell, SWT.SEPARATOR | SWT.HORIZONTAL);
      separator.setLayoutData(
          new FormDataBuilder()
              .left()
              .fullWidth()
              .bottom(okButton, -2 * PropsUi.getMargin())
              .result());

      Button btnIncludeMetadata = new Button(treeShell, SWT.CHECK);
      PropsUi.setLook(btnIncludeMetadata);
      btnIncludeMetadata.setText(
          BaseMessages.getString(PKG, "ProjectGuiPlugin.IncludeMetadata.Label"));
      btnIncludeMetadata.setSelection(true);
      btnIncludeMetadata.setLayoutData(
          new FormDataBuilder().fullWidth().bottom(separator, -2 * PropsUi.getMargin()).result());
      btnIncludeMetadata.addListener(
          SWT.Selection, event -> includeMetadata.set(btnIncludeMetadata.getSelection()));

      Button btnIncludeVariables = new Button(treeShell, SWT.CHECK);
      PropsUi.setLook(btnIncludeVariables);
      btnIncludeVariables.setText(
          BaseMessages.getString(PKG, "ProjectGuiPlugin.IncludeVariables.Label"));
      btnIncludeVariables.setSelection(true);
      btnIncludeVariables.addListener(
          SWT.Selection, event -> includeVariables.set(btnIncludeVariables.getSelection()));
      btnIncludeVariables.setLayoutData(
          new FormDataBuilder()
              .fullWidth()
              .bottom(btnIncludeMetadata, -PropsUi.getMargin())
              .result());

      tree = new FileTree(treeShell, HopVfs.getFileObject(projectHome), projectName);
      tree.setLayoutData(
          new FormDataBuilder()
              .top()
              .fullWidth()
              .bottom(btnIncludeVariables, -PropsUi.getMargin())
              .result());

      BaseTransformDialog.setSize(treeShell);

      treeShell.setDefaultButton(okButton);
      treeShell.pack();
      treeShell.open();

      while (!treeShell.isDisposed()) {
        if (!treeShell.getDisplay().readAndDispatch()) {
          treeShell.getDisplay().sleep();
        }
      }

      if (cancel.get()) {
        return;
      }

      IRunnableWithProgress op =
          monitor -> {
            try {
              monitor.setTaskName(
                  BaseMessages.getString(PKG, "ProjectGuiPlugin.ZipDirectory.Taskname.Text"));
              HashMap<String, String> variablesMap = new HashMap<>();

              for (String name : variables.getVariableNames()) {
                if (!name.contains("java.")
                    && !name.contains("user.")
                    && !name.contains("sun.")
                    && !name.contains("os.")
                    && !name.contains("file.")
                    && !name.contains("jdk.")
                    && !name.contains("http.")
                    && !name.contains("path.")
                    && !name.contains("ftp.")
                    && !name.contains("line.")
                    && !name.contains("awt.")
                    && !name.equals("HOP_METADATA_FOLDER")
                    && !name.contains("HOP_ENVIRONMENT_NAME")
                    && !name.contains("HOP_AUDIT_FOLDER")
                    && !name.contains("HOP_CONFIG_FOLDER")
                    && !name.contains("PROJECT_HOME")
                    && !name.contains("HOP_PROJECTS")
                    && !name.contains("HOP_PLATFORM_OS")
                    && !name.contains("HOP_PROJECT_NAME")
                    && !name.contains("HOP_SERVER_URL")) {
                  String value = variables.getVariable(name);
                  variablesMap.put(name, value);
                }
              }
              ObjectMapper objectMapper = new ObjectMapper();
              String variablesJson = objectMapper.writeValueAsString(variablesMap);

              SerializableMetadataProvider metadataProvider =
                  new SerializableMetadataProvider(hopGui.getMetadataProvider());
              String metadataJson = metadataProvider.toJson();

              FileObject zipFile = HopVfs.getFileObject(zipFilename);
              OutputStream outputStream = HopVfs.getOutputStream(zipFile, false);
              ZipOutputStream zos = new ZipOutputStream(outputStream);
              FileObject projectDirectory = HopVfs.getFileObject(projectHome);
              String projectHomeFolder =
                  HopVfs.getFileObject(projectHome).getParent().getName().getURI();

              // Includes selected files and dependencies
              if (!tree.getFileObjects().isEmpty()) {
                for (FileObject fileObject : tree.getFileObjects()) {
                  // To prevent the zip file from including itself
                  if (zipFile.equals(fileObject)) {
                    continue;
                  }
                  monitor.subTask(fileObject.getName().getURI());
                  zipFile(fileObject, fileObject.getName().getURI(), zos, projectHomeFolder);
                }
              }

              // Includes config file
              zipFile(
                  HopVfs.getFileObject(
                      Const.HOP_CONFIG_FOLDER + Const.FILE_SEPARATOR + Const.HOP_CONFIG),
                  projectDirectory.getName().getBaseName()
                      + Const.FILE_SEPARATOR
                      + Const.HOP_CONFIG,
                  zos,
                  projectHomeFolder);

              // Includes variables
              if (includeVariables.get()) {
                zipString(
                    variablesJson, "variables.json", zos, projectDirectory.getName().getBaseName());
              }

              // Includes metadata
              if (includeMetadata.get()) {
                zipString(
                    metadataJson, "metadata.json", zos, projectDirectory.getName().getBaseName());
              }

              zos.close();
              outputStream.close();
            } catch (Exception e) {
              throw new InvocationTargetException(e, "Error zipping project: " + e.getMessage());
            } finally {
              monitor.done();
            }
          };

      ProgressMonitorDialog pmd = new ProgressMonitorDialog(shell);
      pmd.run(false, op);

      GuiResource.getInstance().toClipboard(zipFilename);

      MessageBox box = new MessageBox(shell, SWT.CLOSE | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "ProjectGuiPlugin.ZipDirectory.Dialog.Header"));
      box.setMessage(
          BaseMessages.getString(PKG, "ProjectGuiPlugin.ZipDirectory.Dialog.Message1", zipFilename)
              + Const.CR
              + BaseMessages.getString(PKG, "ProjectGuiPlugin.ZipDirectory.Dialog.Message2"));
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "ProjectGuiPlugin.ZipDirectory.Error.Dialog.Header"),
          BaseMessages.getString(PKG, "ProjectGuiPlugin.ZipDirectory.Error.Dialog.Message"),
          e);
    }
  }

  /**
   * Add file Object to zip file
   *
   * @param fileToZip the file to add
   * @param filename destination filename
   * @param zipOutputStream zip output stream to append the file to
   * @param projectHomeParent project home folder to be stripped from the base path
   * @throws IOException when failing to add a file to the zip
   */
  public void zipFile(
      FileObject fileToZip,
      String filename,
      ZipOutputStream zipOutputStream,
      String projectHomeParent)
      throws IOException {
    if (fileToZip.isHidden()) {
      return;
    }

    // Build relative filename
    filename = filename.replace(projectHomeParent, "");
    if (filename.startsWith("/")) {
      filename = filename.substring(1);
    }

    zipOutputStream.putNextEntry(new ZipEntry(filename));
    if (fileToZip.isFolder()) {
      zipOutputStream.closeEntry();
      for (FileObject childFile : fileToZip.getChildren()) {
        zipFile(
            childFile,
            filename + "/" + childFile.getName().getBaseName(),
            zipOutputStream,
            projectHomeParent);
      }
    } else {
      InputStream fis = HopVfs.getInputStream(fileToZip);
      fis.transferTo(zipOutputStream);
      fis.close();
      zipOutputStream.closeEntry();
    }
  }

  /**
   * Add a string to a zip file
   *
   * @param stringToZip the string that needs to be added to the zip file
   * @param filename the destination filename inside the zip
   * @param zipOutputStream output stream of the zip file
   * @param projectHomeParent the root folder of the zip file
   * @throws IOException when adding to the zip file fails
   */
  public void zipString(
      String stringToZip,
      String filename,
      ZipOutputStream zipOutputStream,
      String projectHomeParent)
      throws IOException {
    if (Utils.isEmpty(stringToZip)) {
      return;
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(stringToZip.getBytes());
    zipOutputStream.putNextEntry(new ZipEntry(projectHomeParent + "/" + filename));
    byte[] bytes = new byte[1024];
    int length;
    while ((length = bais.read(bytes)) >= 0) {
      zipOutputStream.write(bytes, 0, length);
    }
    zipOutputStream.closeEntry();
    bais.close();
  }
}
