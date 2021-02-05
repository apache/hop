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

package org.apache.hop.projects.project;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.DescribedVariable;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.io.File;
import java.util.Collections;
import java.util.List;

public class ProjectDialog extends Dialog {
  private static final Class<?> PKG = ProjectDialog.class; // For Translator

  private final Project project;
  private final ProjectConfig projectConfig;

  private String returnValue;

  private Shell shell;
  private final PropsUi props;

  private Text wName;
  private TextVar wHome;
  private ComboVar wParentProject;
  private TextVar wConfigFile;
  private Text wDescription;
  private Text wCompany;
  private Text wDepartment;

  private TextVar wMetadataBaseFolder;
  private TextVar wUnitTestsBasePath;
  private TextVar wDataSetCsvFolder;
  private Button wEnforceHomeExecution;
  private TableView wVariables;

  private int margin;
  private int middle;

  private IVariables variables;

  private boolean needingProjectRefresh;

  private String originalName;

  public ProjectDialog(
      Shell parent, Project project, ProjectConfig projectConfig, IVariables variables) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);

    this.project = project;
    this.projectConfig = projectConfig;

    props = PropsUi.getInstance();

    this.variables = new Variables();
    this.variables.initializeFrom(null);
    this.originalName = projectConfig.getProjectName();
    try {
      project.modifyVariables(variables, projectConfig, Collections.emptyList(), null);
    } catch(Exception e) {
      new ErrorDialog(parent, "Error", "There is an error in the project definition, variables couldn't be set", e);
    }
  }

  public String open() {

    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    props.setLook(shell);

    margin = Const.MARGIN + 2;
    middle = Const.MIDDLE_PCT;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText("Project dialog");

    // Buttons go at the bottom of the dialog
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, event -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, event -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin * 3, null);

    Label wlName = new Label(shell, SWT.RIGHT);
    props.setLook(wlName);
    wlName.setText("Name ");
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, 0);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wName);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, margin);
    fdName.right = new FormAttachment(100, 0);
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    Label wlHome = new Label(shell, SWT.RIGHT);
    props.setLook(wlHome);
    wlHome.setText("Home folder ");
    FormData fdlHome = new FormData();
    fdlHome.left = new FormAttachment(0, 0);
    fdlHome.right = new FormAttachment(middle, 0);
    fdlHome.top = new FormAttachment(lastControl, margin);
    wlHome.setLayoutData(fdlHome);
    Button wbHome = new Button(shell, SWT.PUSH);
    props.setLook(wbHome);
    wbHome.setText("Browse...");
    FormData fdbHome = new FormData();
    fdbHome.right = new FormAttachment(100, 0);
    fdbHome.top = new FormAttachment(wlHome, 0, SWT.CENTER);
    wbHome.setLayoutData(fdbHome);
    wbHome.addListener(SWT.Selection, this::browseHomeFolder);
    wHome = new TextVar(variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wHome);
    FormData fdHome = new FormData();
    fdHome.left = new FormAttachment(middle, margin);
    fdHome.right = new FormAttachment(wbHome, -margin);
    fdHome.top = new FormAttachment(wlHome, 0, SWT.CENTER);
    wHome.setLayoutData(fdHome);
    lastControl = wHome;

    Label wlConfigFile = new Label(shell, SWT.RIGHT);
    props.setLook(wlConfigFile);
    wlConfigFile.setText("Configuration file (relative path) ");
    FormData fdlConfigFile = new FormData();
    fdlConfigFile.left = new FormAttachment(0, 0);
    fdlConfigFile.right = new FormAttachment(middle, 0);
    fdlConfigFile.top = new FormAttachment(lastControl, margin);
    wlConfigFile.setLayoutData(fdlConfigFile);
    wConfigFile = new TextVar(variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wConfigFile);
    FormData fdConfigFile = new FormData();
    fdConfigFile.left = new FormAttachment(middle, margin);
    fdConfigFile.right = new FormAttachment(100, 0);
    fdConfigFile.top = new FormAttachment(wlConfigFile, 0, SWT.CENTER);
    wConfigFile.setLayoutData(fdConfigFile);
    lastControl = wConfigFile;

    Label wlParentProject = new Label(shell, SWT.RIGHT);
    props.setLook(wlParentProject);
    wlParentProject.setText("Parent project to inherit from ");
    FormData fdlParentProject = new FormData();
    fdlParentProject.left = new FormAttachment(0, 0);
    fdlParentProject.right = new FormAttachment(middle, 0);
    fdlParentProject.top = new FormAttachment(lastControl, margin);
    wlParentProject.setLayoutData(fdlParentProject);
    wParentProject = new ComboVar(variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wParentProject);
    FormData fdParentProject = new FormData();
    fdParentProject.left = new FormAttachment(middle, margin);
    fdParentProject.right = new FormAttachment(100, 0);
    fdParentProject.top = new FormAttachment(wlParentProject, 0, SWT.CENTER);
    wParentProject.setLayoutData(fdParentProject);
    lastControl = wParentProject;

    Label wlDescription = new Label(shell, SWT.RIGHT);
    props.setLook(wlDescription);
    wlDescription.setText("Description ");
    FormData fdlDescription = new FormData();
    fdlDescription.left = new FormAttachment(0, 0);
    fdlDescription.right = new FormAttachment(middle, 0);
    fdlDescription.top = new FormAttachment(lastControl, margin);
    wlDescription.setLayoutData(fdlDescription);
    wDescription = new Text(shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wDescription);
    FormData fdDescription = new FormData();
    fdDescription.left = new FormAttachment(middle, margin);
    fdDescription.right = new FormAttachment(100, 0);
    fdDescription.top = new FormAttachment(wlDescription, 0, SWT.CENTER);
    wDescription.setLayoutData(fdDescription);
    lastControl = wDescription;

    Label wlCompany = new Label(shell, SWT.RIGHT);
    props.setLook(wlCompany);
    wlCompany.setText("Company ");
    FormData fdlCompany = new FormData();
    fdlCompany.left = new FormAttachment(0, 0);
    fdlCompany.right = new FormAttachment(middle, 0);
    fdlCompany.top = new FormAttachment(lastControl, margin);
    wlCompany.setLayoutData(fdlCompany);
    wCompany = new Text(shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wCompany);
    FormData fdCompany = new FormData();
    fdCompany.left = new FormAttachment(middle, margin);
    fdCompany.right = new FormAttachment(100, 0);
    fdCompany.top = new FormAttachment(wlCompany, 0, SWT.CENTER);
    wCompany.setLayoutData(fdCompany);
    lastControl = wCompany;

    Label wlDepartment = new Label(shell, SWT.RIGHT);
    props.setLook(wlDepartment);
    wlDepartment.setText("Department ");
    FormData fdlDepartment = new FormData();
    fdlDepartment.left = new FormAttachment(0, 0);
    fdlDepartment.right = new FormAttachment(middle, 0);
    fdlDepartment.top = new FormAttachment(lastControl, margin);
    wlDepartment.setLayoutData(fdlDepartment);
    wDepartment = new Text(shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wDepartment);
    FormData fdDepartment = new FormData();
    fdDepartment.left = new FormAttachment(middle, margin);
    fdDepartment.right = new FormAttachment(100, 0);
    fdDepartment.top = new FormAttachment(wlDepartment, 0, SWT.CENTER);
    wDepartment.setLayoutData(fdDepartment);
    lastControl = wDepartment;

    Label wlMetadataBaseFolder = new Label(shell, SWT.RIGHT);
    props.setLook(wlMetadataBaseFolder);
    wlMetadataBaseFolder.setText("Metadata base folder (HOP_METADATA_FOLDER)");
    FormData fdlMetadataBaseFolder = new FormData();
    fdlMetadataBaseFolder.left = new FormAttachment(0, 0);
    fdlMetadataBaseFolder.right = new FormAttachment(middle, 0);
    fdlMetadataBaseFolder.top = new FormAttachment(lastControl, margin);
    wlMetadataBaseFolder.setLayoutData(fdlMetadataBaseFolder);
    wMetadataBaseFolder = new TextVar(variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wMetadataBaseFolder);
    FormData fdMetadataBaseFolder = new FormData();
    fdMetadataBaseFolder.left = new FormAttachment(middle, margin);
    fdMetadataBaseFolder.right = new FormAttachment(100, 0);
    fdMetadataBaseFolder.top = new FormAttachment(wlMetadataBaseFolder, 0, SWT.CENTER);
    wMetadataBaseFolder.setLayoutData(fdMetadataBaseFolder);
    wMetadataBaseFolder.addModifyListener(e -> updateIVariables());
    lastControl = wMetadataBaseFolder;

    Label wlUnitTestsBasePath = new Label(shell, SWT.RIGHT);
    props.setLook(wlUnitTestsBasePath);
    wlUnitTestsBasePath.setText("Unit tests base path (HOP_UNIT_TESTS_FOLDER) ");
    FormData fdlUnitTestsBasePath = new FormData();
    fdlUnitTestsBasePath.left = new FormAttachment(0, 0);
    fdlUnitTestsBasePath.right = new FormAttachment(middle, 0);
    fdlUnitTestsBasePath.top = new FormAttachment(lastControl, margin);
    wlUnitTestsBasePath.setLayoutData(fdlUnitTestsBasePath);
    wUnitTestsBasePath = new TextVar(variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wUnitTestsBasePath);
    FormData fdUnitTestsBasePath = new FormData();
    fdUnitTestsBasePath.left = new FormAttachment(middle, margin);
    fdUnitTestsBasePath.right = new FormAttachment(100, 0);
    fdUnitTestsBasePath.top = new FormAttachment(wlUnitTestsBasePath, 0, SWT.CENTER);
    wUnitTestsBasePath.setLayoutData(fdUnitTestsBasePath);
    wUnitTestsBasePath.addModifyListener(e -> updateIVariables());
    lastControl = wUnitTestsBasePath;

    Label wlDataSetCsvFolder = new Label(shell, SWT.RIGHT);
    props.setLook(wlDataSetCsvFolder);
    wlDataSetCsvFolder.setText("Data Sets CSV Folder (HOP_DATASETS_FOLDER)");
    FormData fdlDataSetCsvFolder = new FormData();
    fdlDataSetCsvFolder.left = new FormAttachment(0, 0);
    fdlDataSetCsvFolder.right = new FormAttachment(middle, 0);
    fdlDataSetCsvFolder.top = new FormAttachment(lastControl, margin);
    wlDataSetCsvFolder.setLayoutData(fdlDataSetCsvFolder);
    wDataSetCsvFolder = new TextVar(variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wDataSetCsvFolder);
    FormData fdDataSetCsvFolder = new FormData();
    fdDataSetCsvFolder.left = new FormAttachment(middle, margin);
    fdDataSetCsvFolder.right = new FormAttachment(100, 0);
    fdDataSetCsvFolder.top = new FormAttachment(wlDataSetCsvFolder, 0, SWT.CENTER);
    wDataSetCsvFolder.setLayoutData(fdDataSetCsvFolder);
    wDataSetCsvFolder.addModifyListener(e -> updateIVariables());
    lastControl = wDataSetCsvFolder;

    Label wlEnforceHomeExecution = new Label(shell, SWT.RIGHT);
    props.setLook(wlEnforceHomeExecution);
    wlEnforceHomeExecution.setText("Enforce executions in project home? ");
    FormData fdlEnforceHomeExecution = new FormData();
    fdlEnforceHomeExecution.left = new FormAttachment(0, 0);
    fdlEnforceHomeExecution.right = new FormAttachment(middle, 0);
    fdlEnforceHomeExecution.top = new FormAttachment(lastControl, margin);
    wlEnforceHomeExecution.setLayoutData(fdlEnforceHomeExecution);
    wEnforceHomeExecution = new Button(shell, SWT.CHECK | SWT.LEFT);
    props.setLook(wEnforceHomeExecution);
    FormData fdEnforceHomeExecution = new FormData();
    fdEnforceHomeExecution.left = new FormAttachment(middle, margin);
    fdEnforceHomeExecution.right = new FormAttachment(100, 0);
    fdEnforceHomeExecution.top = new FormAttachment(wlEnforceHomeExecution, 0, SWT.CENTER);
    wEnforceHomeExecution.setLayoutData(fdEnforceHomeExecution);
    lastControl = wlEnforceHomeExecution;

    Label wlVariables = new Label(shell, SWT.LEFT);
    props.setLook(wlVariables);
    wlVariables.setText("Project variables to set : ");
    FormData fdlVariables = new FormData();
    fdlVariables.left = new FormAttachment(0, 0);
    fdlVariables.right = new FormAttachment(100, 0);
    fdlVariables.top = new FormAttachment(lastControl, 2 * margin);
    wlVariables.setLayoutData(fdlVariables);

    ColumnInfo[] columnInfo =
        new ColumnInfo[] {
          new ColumnInfo("Name", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
          new ColumnInfo("Value", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
          new ColumnInfo(
              "Description (optional information)", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
        };
    columnInfo[0].setUsingVariables(true);
    columnInfo[1].setUsingVariables(true);

    wVariables =
        new TableView(
            new Variables(),
            shell,
            SWT.BORDER,
            columnInfo,
            project.getDescribedVariables().size(),
            null,
            props);
    props.setLook(wVariables);
    FormData fdVariables = new FormData();
    fdVariables.left = new FormAttachment(0, 0);
    fdVariables.right = new FormAttachment(100, 0);
    fdVariables.top = new FormAttachment(wlVariables, margin);
    fdVariables.bottom = new FormAttachment(wOk, -margin * 2);
    wVariables.setLayoutData(fdVariables);
    wVariables.addModifyListener(
        e -> {
          needingProjectRefresh = true;
        });
    // lastControl = wVariables;

    // When enter is hit, close the dialog
    //
    wDescription.addListener(SWT.DefaultSelection, e -> ok());
    wCompany.addListener(SWT.DefaultSelection, e -> ok());
    wDepartment.addListener(SWT.DefaultSelection, e -> ok());
    wMetadataBaseFolder.addListener(SWT.DefaultSelection, e -> ok());
    wUnitTestsBasePath.addListener(SWT.DefaultSelection, e -> ok());
    wDataSetCsvFolder.addListener(SWT.DefaultSelection, e -> ok());
    wParentProject.addListener( SWT.DefaultSelection, e->ok() );
    wParentProject.addModifyListener(e -> needingProjectRefresh = true);
    wHome.addModifyListener(e -> needingProjectRefresh = true);

    // Set the shell size, based upon previous time...
    BaseTransformDialog.setSize(shell);

    getData();

    shell.open();

    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    return returnValue;
  }

  private void browseHomeFolder(Event event) {
    String homeFolder = BaseDialog.presentDirectoryDialog(shell, wHome, variables);

    // Set the name to the base folder if the name is empty
    //
    if (homeFolder != null && StringUtils.isEmpty(wName.getText())) {
      File file = new File(homeFolder);
      wName.setText(Const.NVL(file.getName(), ""));
    }
  }

  private void updateIVariables() {
    Project env = new Project();
    ProjectConfig pc = new ProjectConfig();
    try {
      getInfo(env, pc);
      env.modifyVariables(variables, pc, Collections.emptyList(), null);
    } catch (HopException e) {
      new ErrorDialog(shell, "Error", "There is a configuration error in the project:", e);
    }
  }

  private void ok() {
    try {
      // Do some extra validations to prevent bad data ending up in the projects configuration
      //
      String homeFolder = wHome.getText();
      if (StringUtils.isEmpty(homeFolder)) {
        throw new HopException("Please specify a home folder for your project");
      }
      if (!new File(homeFolder).exists()) {
        throw new HopException(
            "Please specify an existing home folder for your project. Folder '"
                + homeFolder
                + "' doesn't seem to exist.");
      }

      // Renaming the project is not supported.
      //
      String projectName = wName.getText();
      if (StringUtils.isEmpty(projectName)) {
        throw new HopException("Please give your new project a name");
      }
      if (StringUtils.isNotEmpty(originalName)) {
        if (!projectName.equals(originalName)) {
          wName.setText(originalName);
          throw new HopException("Sorry, renaming project '" + originalName + "' is not supported");
        }
      }

      getInfo(project, projectConfig);
      returnValue = projectConfig.getProjectName();
      dispose();
    } catch (HopException e) {
      new ErrorDialog(shell, "Error", "There is a configuration error in the project:", e);
    }
  }

  private void cancel() {
    returnValue = null;

    dispose();
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  private void getData() {
    wName.setText(Const.NVL(projectConfig.getProjectName(), ""));
    wHome.setText(Const.NVL(projectConfig.getProjectHome(), ""));
    wConfigFile.setText(Const.NVL(projectConfig.getConfigFilename(), ""));

    wDescription.setText(Const.NVL(project.getDescription(), ""));
    wCompany.setText(Const.NVL(project.getCompany(), ""));
    wDepartment.setText(Const.NVL(project.getDepartment(), ""));
    wMetadataBaseFolder.setText(Const.NVL(project.getMetadataBaseFolder(), ""));
    wUnitTestsBasePath.setText(Const.NVL(project.getUnitTestsBasePath(), ""));
    wDataSetCsvFolder.setText(Const.NVL(project.getDataSetsCsvFolder(), ""));
    wEnforceHomeExecution.setSelection(project.isEnforcingExecutionInHome());
    for (int i = 0; i < project.getDescribedVariables().size(); i++) {
      DescribedVariable describedVariable = project.getDescribedVariables().get(i);
      TableItem item = wVariables.table.getItem(i);
      item.setText(1, Const.NVL(describedVariable.getName(), ""));
      item.setText(2, Const.NVL(describedVariable.getValue(), ""));
      item.setText(3, Const.NVL(describedVariable.getDescription(), ""));
    }
    wVariables.setRowNums();
    wVariables.optWidth(true);

    // Parent project...
    //
    try {
      wParentProject.setText(Const.NVL(project.getParentProjectName(), ""));

      List<String> names = ProjectsConfigSingleton.getConfig().listProjectConfigNames();
      if (projectConfig.getProjectName() != null) {
        names.remove(projectConfig.getProjectName());
      }
      wParentProject.setItems(names.toArray(new String[0]));
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting list of project names", e);
    }
  }

  private void getInfo(Project project, ProjectConfig projectConfig) throws HopException {

    projectConfig.setProjectName(wName.getText());
    projectConfig.setProjectHome(wHome.getText());
    projectConfig.setConfigFilename(wConfigFile.getText());

    project.setParentProjectName(wParentProject.getText());
    project.setDescription(wDescription.getText());
    project.setCompany(wCompany.getText());
    project.setDepartment(wDepartment.getText());
    project.setMetadataBaseFolder(wMetadataBaseFolder.getText());
    project.setUnitTestsBasePath(wUnitTestsBasePath.getText());
    project.setDataSetsCsvFolder(wDataSetCsvFolder.getText());
    project.setEnforcingExecutionInHome(wEnforceHomeExecution.getSelection());
    project.getDescribedVariables().clear();
    for (int i = 0; i < wVariables.nrNonEmpty(); i++) {
      TableItem item = wVariables.getNonEmpty(i);
      DescribedVariable variable =
          new DescribedVariable(
              item.getText(1), // name
              item.getText(2), // value
              item.getText(3) // description
              );
      project.getDescribedVariables().add(variable);
    }

    // Update the project to the right absolute configuration file
    //
    if (StringUtils.isNotEmpty(projectConfig.getProjectHome())
        && StringUtils.isNotEmpty(projectConfig.configFilename)) {
      project.setConfigFilename(projectConfig.getActualProjectConfigFilename(variables));
    }

    // Check for infinite loops
    //
    project.verifyProjectsChain(projectConfig.getProjectName(), variables);
  }

  /**
   * Gets variablesChanged
   *
   * @return value of variablesChanged
   */
  public boolean isNeedingProjectRefresh() {
    return needingProjectRefresh;
  }

  /** @param needingProjectRefresh The variablesChanged to set */
  public void setNeedingProjectRefresh(boolean needingProjectRefresh) {
    this.needingProjectRefresh = needingProjectRefresh;
  }
}
