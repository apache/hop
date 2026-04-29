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

import java.io.File;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.git.GitRepoProvider;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.GitCloneHelper;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class CloneFromVersionControlDialog extends Dialog {
  private static final Class<?> PKG = CloneFromVersionControlDialog.class;

  private String returnValue;
  private Shell shell;
  private final PropsUi props;
  private final IVariables variables;

  private Text wUrl;
  private TextVar wDirectory;
  private Text wProjectName;
  private Text wToken;
  private Button wShallowClone;
  private Text wShallowDepth;

  public CloneFromVersionControlDialog(Shell parent, IVariables variables) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    this.variables = new Variables();
    this.variables.initializeFrom(variables);
    props = PropsUi.getInstance();
  }

  public String open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    shell.setText(BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Shell.Name"));
    shell.setMinimumSize(650, 400);
    shell.setImage(
        GuiResource.getInstance()
            .getImage(
                "project.svg",
                PKG.getClassLoader(),
                ConstUi.SMALL_ICON_SIZE,
                ConstUi.SMALL_ICON_SIZE));
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    PropsUi.setLook(shell);

    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString("System.Button.OK"));
    wOk.addListener(SWT.Selection, event -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString("System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, event -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin * 3, null);

    Composite comp = new Composite(shell, SWT.NONE);
    comp.setLayout(new FormLayout());
    comp.setLayoutData(
        FormDataBuilder.builder().fullWidth().top().bottom(wOk, -2 * margin).build());
    PropsUi.setLook(comp);

    Control lastControl = null;

    // URL
    Label wlUrl = new Label(comp, SWT.RIGHT);
    wlUrl.setText(BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Label.Url"));
    wlUrl.setLayoutData(FormDataBuilder.builder().left().right(middle, 0).top(margin, 0).build());
    PropsUi.setLook(wlUrl);

    Button wBrowseRepo = new Button(comp, SWT.PUSH);
    wBrowseRepo.setText(
        BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Button.BrowseRepo"));
    wBrowseRepo.setLayoutData(FormDataBuilder.builder().right().top(wlUrl, 0, SWT.CENTER).build());
    wBrowseRepo.addListener(SWT.Selection, e -> browseRepository());
    PropsUi.setLook(wBrowseRepo);

    wUrl = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    FormData fdUrl = new FormData();
    fdUrl.left = new FormAttachment(middle, margin);
    fdUrl.right = new FormAttachment(wBrowseRepo, -margin);
    fdUrl.top = new FormAttachment(wlUrl, 0, SWT.CENTER);
    wUrl.setLayoutData(fdUrl);
    PropsUi.setLook(wUrl);
    lastControl = wUrl;

    // Token (optional) — placed directly below the URL / Browse row
    Label wlToken = new Label(comp, SWT.RIGHT);
    wlToken.setText(BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Label.Token"));
    wlToken.setLayoutData(
        FormDataBuilder.builder().left().right(middle, 0).top(lastControl, margin).build());
    PropsUi.setLook(wlToken);

    wToken = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT | SWT.PASSWORD);
    PropsUi.setLook(wToken);
    wToken.setLayoutData(
        FormDataBuilder.builder().left(middle, margin).right().top(wlToken, 0, SWT.CENTER).build());
    wToken.setToolTipText(
        BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Label.Token.Tooltip"));
    lastControl = wToken;

    // Directory
    Label wlDir = new Label(comp, SWT.RIGHT);
    wlDir.setText(BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Label.Directory"));
    wlDir.setLayoutData(
        FormDataBuilder.builder().left().right(middle, 0).top(lastControl, margin).build());
    PropsUi.setLook(wlDir);

    Button wbDir = new Button(comp, SWT.PUSH);
    wbDir.setText(BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Button.Browse"));
    wbDir.setLayoutData(FormDataBuilder.builder().right().top(wlDir, 0, SWT.CENTER).build());
    wbDir.addListener(SWT.Selection, e -> browseDirectory());
    PropsUi.setLook(wbDir);

    wDirectory = new TextVar(variables, comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    wDirectory.setLayoutData(
        FormDataBuilder.builder()
            .left(middle, margin)
            .right(wbDir, -margin)
            .top(wlDir, 0, SWT.CENTER)
            .build());
    lastControl = wDirectory;

    // Project name
    Label wlName = new Label(comp, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Label.ProjectName"));
    wlName.setLayoutData(
        FormDataBuilder.builder().left().right(middle, 0).top(lastControl, margin).build());
    PropsUi.setLook(wlName);

    wProjectName = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    wProjectName.setLayoutData(
        FormDataBuilder.builder().left(middle, margin).right().top(wlName, 0, SWT.CENTER).build());
    wProjectName.addListener(SWT.Modify, e -> updateProjectNameFromUrl());
    PropsUi.setLook(wProjectName);
    lastControl = wProjectName;

    // Shallow clone
    Label wlShallow = new Label(comp, SWT.RIGHT);
    wlShallow.setText(
        BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Label.ShallowClone"));
    wlShallow.setLayoutData(
        FormDataBuilder.builder().left().right(middle, 0).top(lastControl, margin).build());
    PropsUi.setLook(wlShallow);

    wShallowClone = new Button(comp, SWT.CHECK);
    wShallowClone.setLayoutData(
        FormDataBuilder.builder().left(middle, margin).top(wlShallow, 0, SWT.CENTER).build());
    wShallowClone.addListener(SWT.Selection, e -> updateShallowDepthState());
    PropsUi.setLook(wShallowClone);

    wShallowDepth = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wShallowDepth);
    wShallowDepth.setText("1");
    wShallowDepth.setLayoutData(
        FormDataBuilder.builder()
            .left(wShallowClone, margin)
            .top(wlShallow, 0, SWT.CENTER)
            .width(60)
            .build());

    Label wlCommits = new Label(comp, SWT.LEFT);
    wlCommits.setText(
        BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Label.ShallowClone.Commits"));
    wlCommits.setLayoutData(
        FormDataBuilder.builder()
            .left(wShallowDepth, margin)
            .top(wlShallow, 0, SWT.CENTER)
            .build());
    PropsUi.setLook(wlCommits);
    getData();
    updateShallowDepthState();

    shell.setDefaultButton(wOk);
    wUrl.setFocus();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return returnValue;
  }

  private void browseRepository() {
    GitRepoProvider detectedProvider = GitRepoProvider.detectFromUrl(wUrl.getText().trim());
    SelectRepositoryDialog dialog =
        new SelectRepositoryDialog(shell, detectedProvider, wToken.getText().trim());
    String cloneUrl = dialog.open();
    if (cloneUrl != null) {
      wUrl.setText(cloneUrl);
      String repoName = dialog.getSelectedRepoName();
      if (repoName != null && StringUtils.isEmpty(wProjectName.getText())) {
        wProjectName.setText(repoName);
      }
    }
  }

  private void updateShallowDepthState() {
    wShallowDepth.setEnabled(wShallowClone.getSelection());
  }

  private void updateProjectNameFromUrl() {
    if (StringUtils.isEmpty(wProjectName.getText())) {
      String url = wUrl.getText();
      if (StringUtils.isNotEmpty(url)) {
        String baseName = FilenameUtils.getBaseName(url.replaceFirst("\\.git$", ""));
        if (StringUtils.isNotEmpty(baseName)) {
          wProjectName.setText(baseName);
        }
      }
    }
  }

  private void browseDirectory() {
    BaseDialog.presentDirectoryDialog(shell, wDirectory, variables);
  }

  private void getData() {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    String standardFolder = variables.resolve(config.getStandardProjectsFolder());
    wDirectory.setText(Const.NVL(standardFolder, ""));
  }

  private void ok() {
    try {
      String url = wUrl.getText().trim();
      if (StringUtils.isEmpty(url)) {
        throw new HopException("Please specify the repository URL");
      }
      String directory = variables.resolve(wDirectory.getText());
      if (StringUtils.isEmpty(directory)) {
        throw new HopException("Please specify the directory");
      }
      String projectName = wProjectName.getText().trim();
      if (StringUtils.isEmpty(projectName)) {
        projectName = FilenameUtils.getBaseName(url.replaceFirst("\\.git$", ""));
      }
      if (StringUtils.isEmpty(projectName)) {
        throw new HopException("Please specify a project name");
      }

      ProjectsConfig config = ProjectsConfigSingleton.getConfig();
      if (config.findProjectConfig(projectName) != null) {
        throw new HopException("Project '" + projectName + "' already exists");
      }

      String clonePath = directory + File.separator + projectName;
      FileObject cloneDir = HopVfs.getFileObject(clonePath);
      if (cloneDir.exists()) {
        throw new HopException("Directory '" + clonePath + "' already exists");
      }

      int depth = 0;
      if (wShallowClone.getSelection()) {
        try {
          depth = Integer.parseInt(wShallowDepth.getText().trim());
          if (depth < 1) {
            depth = 1;
          }
        } catch (NumberFormatException e) {
          depth = 1;
        }
      }

      if (!GitCloneHelper.cloneRepo(clonePath, url, wToken.getText(), depth)) {
        return;
      }

      String defaultConfigFile = variables.resolve(config.getDefaultProjectConfigFile());
      ProjectConfig projectConfig = new ProjectConfig(projectName, clonePath, defaultConfigFile);
      Project project = new Project();
      project.setParentProjectName(config.getStandardParentProject());

      String configFilename = projectConfig.getActualProjectConfigFilename(variables);
      FileObject configFile = HopVfs.getFileObject(configFilename);
      if (!configFile.exists()) {
        project.setConfigFilename(configFilename);
        if (!configFile.getParent().exists()) {
          configFile.getParent().createFolder();
        }
        project.saveToFile();
      } else {
        project.setConfigFilename(configFilename);
        project.readFromFile();
      }

      config.addProjectConfig(projectConfig);
      HopConfig.getInstance().saveToFile();

      HopGui hopGui = HopGui.getInstance();
      ProjectsGuiPlugin.updateProjectToolItem(projectName);
      ProjectsGuiPlugin.enableHopGuiProject(projectName, project, null);

      returnValue = projectName;
      dispose();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              ProjectsGuiPlugin.PKG, "ProjectGuiPlugin.AddProject.Error.Dialog.Header"),
          BaseMessages.getString(
              ProjectsGuiPlugin.PKG, "ProjectGuiPlugin.AddProject.Error.Dialog.Message"),
          e);
    }
  }

  private void cancel() {
    returnValue = null;
    dispose();
  }

  private void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
