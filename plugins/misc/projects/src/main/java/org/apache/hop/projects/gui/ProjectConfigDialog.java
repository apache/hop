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

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * Small dialog to edit a project <em>registration</em> ({@link ProjectConfig} in hop-config.json):
 * name, home, config path, group, tags, and read-only. Does not write project-config.json.
 */
public class ProjectConfigDialog extends Dialog {
  private static final Class<?> PKG = ProjectConfigDialog.class;

  private final PropsUi props;
  private final IVariables variables;
  private final ProjectConfig projectConfig;
  private final String originalName;

  private Shell shell;
  private Text wName;
  private TextVar wHome;
  private TextVar wConfigFile;
  private ComboVar wGroup;
  private Text wTags;
  private Button wReadOnly;

  private boolean ok;

  public ProjectConfigDialog(Shell parent, ProjectConfig projectConfig, IVariables variables) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    this.projectConfig = projectConfig;
    this.variables = variables;
    this.originalName = projectConfig.getProjectName();
    this.props = PropsUi.getInstance();
  }

  /**
   * Open the dialog.
   *
   * @return true if the user confirmed changes
   */
  public boolean open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    shell.setText(BaseMessages.getString(PKG, "ProjectConfigDialog.Shell.Name"));
    shell.setImage(
        GuiResource.getInstance()
            .getImage(
                "project.svg",
                PKG.getClassLoader(),
                ConstUi.SMALL_ICON_SIZE,
                ConstUi.SMALL_ICON_SIZE));
    PropsUi.setLook(shell);

    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString("System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString("System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin * 3, null);

    Label wlName = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "ProjectConfigDialog.Label.Name"));
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, 0);
    fdlName.top = new FormAttachment(0, margin * 2);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, margin);
    fdName.right = new FormAttachment(100, 0);
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    Label wlHome = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(wlHome);
    wlHome.setText(BaseMessages.getString(PKG, "ProjectConfigDialog.Label.Home"));
    FormData fdlHome = new FormData();
    fdlHome.left = new FormAttachment(0, 0);
    fdlHome.right = new FormAttachment(middle, 0);
    fdlHome.top = new FormAttachment(lastControl, margin);
    wlHome.setLayoutData(fdlHome);
    Button wbHome = new Button(shell, SWT.PUSH);
    PropsUi.setLook(wbHome);
    wbHome.setText(BaseMessages.getString(PKG, "ProjectConfigDialog.Button.Browse"));
    FormData fdbHome = new FormData();
    fdbHome.right = new FormAttachment(100, 0);
    fdbHome.top = new FormAttachment(wlHome, 0, SWT.CENTER);
    wbHome.setLayoutData(fdbHome);
    wbHome.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wHome, variables));
    wHome = new TextVar(variables, shell, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wHome);
    FormData fdHome = new FormData();
    fdHome.left = new FormAttachment(middle, margin);
    fdHome.right = new FormAttachment(wbHome, -margin);
    fdHome.top = new FormAttachment(wlHome, 0, SWT.CENTER);
    wHome.setLayoutData(fdHome);
    lastControl = wHome;

    Label wlConfigFile = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(wlConfigFile);
    wlConfigFile.setText(BaseMessages.getString(PKG, "ProjectConfigDialog.Label.ConfigFile"));
    FormData fdlConfigFile = new FormData();
    fdlConfigFile.left = new FormAttachment(0, 0);
    fdlConfigFile.right = new FormAttachment(middle, 0);
    fdlConfigFile.top = new FormAttachment(lastControl, margin);
    wlConfigFile.setLayoutData(fdlConfigFile);
    wConfigFile = new TextVar(variables, shell, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wConfigFile);
    FormData fdConfigFile = new FormData();
    fdConfigFile.left = new FormAttachment(middle, margin);
    fdConfigFile.right = new FormAttachment(100, 0);
    fdConfigFile.top = new FormAttachment(wlConfigFile, 0, SWT.CENTER);
    wConfigFile.setLayoutData(fdConfigFile);
    lastControl = wConfigFile;

    Label wlGroup = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(wlGroup);
    wlGroup.setText(BaseMessages.getString(PKG, "ProjectConfigDialog.Label.Group"));
    FormData fdlGroup = new FormData();
    fdlGroup.left = new FormAttachment(0, 0);
    fdlGroup.right = new FormAttachment(middle, 0);
    fdlGroup.top = new FormAttachment(lastControl, margin);
    wlGroup.setLayoutData(fdlGroup);
    wGroup = new ComboVar(variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wGroup);
    FormData fdGroup = new FormData();
    fdGroup.left = new FormAttachment(middle, margin);
    fdGroup.right = new FormAttachment(100, 0);
    fdGroup.top = new FormAttachment(wlGroup, 0, SWT.CENTER);
    wGroup.setLayoutData(fdGroup);
    lastControl = wGroup;

    Label wlTags = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(wlTags);
    wlTags.setText(BaseMessages.getString(PKG, "ProjectConfigDialog.Label.Tags"));
    FormData fdlTags = new FormData();
    fdlTags.left = new FormAttachment(0, 0);
    fdlTags.right = new FormAttachment(middle, 0);
    fdlTags.top = new FormAttachment(lastControl, margin);
    wlTags.setLayoutData(fdlTags);
    wTags = new Text(shell, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wTags);
    wTags.setToolTipText(BaseMessages.getString(PKG, "ProjectConfigDialog.Label.Tags.Tooltip"));
    FormData fdTags = new FormData();
    fdTags.left = new FormAttachment(middle, margin);
    fdTags.right = new FormAttachment(100, 0);
    fdTags.top = new FormAttachment(wlTags, 0, SWT.CENTER);
    wTags.setLayoutData(fdTags);
    lastControl = wTags;

    Label wlReadOnly = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(wlReadOnly);
    wlReadOnly.setText(BaseMessages.getString(PKG, "ProjectConfigDialog.Label.ReadOnly"));
    FormData fdlReadOnly = new FormData();
    fdlReadOnly.left = new FormAttachment(0, 0);
    fdlReadOnly.right = new FormAttachment(middle, 0);
    fdlReadOnly.top = new FormAttachment(lastControl, margin);
    wlReadOnly.setLayoutData(fdlReadOnly);
    wReadOnly = new Button(shell, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wReadOnly);
    FormData fdReadOnly = new FormData();
    fdReadOnly.left = new FormAttachment(middle, margin);
    fdReadOnly.right = new FormAttachment(100, 0);
    fdReadOnly.top = new FormAttachment(wlReadOnly, 0, SWT.CENTER);
    wReadOnly.setLayoutData(fdReadOnly);

    getData();

    shell.setMinimumSize(500, 280);
    shell.setDefaultButton(wOk);
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return ok;
  }

  private void getData() {
    wName.setText(Const.NVL(projectConfig.getProjectName(), ""));
    wHome.setText(Const.NVL(projectConfig.getProjectHome(), ""));
    wConfigFile.setText(Const.NVL(projectConfig.getConfigFilename(), ""));
    wGroup.setText(Const.NVL(projectConfig.getGroup(), ""));
    List<String> groups = ProjectsConfigSingleton.getConfig().listProjectGroups();
    wGroup.setItems(groups.toArray(new String[0]));
    wTags.setText(projectConfig.getTagsAsDisplayString());
    wReadOnly.setSelection(
        projectConfig.isReadOnly()
            || ProjectConfig.isArchiveUri(variables.resolve(projectConfig.getProjectHome())));
  }

  private void ok() {
    try {
      String name = wName.getText();
      if (StringUtils.isEmpty(name)) {
        throw new IllegalArgumentException(
            BaseMessages.getString(PKG, "ProjectConfigDialog.Error.NameRequired"));
      }
      if (StringUtils.isEmpty(wHome.getText())) {
        throw new IllegalArgumentException(
            BaseMessages.getString(PKG, "ProjectConfigDialog.Error.HomeRequired"));
      }
      if (StringUtils.isEmpty(wConfigFile.getText())) {
        throw new IllegalArgumentException(
            BaseMessages.getString(PKG, "ProjectConfigDialog.Error.ConfigRequired"));
      }

      ProjectsConfig config = ProjectsConfigSingleton.getConfig();
      if (!name.equals(originalName)) {
        ProjectConfig clash = config.findProjectConfig(name);
        if (clash != null) {
          throw new IllegalArgumentException(
              BaseMessages.getString(PKG, "ProjectConfigDialog.Error.NameExists", name));
        }
      }

      projectConfig.setProjectName(name);
      projectConfig.setProjectHome(wHome.getText());
      projectConfig.setConfigFilename(wConfigFile.getText());
      projectConfig.setGroup(StringUtils.trimToEmpty(wGroup.getText()));
      projectConfig.setTags(ProjectConfig.parseTags(wTags.getText()));
      projectConfig.setReadOnly(wReadOnly.getSelection());

      ok = true;
      dispose();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ProjectConfigDialog.Error.Header"),
          BaseMessages.getString(PKG, "ProjectConfigDialog.Error.Message"),
          e);
    }
  }

  private void cancel() {
    ok = false;
    dispose();
  }

  private void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  /**
   * Original project name when the dialog opened (for rename detection).
   *
   * @return original name
   */
  public String getOriginalName() {
    return originalName;
  }
}
