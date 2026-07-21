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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.history.AuditEvent;
import org.apache.hop.history.AuditManager;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.ProjectsUtil;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/**
 * Dialog listing all registered projects with filter, metadata columns, and the ability to edit a
 * registration or open the selected project.
 */
public class SelectProjectsDialog extends Dialog {
  private static final Class<?> PKG = SelectProjectsDialog.class;

  /** TableView column index for project name (0 is the row-number column). */
  private static final int COL_NAME = 1;

  private final PropsUi props;
  private final IVariables variables;

  private Shell shell;
  private Text wFilter;
  private TableView wTable;
  private Button wEdit;
  private Button wDelete;
  private Button wOk;

  private String selectedProjectName;

  /**
   * Cached last-used timestamps from AuditManager for the lifetime of this dialog. Loaded once so
   * filter typing does not re-query audit storage on every keystroke.
   */
  private Map<String, String> lastUsedDisplayByProject;

  public SelectProjectsDialog(Shell parent, IVariables variables) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    this.variables = variables;
    this.props = PropsUi.getInstance();
  }

  /**
   * Open the dialog.
   *
   * @return selected project name to open, or {@code null} if cancelled
   */
  public String open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    shell.setText(BaseMessages.getString(PKG, "SelectProjectsDialog.Shell.Name"));
    shell.setImage(
        GuiResource.getInstance()
            .getImage(
                "project.svg",
                PKG.getClassLoader(),
                ConstUi.SMALL_ICON_SIZE,
                ConstUi.SMALL_ICON_SIZE));
    PropsUi.setLook(shell);

    int margin = PropsUi.getMargin();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    shell.setMinimumSize(700, 450);

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString("System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wOk.setEnabled(false);

    wEdit = new Button(shell, SWT.PUSH);
    wEdit.setText(BaseMessages.getString(PKG, "SelectProjectsDialog.Button.Edit"));
    wEdit.setEnabled(false);
    wEdit.addListener(SWT.Selection, e -> editSelected());

    wDelete = new Button(shell, SWT.PUSH);
    wDelete.setText(BaseMessages.getString(PKG, "SelectProjectsDialog.Button.Delete"));
    wDelete.setEnabled(false);
    wDelete.addListener(SWT.Selection, e -> deleteSelected());

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString("System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wEdit, wDelete, wCancel}, margin * 3, null);

    Label wlFilter = new Label(shell, SWT.LEFT);
    PropsUi.setLook(wlFilter);
    wlFilter.setText(BaseMessages.getString(PKG, "SelectProjectsDialog.Label.Filter"));
    FormData fdlFilter = new FormData();
    fdlFilter.left = new FormAttachment(0, 0);
    fdlFilter.top = new FormAttachment(0, margin);
    wlFilter.setLayoutData(fdlFilter);

    wFilter = new Text(shell, SWT.SINGLE | SWT.BORDER | SWT.SEARCH | SWT.ICON_CANCEL);
    PropsUi.setLook(wFilter);
    wFilter.setToolTipText(
        BaseMessages.getString(PKG, "SelectProjectsDialog.Label.Filter.Tooltip"));
    FormData fdFilter = new FormData();
    fdFilter.left = new FormAttachment(wlFilter, margin);
    fdFilter.right = new FormAttachment(100, 0);
    fdFilter.top = new FormAttachment(wlFilter, 0, SWT.CENTER);
    wFilter.setLayoutData(fdFilter);
    wFilter.addModifyListener(e -> refreshTable());

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectProjectsDialog.Table.Col.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectProjectsDialog.Table.Col.Group"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectProjectsDialog.Table.Col.Folder"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectProjectsDialog.Table.Col.Tags"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectProjectsDialog.Table.Col.LastUsed"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectProjectsDialog.Table.Col.ReadOnly"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
        };

    wTable =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL,
            columns,
            0,
            null,
            props);
    wTable.setReadonly(true);
    PropsUi.setLook(wTable);
    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(0, 0);
    fdTable.right = new FormAttachment(100, 0);
    fdTable.top = new FormAttachment(wFilter, margin);
    fdTable.bottom = new FormAttachment(wOk, -margin * 2);
    wTable.setLayoutData(fdTable);

    wTable.table.addListener(SWT.Selection, e -> updateButtons());
    wTable.table.addListener(SWT.DefaultSelection, e -> ok());

    // Load audit last-used once for this dialog instance (filter uses the cache).
    loadLastUsedCache();
    refreshTable();
    selectCurrentProject();

    shell.setDefaultButton(wOk);
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return selectedProjectName;
  }

  /**
   * Load project "open" events from AuditManager into {@link #lastUsedDisplayByProject}. Called
   * once per dialog open so filter keystrokes stay cheap.
   */
  private void loadLastUsedCache() {
    lastUsedDisplayByProject = new HashMap<>();
    try {
      List<AuditEvent> projectOpenEvents =
          AuditManager.findEvents(
              ProjectsUtil.STRING_PROJECTS_AUDIT_GROUP,
              ProjectsUtil.STRING_PROJECT_AUDIT_TYPE,
              "open",
              100,
              true);
      DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
      for (AuditEvent event : projectOpenEvents) {
        if (StringUtils.isEmpty(event.getName()) || event.getDate() == null) {
          continue;
        }
        lastUsedDisplayByProject.put(event.getName(), dateFormat.format(event.getDate()));
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error reading last used project dates from the audit logs", e);
    }
  }

  private String formatLastUsed(String projectName) {
    if (lastUsedDisplayByProject == null || projectName == null) {
      return "";
    }
    return Const.NVL(lastUsedDisplayByProject.get(projectName), "");
  }

  private void refreshTable() {
    String previouslySelected = getSelectedProjectName();
    String filter = wFilter.getText();

    List<ProjectConfig> projects =
        new ArrayList<>(ProjectsConfigSingleton.getConfig().getProjectConfigurations());
    projects.sort(
        Comparator.comparing(
                (ProjectConfig p) -> Const.NVL(p.getGroup(), ""), String.CASE_INSENSITIVE_ORDER)
            .thenComparing(p -> Const.NVL(p.getProjectName(), ""), String.CASE_INSENSITIVE_ORDER));

    wTable.table.removeAll();
    for (ProjectConfig projectConfig : projects) {
      if (!projectConfig.matchesFilter(filter)) {
        continue;
      }
      TableItem item = new TableItem(wTable.table, SWT.NONE);
      // Column 0 is the TableView row-number column; name is always column 1
      item.setText(COL_NAME, Const.NVL(projectConfig.getProjectName(), ""));
      item.setText(2, Const.NVL(projectConfig.getGroup(), ""));
      item.setText(3, Const.NVL(projectConfig.getProjectHome(), ""));
      item.setText(4, projectConfig.getTagsAsDisplayString());
      item.setText(5, formatLastUsed(projectConfig.getProjectName()));
      item.setText(
          6,
          projectConfig.isReadOnly()
              ? BaseMessages.getString(PKG, "SelectProjectsDialog.Table.Yes")
              : BaseMessages.getString(PKG, "SelectProjectsDialog.Table.No"));
    }
    wTable.optimizeTableView();

    if (StringUtils.isNotEmpty(previouslySelected)) {
      selectProjectByName(previouslySelected);
    }
    updateButtons();
  }

  private void selectCurrentProject() {
    String current = HopNamespace.getNamespace();
    if (StringUtils.isNotEmpty(current)) {
      selectProjectByName(current);
    }
    updateButtons();
  }

  private void selectProjectByName(String name) {
    for (TableItem item : wTable.table.getItems()) {
      if (name.equals(item.getText(COL_NAME))) {
        wTable.table.setSelection(item);
        wTable.table.showSelection();
        return;
      }
    }
  }

  /**
   * Project name from the selected row. Always use column 1 (Name) — TableView sort can recreate
   * items so item data is not reliable after header clicks.
   */
  private String getSelectedProjectName() {
    int index = wTable.getSelectionIndex();
    if (index < 0 || index >= wTable.table.getItemCount()) {
      return null;
    }
    String name = wTable.table.getItem(index).getText(COL_NAME);
    return StringUtils.isEmpty(name) ? null : name;
  }

  private void updateButtons() {
    boolean hasSelection = StringUtils.isNotEmpty(getSelectedProjectName());
    wOk.setEnabled(hasSelection);
    wEdit.setEnabled(hasSelection);
    wDelete.setEnabled(hasSelection);
  }

  private void editSelected() {
    String name = getSelectedProjectName();
    if (name == null) {
      return;
    }
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    ProjectConfig projectConfig = config.findProjectConfig(name);
    if (projectConfig == null) {
      return;
    }

    ProjectConfigDialog editDialog = new ProjectConfigDialog(shell, projectConfig, variables);
    if (editDialog.open()) {
      try {
        config.updateProjectConfig(editDialog.getOriginalName(), projectConfig);
        ProjectsConfigSingleton.saveConfig();
        refreshTable();
        selectProjectByName(projectConfig.getProjectName());
        updateButtons();
      } catch (Exception e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "SelectProjectsDialog.Error.Save.Header"),
            BaseMessages.getString(PKG, "SelectProjectsDialog.Error.Save.Message"),
            e);
      }
    }
  }

  private void deleteSelected() {
    String name = getSelectedProjectName();
    if (name == null) {
      return;
    }
    // Re-use the same confirmation / parent-reference / hop-config removal as the toolbar action.
    // Pass this dialog shell so MessageBox/ErrorDialog are modal children (not the main Hop shell).
    boolean deleted = new ProjectsGuiPlugin().deleteRegisteredProject(shell, name);
    if (deleted) {
      refreshTable();
      updateButtons();
    }
  }

  private void ok() {
    selectedProjectName = getSelectedProjectName();
    if (selectedProjectName == null) {
      return;
    }
    dispose();
  }

  private void cancel() {
    selectedProjectName = null;
    dispose();
  }

  private void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
