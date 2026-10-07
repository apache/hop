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

package org.apache.hop.projects.project;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.projects.environment.EmbeddedEnvironment;
import org.apache.hop.projects.environment.EmbeddedEnvironmentValidator;
import org.apache.hop.projects.environment.EmbeddedEnvironmentVariable;
import org.apache.hop.projects.util.Defaults;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.NamingSchemeTypes;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.HelpUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/** Edits one embedded environment: name, description, and variables. */
public class EmbeddedEnvironmentDialog extends Dialog {
  private static final Class<?> PKG = EmbeddedEnvironmentDialog.class;

  private final EmbeddedEnvironment environment;
  private final List<String> otherNames;
  private final IVariables variables;
  private final PropsUi props;

  private Shell shell;
  private TextVar wName;
  private Text wDescription;
  private TableView wVariables;

  private EmbeddedEnvironment returnValue;

  public EmbeddedEnvironmentDialog(
      Shell parent,
      EmbeddedEnvironment environment,
      List<String> otherNames,
      IVariables variables) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    this.environment = environment;
    this.otherNames = otherNames == null ? List.of() : otherNames;
    this.variables = variables;
    this.props = PropsUi.getInstance();
  }

  /**
   * @return the edited environment, or null when cancelled. The environment passed to the
   *     constructor is updated only when the user confirms.
   */
  public EmbeddedEnvironment open() {
    environment.absorbLegacyVariables();
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    shell.setImage(
        GuiResource.getInstance()
            .getImage(
                "environment.svg",
                PKG.getClassLoader(),
                ConstUi.SMALL_ICON_SIZE,
                ConstUi.SMALL_ICON_SIZE));
    PropsUi.setLook(shell);

    int margin = PropsUi.getMargin() + 2;
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "EmbeddedEnvironmentDialog.Shell.Name"));

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, event -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, event -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin * 3, null);
    HelpUtils.createHelpButton(shell, Const.getDocUrl(Defaults.DOCUMENTATION_URI));

    int middle = props.getMiddlePct();
    Control last = addName(margin, middle);
    last = addDescription(margin, middle, last);

    int size = environment.getVariables() == null ? 0 : environment.getVariables().size();
    wVariables =
        new TableView(
            variables, shell, SWT.BORDER, variableColumns(), Math.max(size, 3), event -> {}, props);
    PropsUi.setLook(wVariables);
    FormData fdVariables = new FormData();
    fdVariables.left = new FormAttachment(0, 0);
    fdVariables.top = new FormAttachment(last, margin);
    fdVariables.right = new FormAttachment(100, 0);
    fdVariables.bottom = new FormAttachment(wOk, -margin * 2);
    wVariables.setLayoutData(fdVariables);
    fillVariables(wVariables, environment.getVariables());

    wName.setText(Const.NVL(environment.getName(), ""));
    wDescription.setText(Const.NVL(environment.getDescription(), ""));

    shell.setMinimumSize(700, 500);
    shell.setDefaultButton(wOk);
    wName.setFocus();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return returnValue;
  }

  private Control addName(int margin, int middle) {
    Label label = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, "EmbeddedEnvironmentDialog.Label.Name"));
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.right = new FormAttachment(middle, 0);
    fdLabel.top = new FormAttachment(0, margin);
    label.setLayoutData(fdLabel);

    wName =
        new TextVar(variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT)
            .asNameField(NamingSchemeTypes.HOP_METADATA);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, margin);
    fdName.right = new FormAttachment(100, 0);
    fdName.top = new FormAttachment(label, 0, SWT.CENTER);
    wName.setLayoutData(fdName);
    return wName;
  }

  private Control addDescription(int margin, int middle, Control previous) {
    Label label = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, "EmbeddedEnvironmentDialog.Label.Description"));
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.right = new FormAttachment(middle, 0);
    fdLabel.top = new FormAttachment(previous, margin);
    label.setLayoutData(fdLabel);

    wDescription = new Text(shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wDescription);
    FormData fdDescription = new FormData();
    fdDescription.left = new FormAttachment(middle, margin);
    fdDescription.right = new FormAttachment(100, 0);
    fdDescription.top = new FormAttachment(label, 0, SWT.CENTER);
    wDescription.setLayoutData(fdDescription);
    return wDescription;
  }

  private ColumnInfo[] variableColumns() {
    ColumnInfo[] columnInfo =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "EmbeddedEnvironmentDialog.Column.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "EmbeddedEnvironmentDialog.Column.Default"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "EmbeddedEnvironmentDialog.Column.Description"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "EmbeddedEnvironmentDialog.Column.Mandatory"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {"Y", "N"},
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "EmbeddedEnvironmentDialog.Column.Secret"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {"Y", "N"},
              true),
        };
    columnInfo[0].setUsingVariables(true);
    columnInfo[0].setNamingSchemeType(NamingSchemeTypes.HOP_VARIABLE);
    columnInfo[1].setUsingVariables(true);
    columnInfo[1].setToolTip(
        BaseMessages.getString(PKG, "EmbeddedEnvironmentDialog.Column.Default.Tooltip"));
    columnInfo[3].setUsingVariables(false);
    columnInfo[3].setToolTip(
        BaseMessages.getString(PKG, "EmbeddedEnvironmentDialog.Column.Mandatory.Tooltip"));
    columnInfo[4].setUsingVariables(false);
    columnInfo[4].setToolTip(
        BaseMessages.getString(PKG, "EmbeddedEnvironmentDialog.Column.Secret.Tooltip"));
    return columnInfo;
  }

  private void fillVariables(TableView table, List<EmbeddedEnvironmentVariable> rows) {
    if (rows == null) {
      return;
    }
    for (int i = 0; i < rows.size(); i++) {
      EmbeddedEnvironmentVariable variable = rows.get(i);
      TableItem item = table.table.getItem(i);
      item.setText(1, Const.NVL(variable.getName(), ""));
      item.setText(2, Const.NVL(variable.getDefaultValue(), ""));
      item.setText(3, Const.NVL(variable.getDescription(), ""));
      item.setText(4, variable.isMandatory() ? "Y" : "N");
      item.setText(5, variable.isSecret() ? "Y" : "N");
    }
    table.setRowNums();
    table.optWidth(true);
  }

  private List<EmbeddedEnvironmentVariable> readVariables(TableView table) {
    List<EmbeddedEnvironmentVariable> rows = new ArrayList<>();
    for (int i = 0; i < table.nrNonEmpty(); i++) {
      TableItem item = table.getNonEmpty(i);
      if (StringUtils.isBlank(item.getText(1))) {
        continue;
      }
      rows.add(
          new EmbeddedEnvironmentVariable(
              item.getText(1),
              item.getText(2),
              item.getText(3),
              "Y".equals(item.getText(4)),
              "Y".equals(item.getText(5))));
    }
    return rows;
  }

  private void ok() {
    EmbeddedEnvironment edited = new EmbeddedEnvironment();
    edited.setName(wName.getText());
    edited.setDescription(wDescription.getText());
    edited.setVariables(readVariables(wVariables));
    EmbeddedEnvironmentValidator.normalize(edited);

    if (EmbeddedEnvironmentValidator.missingName(edited)) {
      showError(
          "EmbeddedEnvironmentDialog.MissingName.Header",
          "EmbeddedEnvironmentDialog.MissingName.Message");
      return;
    }
    String duplicateVariable = EmbeddedEnvironmentValidator.duplicateVariableName(edited);
    if (duplicateVariable != null) {
      showError(
          "EmbeddedEnvironmentDialog.DuplicateVariable.Header",
          "EmbeddedEnvironmentDialog.DuplicateVariable.Message",
          duplicateVariable);
      return;
    }
    if (EmbeddedEnvironmentValidator.nameTaken(edited.getName(), otherNames)) {
      showError(
          "EmbeddedEnvironmentDialog.DuplicateName.Header",
          "EmbeddedEnvironmentDialog.DuplicateName.Message",
          edited.getName());
      return;
    }

    environment.setName(edited.getName());
    environment.setDescription(edited.getDescription());
    environment.setVariables(edited.getVariables());
    returnValue = environment;
    dispose();
  }

  private void showError(String headerKey, String messageKey, String... args) {
    MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
    box.setText(BaseMessages.getString(PKG, headerKey));
    box.setMessage(BaseMessages.getString(PKG, messageKey, (Object[]) args));
    box.open();
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
