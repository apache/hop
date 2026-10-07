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
import org.apache.hop.projects.environment.EmbeddedEnvironmentImporter;
import org.apache.hop.projects.environment.EmbeddedEnvironmentImporter.EnvironmentSource;
import org.apache.hop.projects.environment.EmbeddedEnvironmentImporter.Kind;
import org.apache.hop.projects.environment.EmbeddedEnvironmentImporter.VariableAssignment;
import org.apache.hop.projects.util.Defaults;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
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

/**
 * Asks how each variable from the selected lifecycle environments should be stored: as a variable,
 * as mandatory, or as a secret. Values from the configuration files are not shown or copied.
 */
public class ImportEmbeddedEnvironmentsDialog extends Dialog {
  private static final Class<?> PKG = ImportEmbeddedEnvironmentsDialog.class;

  private final List<EnvironmentSource> sources;
  private final IVariables variables;
  private final PropsUi props;

  private Shell shell;
  private TableView wVariables;
  private List<EmbeddedEnvironment> returnValue;

  public ImportEmbeddedEnvironmentsDialog(
      Shell parent, List<EnvironmentSource> sources, IVariables variables) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    this.sources = sources == null ? List.of() : sources;
    this.variables = variables;
    this.props = PropsUi.getInstance();
  }

  /**
   * @return the embedded environments to add, or null when cancelled
   */
  public List<EmbeddedEnvironment> open() {
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
    shell.setText(BaseMessages.getString(PKG, "ImportEmbeddedEnvironmentsDialog.Shell.Name"));

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, event -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, event -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin * 3, null);
    HelpUtils.createHelpButton(shell, Const.getDocUrl(Defaults.DOCUMENTATION_URI));

    Label explanation = new Label(shell, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(explanation);
    explanation.setText(
        BaseMessages.getString(PKG, "ImportEmbeddedEnvironmentsDialog.Explanation"));
    FormData fdExplanation = new FormData();
    fdExplanation.left = new FormAttachment(0, 0);
    fdExplanation.right = new FormAttachment(100, 0);
    fdExplanation.top = new FormAttachment(0, 0);
    explanation.setLayoutData(fdExplanation);

    Control last = explanation;
    String unreadable = unreadableFiles();
    if (unreadable != null) {
      Label warning = new Label(shell, SWT.LEFT | SWT.WRAP);
      PropsUi.setLook(warning);
      warning.setText(unreadable);
      FormData fdWarning = new FormData();
      fdWarning.left = new FormAttachment(0, 0);
      fdWarning.right = new FormAttachment(100, 0);
      fdWarning.top = new FormAttachment(explanation, margin);
      warning.setLayoutData(fdWarning);
      last = warning;
    }

    List<Row> rows = rowsInOrder();
    wVariables =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.SINGLE,
            columns(),
            Math.max(rows.size(), 1),
            false,
            null,
            props,
            true,
            null,
            false,
            false);
    PropsUi.setLook(wVariables);
    FormData fdVariables = new FormData();
    fdVariables.left = new FormAttachment(0, 0);
    fdVariables.right = new FormAttachment(100, 0);
    fdVariables.top = new FormAttachment(last, margin);
    fdVariables.bottom = new FormAttachment(wOk, -margin * 2);
    wVariables.setLayoutData(fdVariables);
    fillRows(rows);

    shell.setMinimumSize(760, 480);
    shell.setDefaultButton(wOk);
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return returnValue;
  }

  private void ok() {
    List<Row> edited = readRows();
    List<EmbeddedEnvironment> environments = new ArrayList<>();
    for (EnvironmentSource source : sources) {
      List<VariableAssignment> forSource = new ArrayList<>();
      for (Row row : edited) {
        if (source.getName().equals(row.environmentName)) {
          forSource.add(new VariableAssignment(row.name, row.description, row.kind));
        }
      }
      environments.add(
          EmbeddedEnvironmentImporter.toEmbeddedEnvironment(
              source.getName(), source.getDescription(), forSource));
    }
    returnValue = environments;
    dispose();
  }

  private void cancel() {
    returnValue = null;
    dispose();
  }

  private void dispose() {
    shell.dispose();
  }

  private ColumnInfo[] columns() {
    String[] kinds =
        new String[] {kindLabel(Kind.VARIABLE), kindLabel(Kind.MANDATORY), kindLabel(Kind.SECRET)};
    ColumnInfo environment =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ImportEmbeddedEnvironmentsDialog.Column.Environment"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false,
            true);
    ColumnInfo name =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ImportEmbeddedEnvironmentsDialog.Column.Name"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false,
            true);
    ColumnInfo description =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ImportEmbeddedEnvironmentsDialog.Column.Description"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false,
            false);
    ColumnInfo kind =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ImportEmbeddedEnvironmentsDialog.Column.Kind"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            kinds,
            true);
    return new ColumnInfo[] {environment, name, description, kind};
  }

  private void fillRows(List<Row> rows) {
    for (int i = 0; i < rows.size() && i < wVariables.table.getItemCount(); i++) {
      Row row = rows.get(i);
      TableItem item = wVariables.table.getItem(i);
      item.setText(1, Const.NVL(row.environmentName, ""));
      item.setText(2, Const.NVL(row.name, ""));
      item.setText(3, Const.NVL(row.description, ""));
      item.setText(4, kindLabel(row.kind));
    }
    wVariables.setRowNums();
    wVariables.optWidth(true);
  }

  private List<Row> readRows() {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < wVariables.nrNonEmpty(); i++) {
      TableItem item = wVariables.getNonEmpty(i);
      if (StringUtils.isBlank(item.getText(2))) {
        continue;
      }
      rows.add(
          new Row(
              item.getText(1),
              item.getText(2),
              StringUtils.trimToNull(item.getText(3)),
              kindOf(item.getText(4))));
    }
    return rows;
  }

  private List<Row> rowsInOrder() {
    List<Row> rows = new ArrayList<>();
    for (EnvironmentSource source : sources) {
      for (VariableAssignment assignment : source.getVariables()) {
        rows.add(
            new Row(
                source.getName(),
                assignment.getName(),
                assignment.getDescription(),
                assignment.getKind()));
      }
    }
    return rows;
  }

  /** One table row: the environment it belongs to and the list the user chose. */
  private static final class Row {
    private final String environmentName;
    private final String name;
    private final String description;
    private final Kind kind;

    private Row(String environmentName, String name, String description, Kind kind) {
      this.environmentName = environmentName;
      this.name = name;
      this.description = description;
      this.kind = kind;
    }
  }

  private String unreadableFiles() {
    List<String> files = new ArrayList<>();
    for (EnvironmentSource source : sources) {
      for (String filename : source.getUnreadableFiles()) {
        if (StringUtils.isNotBlank(filename) && !files.contains(filename)) {
          files.add(filename);
        }
      }
    }
    if (files.isEmpty()) {
      return null;
    }
    int shown = Math.min(files.size(), 8);
    StringBuilder text = new StringBuilder();
    text.append(BaseMessages.getString(PKG, "ImportEmbeddedEnvironmentsDialog.Unreadable"));
    for (int i = 0; i < shown; i++) {
      text.append(Const.CR).append(files.get(i));
    }
    if (files.size() > shown) {
      text.append(Const.CR).append("...");
    }
    return text.toString();
  }

  private static String kindLabel(Kind kind) {
    Kind resolved = kind == null ? Kind.VARIABLE : kind;
    return switch (resolved) {
      case MANDATORY ->
          BaseMessages.getString(PKG, "ImportEmbeddedEnvironmentsDialog.Kind.Mandatory");
      case SECRET -> BaseMessages.getString(PKG, "ImportEmbeddedEnvironmentsDialog.Kind.Secret");
      default -> BaseMessages.getString(PKG, "ImportEmbeddedEnvironmentsDialog.Kind.Variable");
    };
  }

  private static Kind kindOf(String label) {
    if (kindLabel(Kind.MANDATORY).equals(label)) {
      return Kind.MANDATORY;
    }
    if (kindLabel(Kind.SECRET).equals(label)) {
      return Kind.SECRET;
    }
    return Kind.VARIABLE;
  }
}
