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

package org.apache.hop.pipeline.transforms.fake;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import net.datafaker.Faker;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class FakeDialog extends BaseTransformDialog {
  private static final Class<?> PKG = FakeDialog.class;

  /** Table column holding the output field name. */
  private static final int COL_NAME = 1;

  /** Table column holding the encoded generator (see {@link #encode}/{@link #decode}). */
  private static final int COL_GENERATOR = 2;

  private TableView wFields;
  private ComboVar wLocale;

  private final FakeMeta input;

  public FakeDialog(
      Shell parent, IVariables variables, FakeMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "FakeDialog.DialogTitle"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    Control lastControl = wSpacer;

    // Locale line
    Label wlLocale = new Label(shell, SWT.RIGHT);
    wlLocale.setText(BaseMessages.getString(PKG, "FakeDialog.Label.Locale"));
    PropsUi.setLook(wlLocale);
    FormData fdlLocale = new FormData();
    fdlLocale.left = new FormAttachment(0, 0);
    fdlLocale.right = new FormAttachment(middle, -margin);
    fdlLocale.top = new FormAttachment(lastControl, margin);
    wlLocale.setLayoutData(fdlLocale);
    wLocale = new ComboVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wLocale.setItems(FakeMeta.getFakerLocales());
    PropsUi.setLook(wLocale);
    wLocale.addModifyListener(lsMod);
    FormData fdLocale = new FormData();
    fdLocale.left = new FormAttachment(middle, 0);
    fdLocale.top = new FormAttachment(wlLocale, 0, SWT.CENTER);
    fdLocale.right = new FormAttachment(100, 0);
    wLocale.setLayoutData(fdLocale);

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "FakeDialog.Name.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FakeDialog.Generator.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT_BUTTON,
              false),
        };
    columns[1].setToolTip(BaseMessages.getString(PKG, "FakeDialog.Generator.Tooltip"));
    // A TEXT_BUTTON column only renders its "..." button when the column uses variables.
    columns[1].setUsingVariables(true);
    columns[1].setTextVarButtonSelectionListener(getGeneratorBrowseAdapter());

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            input.getFields().size(),
            lsMod,
            props);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wLocale, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -margin);
    wFields.setLayoutData(fdFields);

    lsResize =
        event -> {
          Point size = shell.getSize();
          wFields.setSize(size.x - 10, size.y - 50);
          wFields.table.setSize(size.x - 10, size.y - 50);
          wFields.redraw();
        };
    shell.addListener(SWT.Resize, lsResize);

    getData();

    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Opens the searchable generator browser for the row whose '...' button was clicked. */
  private SelectionAdapter getGeneratorBrowseAdapter() {
    return new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        TableItem item = wFields.getActiveTableItem();
        if (item == null) {
          return;
        }
        int column = wFields.getActiveTableColumn();

        FakeField current = decode(item.getText(column));
        current.setName(item.getText(COL_NAME));

        Faker faker = FakerCatalog.createFaker(variables.resolve(wLocale.getText()));
        FakerBrowserDialog browser =
            new FakerBrowserDialog(
                shell, variables, faker, current, FakeDialog.this::appendFieldRow);
        FakeField picked = browser.open();
        if (picked != null) {
          item.setText(column, encode(picked));
          String name = picked.getName();
          item.setText(COL_NAME, Utils.isEmpty(name) ? Const.NVL(picked.getTopic(), "") : name);
          input.setChanged();
        }
      }
    };
  }

  /**
   * Appends a new field row to the grid. Invoked by the generator browser's "Add field" button so
   * several fields can be added in one visit to the browser.
   */
  void appendFieldRow(FakeField field) {
    String name =
        Utils.isEmpty(field.getName()) ? Const.NVL(field.getTopic(), "field") : field.getName();
    wFields.add(uniqueFieldName(name), encode(field));
    wFields.setRowNums();
    wFields.optWidth(true);
    input.setChanged();
  }

  /** Make a field name unique within the grid by appending {@code _2}, {@code _3}, ... */
  private String uniqueFieldName(String base) {
    Set<String> existing = new HashSet<>();
    for (TableItem item : wFields.table.getItems()) {
      existing.add(item.getText(COL_NAME));
    }
    if (!existing.contains(base)) {
      return base;
    }
    int suffix = 2;
    while (existing.contains(base + "_" + suffix)) {
      suffix++;
    }
    return base + "_" + suffix;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wLocale.setText(Const.NVL(input.getLocale(), ""));
    for (int row = 0; row < input.getFields().size(); row++) {
      FakeField field = input.getFields().get(row);
      TableItem item = wFields.table.getItem(row);
      item.setText(COL_NAME, Const.NVL(field.getName(), ""));
      item.setText(COL_GENERATOR, encode(field));
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText();

    getInfo(input);

    dispose();
  }

  private void getInfo(FakeMeta meta) {
    meta.setLocale(wLocale.getText());
    meta.getFields().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      FakeField field = decode(item.getText(COL_GENERATOR));
      field.setName(item.getText(COL_NAME));
      if (field.isValid()) {
        meta.getFields().add(field);
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Encoding of a generator (type, topic and arguments) into the single editable grid cell.
  // Format: accessor.topic(argType=argValue, argType=argValue)
  // -----------------------------------------------------------------------------------------------

  static String encode(FakeField field) {
    if (Utils.isEmpty(field.getType()) || Utils.isEmpty(field.getTopic())) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    sb.append(field.getType()).append('.').append(field.getTopic()).append('(');
    boolean first = true;
    for (FakeArgument argument : field.getArguments()) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      sb.append(Const.NVL(argument.getType(), ""))
          .append('=')
          .append(Const.NVL(argument.getValue(), ""));
    }
    return sb.append(')').toString();
  }

  static FakeField decode(String text) {
    FakeField field = new FakeField();
    if (Utils.isEmpty(text)) {
      return field;
    }
    String head = text.trim();
    String argumentBlock = "";
    int open = head.indexOf('(');
    if (open >= 0) {
      int close = head.lastIndexOf(')');
      argumentBlock = head.substring(open + 1, close > open ? close : head.length()).trim();
      head = head.substring(0, open).trim();
    }
    int dot = head.indexOf('.');
    if (dot >= 0) {
      field.setType(head.substring(0, dot).trim());
      field.setTopic(head.substring(dot + 1).trim());
    } else {
      field.setType(head);
    }

    List<FakeArgument> arguments = new ArrayList<>();
    if (!argumentBlock.isEmpty()) {
      for (String part : argumentBlock.split(",")) {
        int equals = part.indexOf('=');
        if (equals >= 0) {
          arguments.add(
              new FakeArgument(
                  part.substring(0, equals).trim(), part.substring(equals + 1).trim()));
        }
      }
    }
    field.setArguments(arguments);
    return field;
  }
}
