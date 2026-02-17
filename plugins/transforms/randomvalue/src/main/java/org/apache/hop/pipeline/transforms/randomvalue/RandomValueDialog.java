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
package org.apache.hop.pipeline.transforms.randomvalue;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class RandomValueDialog extends BaseTransformDialog {
  private static final Class<?> PKG = RandomValueMeta.class;

  private TextVar wSeed;
  private TableView wFields;

  private final RandomValueMeta input;

  public RandomValueDialog(
      Shell parent,
      IVariables variables,
      RandomValueMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "RandomValueDialog.DialogTitle"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    Control lastControl = wSpacer;

    // Seed line
    Label wlSeed = new Label(shell, SWT.RIGHT);
    wlSeed.setText(BaseMessages.getString(PKG, "RandomValueDialog.Seed.Label"));
    PropsUi.setLook(wlSeed);
    FormData fdlSeed = new FormData();
    fdlSeed.left = new FormAttachment(0, 0);
    fdlSeed.right = new FormAttachment(middle, -margin);
    fdlSeed.top = new FormAttachment(lastControl, margin);
    wlSeed.setLayoutData(fdlSeed);
    wSeed = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSeed.setText(transformName);
    PropsUi.setLook(wSeed);
    FormData fdSeed = new FormData();
    fdSeed.left = new FormAttachment(middle, 0);
    fdSeed.top = new FormAttachment(wlSeed, 0, SWT.CENTER);
    fdSeed.right = new FormAttachment(100, 0);
    wSeed.setLayoutData(fdSeed);
    lastControl = wSeed;

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "RandomValueDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(lastControl, margin);
    wlFields.setLayoutData(fdlFields);

    final int nrRows = input.getFields().size();

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "RandomValueDialog.NameColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RandomValueDialog.TypeColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false)
        };
    columns[1].setSelectionAdapter(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            enterTypeSelection(e);
          }
        });

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            nrRows,
            null,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -margin);
    wFields.setLayoutData(fdFields);

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void enterTypeSelection(SelectionEvent e) {
    EnterSelectionDialog esd =
        new EnterSelectionDialog(
            shell,
            RandomValueMeta.RandomType.getDescriptions(),
            BaseMessages.getString(PKG, "RandomValueDialog.SelectInfoType.DialogTitle"),
            BaseMessages.getString(PKG, "RandomValueDialog.SelectInfoType.DialogMessage"));
    String string = esd.open();
    if (string != null) {
      TableView tv = (TableView) e.widget;
      tv.setText(string, e.x, e.y);
      input.setChanged();
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wSeed.setText(Const.NVL(input.getSeed(), ""));
    for (int i = 0; i < input.getFields().size(); i++) {
      TableItem item = wFields.table.getItem(i);
      RandomValueMeta.RVField field = input.getFields().get(i);

      item.setText(1, Const.NVL(field.getName(), ""));
      if (field.getType() != null) {
        item.setText(2, field.getType().getDescription());
      }
    }

    wFields.optimizeTableView();
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value
    input.setSeed(wSeed.getText());
    input.getFields().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      RandomValueMeta.RVField field = new RandomValueMeta.RVField();
      input.getFields().add(field);
      field.setName(item.getText(1));
      field.setType(RandomValueMeta.RandomType.lookupDescription(item.getText(2)));
    }
    input.setChanged();
    dispose();
  }
}
