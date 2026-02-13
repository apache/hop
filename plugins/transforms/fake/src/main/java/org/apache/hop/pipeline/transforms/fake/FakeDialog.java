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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class FakeDialog extends BaseTransformDialog {
  private static final Class<?> PKG = FakeDialog.class;

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
    wLocale.setText(transformName);
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
              BaseMessages.getString(PKG, "FakeDialog.Type.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[0]),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FakeDialog.Topic.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[0]),
        };
    columns[1].setComboValuesSelectionListener(this::getComboValues);
    columns[2].setComboValuesSelectionListener(this::getComboValues);

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

  private String[] getComboValues(TableItem tableItem, int rowNr, int colNr) {
    if (colNr == 2) {
      return FakerType.getTypeDescriptions();
    }
    if (colNr == 3) {
      String typeDescription = tableItem.getText(2);
      FakerType fakerType = FakerType.getTypeUsingDescription(typeDescription);
      if (fakerType != null) {
        return getMethodNames(fakerType);
      }
    }
    return new String[] {};
  }

  public String[] getMethodNames(FakerType fakerType) {
    try {
      Method[] methods = fakerType.getFakerClass().getDeclaredMethods();
      ArrayList<String> tempNames = new ArrayList<>();
      for (Method method : methods) {
        // Ignore methods needing parameters for now
        // or that doesn't return type (String, long or Date)
        if (method.getParameterCount() == 0
            && Modifier.isPublic(method.getModifiers())
            && (method.getReturnType().isAssignableFrom(String.class)
                || method.getReturnType().isAssignableFrom(long.class)
                || method.getReturnType().isAssignableFrom(Date.class))) {
          tempNames.add(method.getName());
        }
      }
      String[] names = new String[tempNames.size()];
      tempNames.toArray(names);
      return names;
    } catch (Exception e) {
      return new String[] {};
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wLocale.setText(Const.NVL(input.getLocale(), ""));
    for (int row = 0; row < input.getFields().size(); row++) {
      FakeField fakeField = input.getFields().get(row);
      TableItem item = wFields.table.getItem(row);
      int col = 1;
      item.setText(col++, Const.NVL(fakeField.getName(), ""));
      item.setText(
          col++, Const.NVL(FakerType.getTypeUsingName(fakeField.getType()).getDescription(), ""));
      item.setText(col, Const.NVL(fakeField.getTopic(), ""));
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

    transformName = wTransformName.getText(); // return value

    getInfo(input);

    dispose();
  }

  private void getInfo(FakeMeta meta) {
    meta.setLocale(wLocale.getText());
    meta.getFields().clear();
    List<TableItem> nonEmptyItems = wFields.getNonEmptyItems();
    for (TableItem tableItem : nonEmptyItems) {
      int col = 1;
      String name = tableItem.getText(col++);
      String typeDesc = tableItem.getText(col++);
      String topic = tableItem.getText(col);

      FakerType fakerType = FakerType.getTypeUsingDescription(typeDesc);
      if (fakerType == null) {
        fakerType = FakerType.getTypeUsingName(typeDesc);
      }
      if (fakerType != null) {
        meta.getFields().add(new FakeField(name, fakerType.name(), topic));
      }
    }
  }
}
