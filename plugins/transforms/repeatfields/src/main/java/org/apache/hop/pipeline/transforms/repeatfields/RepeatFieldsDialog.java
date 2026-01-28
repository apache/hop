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
 *
 */

package org.apache.hop.pipeline.transforms.repeatfields;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class RepeatFieldsDialog extends BaseTransformDialog {
  private static final Class<?> PKG = RepeatFields.class;

  public static final String STRING_SORT_WARNING_PARAMETER = "RepeatFieldsWarning";

  private ColumnInfo[] ciGroup;
  private TableView wGroup;

  private ColumnInfo[] ciRepeats;
  private TableView wRepeats;

  private final RepeatFieldsMeta input;
  private final List<String> inputFields = new ArrayList<>();

  public RepeatFieldsDialog(
      Shell parent,
      IVariables variables,
      RepeatFieldsMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "RepeatFieldsDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    Label wlGroup = new Label(shell, SWT.NONE);
    wlGroup.setText(BaseMessages.getString(PKG, "RepeatFieldsDialog.Group.Label"));
    PropsUi.setLook(wlGroup);
    FormData fdlGroup = new FormData();
    fdlGroup.left = new FormAttachment(0, 0);
    fdlGroup.top = new FormAttachment(0, margin);
    wlGroup.setLayoutData(fdlGroup);

    // The group fields
    //
    int groupRows = input.getGroupFields().size();
    ciGroup =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "RepeatFieldsDialog.ColumnInfo.GroupField"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false)
        };

    wGroup =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciGroup,
            groupRows,
            null,
            props);

    Button wGet = new Button(shell, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "RepeatFieldsDialog.GetFields.Button"));
    FormData fdGet = new FormData();
    fdGet.top = new FormAttachment(wlGroup, margin);
    fdGet.right = new FormAttachment(100, 0);
    wGet.setLayoutData(fdGet);
    wGet.addListener(SWT.Selection, e -> getGroups());

    FormData fdGroup = new FormData();
    fdGroup.left = new FormAttachment(0, 0);
    fdGroup.top = new FormAttachment(wlGroup, margin);
    fdGroup.right = new FormAttachment(wGet, -margin);
    fdGroup.bottom = new FormAttachment(30, 0); // 35% from the top
    wGroup.setLayoutData(fdGroup);

    // The repeat fields section
    //
    Label wlRepeats = new Label(shell, SWT.NONE);
    wlRepeats.setText(BaseMessages.getString(PKG, "RepeatFieldsDialog.RepeatFields.Label"));
    PropsUi.setLook(wlRepeats);
    FormData fdlRepeats = new FormData();
    fdlRepeats.left = new FormAttachment(0, 0);
    fdlRepeats.top = new FormAttachment(wGroup, margin);
    wlRepeats.setLayoutData(fdlRepeats);

    ciRepeats =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "RepeatFieldsDialog.ColumnInfo.RepeatType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RepeatFieldsDialog.ColumnInfo.SourceFieldName"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RepeatFieldsDialog.ColumnInfo.TargetFieldName"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RepeatFieldsDialog.ColumnInfo.IndicatorFieldName"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RepeatFieldsDialog.ColumnInfo.IndicatorValue"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };

    ciRepeats[0].setComboValues(RepeatFieldsMeta.RepeatType.getDescriptions());
    ciRepeats[1].setUsingVariables(true);
    ciRepeats[2].setUsingVariables(true);
    ciRepeats[3].setUsingVariables(true);

    wRepeats =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciRepeats,
            input.getRepeats().size(),
            null,
            props);

    Button wGetRepeats = new Button(shell, SWT.PUSH);
    wGetRepeats.setText(BaseMessages.getString(PKG, "RepeatFieldsDialog.GetRepeatFields.Button"));
    FormData fdGetRepeats = new FormData();
    fdGetRepeats.top = new FormAttachment(wlRepeats, margin);
    fdGetRepeats.right = new FormAttachment(100, 0);
    wGetRepeats.setLayoutData(fdGetRepeats);
    wGetRepeats.addListener(SWT.Selection, e -> getRepeats());

    FormData fdRepeats = new FormData();
    fdRepeats.left = new FormAttachment(0, 0);
    fdRepeats.top = new FormAttachment(wlRepeats, margin);
    fdRepeats.right = new FormAttachment(wGetRepeats, -margin);
    fdRepeats.bottom = new FormAttachment(100, -50);
    wRepeats.setLayoutData(fdRepeats);

    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

              // Remember these fields...
              for (int i = 0; i < row.size(); i++) {
                inputFields.add(row.getValueMeta(i).getName());
              }
              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    ciGroup[0].setComboValues(fieldNames);
    ciRepeats[1].setComboValues(fieldNames);
    ciRepeats[3].setComboValues(fieldNames);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    for (int i = 0; i < input.getGroupFields().size(); i++) {
      TableItem item = wGroup.table.getItem(i);
      item.setText(1, Const.NVL(input.getGroupFields().get(i), ""));
    }

    for (int i = 0; i < input.getRepeats().size(); i++) {
      Repeat repeat = input.getRepeats().get(i);
      TableItem item = wRepeats.table.getItem(i);
      item.setText(1, repeat.getType() != null ? repeat.getType().getDescription() : "");
      item.setText(2, Const.NVL(repeat.getSourceField(), ""));
      item.setText(3, Const.NVL(repeat.getTargetField(), ""));
      item.setText(4, Const.NVL(repeat.getIndicatorFieldName(), ""));
      item.setText(5, Const.NVL(repeat.getIndicatorValue(), ""));
    }

    wGroup.setRowNums();
    wGroup.optWidth(true);
    wRepeats.setRowNums();
    wRepeats.optWidth(true);
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText();

    getInfo(input);

    showSortWarning();

    dispose();
  }

  private void showSortWarning() {
    if (!input.getGroupFields().isEmpty()
        && "Y".equalsIgnoreCase(props.getCustomParameter(STRING_SORT_WARNING_PARAMETER, "Y"))) {
      MessageDialogWithToggle md =
          new MessageDialogWithToggle(
              shell,
              BaseMessages.getString(PKG, "RepeatFieldsDialog.SortWarningDialog.DialogTitle"),
              BaseMessages.getString(
                      PKG, "RepeatFieldsDialog.SortWarningDialog.DialogMessage", Const.CR)
                  + Const.CR,
              SWT.ICON_WARNING,
              new String[] {
                BaseMessages.getString(PKG, "RepeatFieldsDialog.SortWarningDialog.Option1")
              },
              BaseMessages.getString(PKG, "RepeatFieldsDialog.SortWarningDialog.Option2"),
              "N".equalsIgnoreCase(props.getCustomParameter(STRING_SORT_WARNING_PARAMETER, "Y")));
      md.open();
      props.setCustomParameter(STRING_SORT_WARNING_PARAMETER, md.getToggleState() ? "N" : "Y");
    }
  }

  /** Copy the information in the widgets into the metadata */
  private void getInfo(RepeatFieldsMeta meta) {
    // The group
    meta.getGroupFields().clear();
    for (TableItem item : wGroup.getNonEmptyItems()) {
      input.getGroupFields().add(item.getText(1));
    }

    // The fields to repeat
    //
    meta.getRepeats().clear();
    for (TableItem item : wRepeats.getNonEmptyItems()) {
      Repeat repeat = new Repeat();
      repeat.setType(RepeatFieldsMeta.RepeatType.lookupDescription(item.getText(1)));
      repeat.setSourceField(item.getText(2));
      repeat.setTargetField(item.getText(3));
      repeat.setIndicatorFieldName(item.getText(4));
      repeat.setIndicatorValue(item.getText(5));
      meta.getRepeats().add(repeat);
    }
  }

  private void getGroups() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wGroup, 1, new int[] {1}, new int[] {}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"),
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"),
          ke);
    }
  }

  private void getRepeats() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wRepeats, 2, new int[] {2}, new int[] {}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"),
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"),
          ke);
    }
  }
}
