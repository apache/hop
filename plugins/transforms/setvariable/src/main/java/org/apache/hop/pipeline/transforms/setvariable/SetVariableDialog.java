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

package org.apache.hop.pipeline.transforms.setvariable;

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
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class SetVariableDialog extends BaseTransformDialog {
  private static final Class<?> PKG = SetVariableMeta.class;

  public static final String STRING_USAGE_WARNING_PARAMETER = "SetVariableUsageWarning";

  private Text wTransformName;

  private Button wFormat;

  private TableView wFields;

  private final SetVariableMeta input;

  private final List<String> inputFields = new ArrayList<>();

  private ColumnInfo[] colinf;

  public SetVariableDialog(
      Shell parent,
      IVariables variables,
      SetVariableMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "SetVariableDialog.DialogTitle"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // Some buttons at the bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wGet = new Button(shell, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));
    wGet.addListener(SWT.Selection, e -> get());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wGet, wCancel}, margin, null);

    // TransformName line
    Label wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "SetVariableDialog.TransformName.Label"));
    PropsUi.setLook(wlTransformName);
    FormData fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    FormData fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    Label wlFormat = new Label(shell, SWT.RIGHT);
    wlFormat.setText(BaseMessages.getString(PKG, "SetVariableDialog.Format.Label"));
    wlFormat.setToolTipText(BaseMessages.getString(PKG, "SetVariableDialog.Format.Tooltip"));
    PropsUi.setLook(wlFormat);
    FormData fdlFormat = new FormData();
    fdlFormat.left = new FormAttachment(0, 0);
    fdlFormat.right = new FormAttachment(middle, -margin);
    fdlFormat.top = new FormAttachment(wTransformName, margin);
    wlFormat.setLayoutData(fdlFormat);
    wFormat = new Button(shell, SWT.CHECK);
    wFormat.setToolTipText(BaseMessages.getString(PKG, "SetVariableDialog.Format.Tooltip"));
    PropsUi.setLook(wFormat);
    FormData fdFormat = new FormData();
    fdFormat.left = new FormAttachment(middle, 0);
    fdFormat.top = new FormAttachment(wlFormat, 0, SWT.CENTER);
    wFormat.setLayoutData(fdFormat);
    wFormat.addSelectionListener(new ComponentSelectionListener(input));

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "SetVariableDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wFormat, margin);
    wlFields.setLayoutData(fdlFields);

    final int FieldsRows = input.getVariables().size();
    colinf = new ColumnInfo[4];
    colinf[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SetVariableDialog.Fields.Column.FieldName"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    colinf[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SetVariableDialog.Fields.Column.VariableName"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SetVariableDialog.Fields.Column.VariableType"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            VariableItem.getVariableTypeDescriptionsList(),
            false);
    colinf[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SetVariableDialog.Fields.Column.DefaultValue"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[3].setUsingVariables(true);
    colinf[3].setToolTip(
        BaseMessages.getString(PKG, "SetVariableDialog.Fields.Column.DefaultValue.Tooltip"));

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -2 * margin);
    wFields.setLayoutData(fdFields);

    //
    // Search the fields in the background

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
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    colinf[0].setComboValues(fieldNames);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wTransformName.setText(transformName);

    for (int i = 0; i < input.getVariables().size(); i++) {
      TableItem item = wFields.table.getItem(i);
      VariableItem vi = input.getVariables().get(i);
      String src = vi.getFieldName();
      String tgt = vi.getVariableName();
      String typ = VariableItem.getVariableTypeDescription(vi.getVariableType());
      String tvv = vi.getDefaultValue();

      if (src != null) {
        item.setText(1, src);
      }
      if (tgt != null) {
        item.setText(2, tgt);
      }
      if (typ != null) {
        item.setText(3, typ);
      }
      if (tvv != null) {
        item.setText(4, tvv);
      }
    }

    wFormat.setSelection(input.isUsingFormatting());

    wFields.setRowNums();
    wFields.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
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

    int count = wFields.nrNonEmpty();

    input.getVariables().clear();
    for (int i = 0; i < count; i++) {
      TableItem item = wFields.getNonEmpty(i);

      input
          .getVariables()
          .add(
              new VariableItem(
                  item.getText(1),
                  item.getText(2),
                  VariableItem.getVariableTypeFromDesc(item.getText(3)),
                  item.getText(4)));
    }

    input.setUsingFormatting(wFormat.getSelection());

    // Show a warning (optional)
    //
    if ("Y".equalsIgnoreCase(props.getCustomParameter(STRING_USAGE_WARNING_PARAMETER, "Y"))) {
      MessageDialogWithToggle md =
          new MessageDialogWithToggle(
              shell,
              BaseMessages.getString(PKG, "SetVariableDialog.UsageWarning.DialogTitle"),
              BaseMessages.getString(PKG, "SetVariableDialog.UsageWarning.DialogMessage", Const.CR)
                  + Const.CR,
              SWT.ICON_WARNING,
              new String[] {BaseMessages.getString(PKG, "SetVariableDialog.UsageWarning.Option1")},
              BaseMessages.getString(PKG, "SetVariableDialog.UsageWarning.Option2"),
              "N".equalsIgnoreCase(props.getCustomParameter(STRING_USAGE_WARNING_PARAMETER, "Y")));
      md.open();
      props.setCustomParameter(STRING_USAGE_WARNING_PARAMETER, md.getToggleState() ? "N" : "Y");
    }

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (!Utils.isEmpty(r)) {
        BaseTransformDialog.getFieldsFromPrevious(
            r,
            wFields,
            1,
            new int[] {1},
            new int[] {},
            -1,
            -1,
            (tableItem, v) -> {
              tableItem.setText(2, v.getName().toUpperCase());
              tableItem.setText(
                  3,
                  VariableItem.getVariableTypeDescription(
                      VariableItem.VARIABLE_TYPE_ROOT_WORKFLOW));
              return true;
            });
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "SetVariableDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "Set.FailedToGetFields.DialogMessage"),
          ke);
    }
  }
}
