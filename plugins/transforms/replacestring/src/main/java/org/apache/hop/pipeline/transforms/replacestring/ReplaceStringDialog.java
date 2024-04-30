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

package org.apache.hop.pipeline.transforms.replacestring;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
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

/** Search and replace in string. */
public class ReplaceStringDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = ReplaceStringMeta.class; // For Translator

  private TableView wFields;

  private final ReplaceStringMeta input;

  private final List<String> inputFields = new ArrayList<>();

  private ColumnInfo[] ciKey;

  public ReplaceStringDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname) {
    super(parent, variables, (ReplaceStringMeta) in, tr, sname);
    input = (ReplaceStringMeta) in;
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
    shell.setText(BaseMessages.getString(PKG, "ReplaceStringDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "ReplaceStringDialog.TransformName.Label"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    // THE BUTTONS
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    wGet = new Button(shell, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "ReplaceStringDialog.GetFields.Button"));
    wGet.addListener(SWT.Selection, e -> get());
    setButtonPositions(new Button[] {wOk, wGet, wCancel}, margin, null);

    Label wlKey = new Label(shell, SWT.NONE);
    wlKey.setText(BaseMessages.getString(PKG, "ReplaceStringDialog.Fields.Label"));
    PropsUi.setLook(wlKey);
    FormData fdlKey = new FormData();
    fdlKey.left = new FormAttachment(0, 0);
    fdlKey.top = new FormAttachment(wTransformName, 2 * margin);
    wlKey.setLayoutData(fdlKey);

    int nrFieldCols = 10;
    int nrFieldRows = input.getFields().size();

    ciKey = new ColumnInfo[nrFieldCols];
    ciKey[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ReplaceStringDialog.ColumnInfo.InStreamField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciKey[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ReplaceStringDialog.ColumnInfo.OutStreamField"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    ciKey[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ReplaceStringDialog.ColumnInfo.useRegEx"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            BaseMessages.getString(PKG, "System.Combo.Yes"),
            BaseMessages.getString(PKG, "System.Combo.No"));
    ciKey[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ReplaceStringDialog.ColumnInfo.Replace"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    ciKey[4] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ReplaceStringDialog.ColumnInfo.By"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    ciKey[5] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ReplaceStringDialog.ColumnInfo.SetEmptyString"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            BaseMessages.getString(PKG, "System.Combo.Yes"),
            BaseMessages.getString(PKG, "System.Combo.No"));

    ciKey[6] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ReplaceStringDialog.ColumnInfo.FieldReplaceBy"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);

    ciKey[7] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ReplaceStringDialog.ColumnInfo.WholeWord"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            BaseMessages.getString(PKG, "System.Combo.Yes"),
            BaseMessages.getString(PKG, "System.Combo.No"));
    ciKey[8] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ReplaceStringDialog.ColumnInfo.CaseSensitive"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            BaseMessages.getString(PKG, "System.Combo.Yes"),
            BaseMessages.getString(PKG, "System.Combo.No"));
    ciKey[9] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ReplaceStringDialog.ColumnInfo.IsUnicode"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            BaseMessages.getString(PKG, "System.Combo.Yes"),
            BaseMessages.getString(PKG, "System.Combo.No"));

    ciKey[1].setToolTip(
        BaseMessages.getString(PKG, "ReplaceStringDialog.ColumnInfo.OutStreamField.Tooltip"));
    ciKey[1].setUsingVariables(true);
    ciKey[3].setUsingVariables(true);
    ciKey[4].setUsingVariables(true);

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciKey,
            nrFieldRows,
            lsMod,
            props);

    FormData fdKey = new FormData();
    fdKey.left = new FormAttachment(0, 0);
    fdKey.top = new FormAttachment(wlKey, margin);
    fdKey.right = new FormAttachment(100, -margin);
    fdKey.bottom = new FormAttachment(wOk, -2 * margin);
    wFields.setLayoutData(fdKey);

    //
    // Search the fields in the background
    //

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
              logError(BaseMessages.getString(PKG, "ReplaceString.Error.CanNotGetFields"));
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
    ciKey[0].setComboValues(fieldNames);
    ciKey[6].setComboValues(fieldNames);
  }

  public String toBooleanDescription(boolean b) {
    if (b) {
      return BaseMessages.getString(PKG, "System.Combo.Yes");
    } else {
      return BaseMessages.getString(PKG, "System.Combo.No");
    }
  }

  public boolean toBoolean(String description) {
    return BaseMessages.getString(PKG, "System.Combo.Yes").equalsIgnoreCase(description);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    for (int i = 0; i < input.getFields().size(); i++) {
      ReplaceStringMeta.RSField field = input.getFields().get(i);
      TableItem item = wFields.table.getItem(i);
      item.setText(1, Const.NVL(field.getFieldInStream(), ""));
      item.setText(2, Const.NVL(field.getFieldOutStream(), ""));
      item.setText(3, toBooleanDescription(field.isUsingRegEx()));
      item.setText(4, Const.NVL(field.getReplaceString(), ""));
      item.setText(5, Const.NVL(field.getReplaceByString(), ""));
      item.setText(6, toBooleanDescription(field.isSettingEmptyString()));
      item.setText(7, Const.NVL(field.getReplaceFieldByString(), ""));
      item.setText(8, toBooleanDescription(field.isReplacingWholeWord()));
      item.setText(9, toBooleanDescription(field.isCaseSensitive()));
      item.setText(10, toBooleanDescription(field.isUnicode()));
    }

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

  private void getInfo(ReplaceStringMeta m) {

    m.getFields().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      ReplaceStringMeta.RSField field = new ReplaceStringMeta.RSField();
      m.getFields().add(field);

      field.setFieldInStream(item.getText(1));
      field.setFieldOutStream(item.getText(2));
      field.setUsingRegEx(toBoolean(item.getText(3)));
      field.setReplaceString(item.getText(4));
      field.setReplaceByString(item.getText(5));
      field.setSettingEmptyString(toBoolean(item.getText(6)));
      field.setReplaceFieldByString(item.getText(7));
      field.setReplacingWholeWord(toBoolean(item.getText(8)));
      field.setCaseSensitive(toBoolean(item.getText(9)));
      field.setUnicode(toBoolean(item.getText(10)));
    }

    transformName = wTransformName.getText(); // return value
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    // Get the information for the dialog into the input structure.
    getInfo(input);

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        ITableItemInsertListener listener =
            (tableItem, v) -> {
              if (v.getType() == IValueMeta.TYPE_STRING) {
                // Only process strings
                tableItem.setText(3, BaseMessages.getString(PKG, "System.Combo.No"));
                tableItem.setText(6, BaseMessages.getString(PKG, "System.Combo.No"));
                tableItem.setText(8, BaseMessages.getString(PKG, "System.Combo.No"));
                tableItem.setText(9, BaseMessages.getString(PKG, "System.Combo.No"));
                tableItem.setText(10, BaseMessages.getString(PKG, "System.Combo.No"));
                return true;
              } else {
                return false;
              }
            };
        BaseTransformDialog.getFieldsFromPrevious(
            r, wFields, 1, new int[] {1}, new int[] {}, -1, -1, listener);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ReplaceStringDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "ReplaceStringDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }
}
