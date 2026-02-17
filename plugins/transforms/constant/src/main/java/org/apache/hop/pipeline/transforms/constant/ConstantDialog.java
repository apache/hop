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

package org.apache.hop.pipeline.transforms.constant;

import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class ConstantDialog extends BaseTransformDialog {
  private static final Class<?> PKG = ConstantMeta.class;

  private static final String SYSTEM_COMBO_YES = "System.Combo.Yes";

  private TableView wFields;

  private final ConstantMeta input;

  public ConstantDialog(
      Shell parent, IVariables variables, ConstantMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "ConstantDialog.DialogTitle"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "ConstantDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wSpacer, margin);
    wlFields.setLayoutData(fdlFields);

    final int FieldsCols = 10;
    final int FieldsRows = input.getFields().size();

    ColumnInfo[] colinf = new ColumnInfo[FieldsCols];
    colinf[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConstantDialog.Name.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConstantDialog.Type.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            ValueMetaFactory.getValueMetaNames());
    colinf[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConstantDialog.Format.Column"),
            ColumnInfo.COLUMN_TYPE_FORMAT,
            2);
    colinf[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConstantDialog.Length.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[4] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConstantDialog.Precision.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[5] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConstantDialog.Currency.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[6] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConstantDialog.Decimal.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[7] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConstantDialog.Group.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[8] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConstantDialog.Value.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[9] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConstantDialog.Value.SetEmptyString"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {
              BaseMessages.getString(PKG, SYSTEM_COMBO_YES),
              BaseMessages.getString(PKG, "System.Combo.No")
            });

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
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    int i;
    if (log.isDebug()) {
      logDebug("getting fields info...");
    }

    for (i = 0; i < input.getFields().size(); i++) {
      ConstantField field = input.fields.get(i);
      if (!StringUtils.isEmpty(field.getFieldName())) {
        TableItem item = wFields.table.getItem(i);
        int col = 1;
        item.setText(col++, field.getFieldName());

        String type = field.getFieldType();
        String format = field.getFieldFormat();
        String length = field.getFieldLength() < 0 ? "" : ("" + field.getFieldLength());
        String prec = field.getFieldPrecision() < 0 ? "" : ("" + field.getFieldPrecision());

        String curr = field.getCurrency();
        String group = field.getGroup();
        String decim = field.getDecimal();
        String def = field.getValue();

        item.setText(col++, Const.NVL(type, ""));
        item.setText(col++, Const.NVL(format, ""));
        item.setText(col++, Const.NVL(length, ""));
        item.setText(col++, Const.NVL(prec, ""));
        item.setText(col++, Const.NVL(curr, ""));
        item.setText(col++, Const.NVL(decim, ""));
        item.setText(col++, Const.NVL(group, ""));
        item.setText(col++, Const.NVL(def, ""));
        item.setText(
            col,
            field.isEmptyString()
                ? BaseMessages.getString(PKG, SYSTEM_COMBO_YES)
                : BaseMessages.getString(PKG, "System.Combo.No"));
      }
    }

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

    int i;

    int nrFields = wFields.nrNonEmpty();
    List<ConstantField> fields = input.getFields();
    fields.clear();

    for (i = 0; i < nrFields; i++) {
      TableItem item = wFields.getNonEmpty(i);
      boolean isEmptyStringFlag =
          BaseMessages.getString(PKG, SYSTEM_COMBO_YES).equalsIgnoreCase(item.getText(10));
      ConstantField field =
          (isEmptyStringFlag
              ? new ConstantField(
                  item.getText(1),
                  isEmptyStringFlag ? "String" : item.getText(2),
                  isEmptyStringFlag)
              : new ConstantField(
                  item.getText(1),
                  isEmptyStringFlag ? "String" : item.getText(2),
                  isEmptyStringFlag ? "" : item.getText(9)));

      field.setFieldFormat(item.getText(3));
      String slength = item.getText(4);
      String sprec = item.getText(5);
      field.setCurrency(item.getText(6));
      field.setDecimal(item.getText(7));
      field.setGroup(item.getText(8));

      try {
        field.setFieldLength(Integer.parseInt(slength));
      } catch (Exception e) {
        field.setFieldLength(-1);
      }
      try {
        field.setFieldPrecision(Integer.parseInt(sprec));
      } catch (Exception e) {
        field.setFieldPrecision(-1);
      }

      fields.add(field);
    }

    dispose();
  }
}
