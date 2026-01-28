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

package org.apache.hop.pipeline.transforms.sort;

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
import org.apache.hop.ui.core.widget.CheckBoxVar;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class SortRowsDialog extends BaseTransformDialog {
  private static final Class<?> PKG = SortRowsMeta.class;
  public static final String CONST_SYSTEM_COMBO_YES = "System.Combo.Yes";
  public static final String CONST_SYSTEM_COMBO_PRIMARY = "System.Combo.Primary";
  public static final String CONST_SYSTEM_COMBO_NO = "System.Combo.No";

  private TextVar wSortDir;

  private Text wPrefix;

  private TextVar wSortSize;

  private TextVar wFreeMemory;

  private CheckBoxVar wCompress;

  private Button wUniqueRows;

  private TableView wFields;

  private final SortRowsMeta input;
  private final List<String> inputFields = new ArrayList<>();
  private ColumnInfo[] colinf;

  public SortRowsDialog(
      Shell parent, IVariables variables, SortRowsMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "SortRowsDialog.DialogTitle"));

    buildButtonBar().ok(e -> ok()).get(e -> get()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    // Temp directory for sorting
    Label wlSortDir = new Label(shell, SWT.RIGHT);
    wlSortDir.setText(BaseMessages.getString(PKG, "SortRowsDialog.SortDir.Label"));
    PropsUi.setLook(wlSortDir);
    FormData fdlSortDir = new FormData();
    fdlSortDir.left = new FormAttachment(0, 0);
    fdlSortDir.right = new FormAttachment(middle, -margin);
    fdlSortDir.top = new FormAttachment(wSpacer, margin);
    wlSortDir.setLayoutData(fdlSortDir);

    Button wbSortDir = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSortDir);
    wbSortDir.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbSortDir = new FormData();
    fdbSortDir.right = new FormAttachment(100, 0);
    fdbSortDir.top = new FormAttachment(wSpacer, margin);
    wbSortDir.setLayoutData(fdbSortDir);

    wSortDir = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSortDir);
    wSortDir.addModifyListener(lsMod);
    FormData fdSortDir = new FormData();
    fdSortDir.left = new FormAttachment(middle, 0);
    fdSortDir.top = new FormAttachment(wSpacer, margin);
    fdSortDir.right = new FormAttachment(wbSortDir, -margin);
    wSortDir.setLayoutData(fdSortDir);

    // Whenever something changes, set the tooltip to the expanded version:
    wSortDir.addModifyListener(e -> wSortDir.setToolTipText(variables.resolve(wSortDir.getText())));

    wbSortDir.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wSortDir, variables));

    // Prefix of temporary file
    Label wlPrefix = new Label(shell, SWT.RIGHT);
    wlPrefix.setText(BaseMessages.getString(PKG, "SortRowsDialog.Prefix.Label"));
    PropsUi.setLook(wlPrefix);
    FormData fdlPrefix = new FormData();
    fdlPrefix.left = new FormAttachment(0, 0);
    fdlPrefix.right = new FormAttachment(middle, -margin);
    fdlPrefix.top = new FormAttachment(wbSortDir, margin);
    wlPrefix.setLayoutData(fdlPrefix);
    wPrefix = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPrefix);
    wPrefix.addModifyListener(lsMod);
    FormData fdPrefix = new FormData();
    fdPrefix.left = new FormAttachment(middle, 0);
    fdPrefix.top = new FormAttachment(wbSortDir, margin);
    fdPrefix.right = new FormAttachment(100, 0);
    wPrefix.setLayoutData(fdPrefix);
    wPrefix.setText("srt");

    // Maximum number of lines to keep in memory before using temporary files
    Label wlSortSize = new Label(shell, SWT.RIGHT);
    wlSortSize.setText(BaseMessages.getString(PKG, "SortRowsDialog.SortSize.Label"));
    PropsUi.setLook(wlSortSize);
    FormData fdlSortSize = new FormData();
    fdlSortSize.left = new FormAttachment(0, 0);
    fdlSortSize.right = new FormAttachment(middle, -margin);
    fdlSortSize.top = new FormAttachment(wPrefix, margin);
    wlSortSize.setLayoutData(fdlSortSize);
    wSortSize = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSortSize);
    wSortSize.addModifyListener(lsMod);
    FormData fdSortSize = new FormData();
    fdSortSize.left = new FormAttachment(middle, 0);
    fdSortSize.top = new FormAttachment(wPrefix, margin);
    fdSortSize.right = new FormAttachment(100, 0);
    wSortSize.setLayoutData(fdSortSize);

    // Free Memory to keep
    Label wlFreeMemory = new Label(shell, SWT.RIGHT);
    wlFreeMemory.setText(BaseMessages.getString(PKG, "SortRowsDialog.FreeMemory.Label"));
    wlFreeMemory.setToolTipText(BaseMessages.getString(PKG, "SortRowsDialog.FreeMemory.ToolTip"));
    PropsUi.setLook(wlFreeMemory);
    FormData fdlFreeMemory = new FormData();
    fdlFreeMemory.left = new FormAttachment(0, 0);
    fdlFreeMemory.right = new FormAttachment(middle, -margin);
    fdlFreeMemory.top = new FormAttachment(wSortSize, margin);
    wlFreeMemory.setLayoutData(fdlFreeMemory);
    wFreeMemory = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wFreeMemory.setToolTipText(BaseMessages.getString(PKG, "SortRowsDialog.FreeMemory.ToolTip"));
    PropsUi.setLook(wFreeMemory);
    wFreeMemory.addModifyListener(lsMod);
    FormData fdFreeMemory = new FormData();
    fdFreeMemory.left = new FormAttachment(middle, 0);
    fdFreeMemory.top = new FormAttachment(wSortSize, margin);
    fdFreeMemory.right = new FormAttachment(100, 0);
    wFreeMemory.setLayoutData(fdFreeMemory);

    // Using compression for temporary files?
    Label wlCompress = new Label(shell, SWT.RIGHT);
    wlCompress.setText(BaseMessages.getString(PKG, "SortRowsDialog.Compress.Label"));
    PropsUi.setLook(wlCompress);
    FormData fdlCompress = new FormData();
    fdlCompress.left = new FormAttachment(0, 0);
    fdlCompress.right = new FormAttachment(middle, -margin);
    fdlCompress.top = new FormAttachment(wFreeMemory, margin);
    wlCompress.setLayoutData(fdlCompress);
    wCompress = new CheckBoxVar(variables, shell, SWT.CHECK, "");
    PropsUi.setLook(wCompress);
    FormData fdCompress = new FormData();
    fdCompress.left = new FormAttachment(middle, 0);
    fdCompress.top = new FormAttachment(wlCompress, 0, SWT.CENTER);
    fdCompress.right = new FormAttachment(100, 0);
    wCompress.setLayoutData(fdCompress);
    wCompress.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            log.logDetailed(
                "SortRowsDialog", "Selection Listener for compress: " + wCompress.getSelection());
            input.setChanged();
          }
        });

    // Using compression for temporary files?
    Label wlUniqueRows = new Label(shell, SWT.RIGHT);
    wlUniqueRows.setText(BaseMessages.getString(PKG, "SortRowsDialog.UniqueRows.Label"));
    PropsUi.setLook(wlUniqueRows);
    FormData fdlUniqueRows = new FormData();
    fdlUniqueRows.left = new FormAttachment(0, 0);
    fdlUniqueRows.right = new FormAttachment(middle, -margin);
    fdlUniqueRows.top = new FormAttachment(wCompress, margin);
    wlUniqueRows.setLayoutData(fdlUniqueRows);
    wUniqueRows = new Button(shell, SWT.CHECK);
    wUniqueRows.setToolTipText(BaseMessages.getString(PKG, "SortRowsDialog.UniqueRows.Tooltip"));
    PropsUi.setLook(wUniqueRows);
    FormData fdUniqueRows = new FormData();
    fdUniqueRows.left = new FormAttachment(middle, 0);
    fdUniqueRows.top = new FormAttachment(wlUniqueRows, 0, SWT.CENTER);
    fdUniqueRows.right = new FormAttachment(100, 0);
    wUniqueRows.setLayoutData(fdUniqueRows);
    wUniqueRows.addSelectionListener(new ComponentSelectionListener(input));

    // Table with fields to sort and sort direction
    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "SortRowsDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wUniqueRows, margin);
    wlFields.setLayoutData(fdlFields);

    final int FieldsRows = input.getSortFields().size();

    colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "SortRowsDialog.Fieldname.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SortRowsDialog.Ascending.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES),
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO)
              }),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SortRowsDialog.CaseInsensitive.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES),
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO)
              }),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SortRowsDialog.CollatorDisabled.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES),
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO)
              }),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SortRowsDialog.CollatorStrength.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_PRIMARY),
                BaseMessages.getString(PKG, "System.Combo.Secondary"),
                BaseMessages.getString(PKG, "System.Combo.Tertiary"),
                BaseMessages.getString(PKG, "System.Combo.Identical")
              },
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SortRowsDialog.PreSortedField.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES),
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO)
              })
        };

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
    fdFields.bottom = new FormAttachment(100, -50);
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

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    colinf[0].setComboValues(fieldNames);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input.getPrefix() != null) {
      wPrefix.setText(input.getPrefix());
    }
    if (input.getDirectory() != null) {
      wSortDir.setText(input.getDirectory());
    }
    wSortSize.setText(Const.NVL(input.getSortSize(), ""));
    wFreeMemory.setText(Const.NVL(input.getFreeMemoryLimit(), ""));
    wCompress.setSelection(input.isCompressFiles());
    wCompress.setVariableName(input.getCompressFilesVariable());
    wUniqueRows.setSelection(input.isOnlyPassingUniqueRows());

    Table table = wFields.table;
    if (!input.getSortFields().isEmpty()) {
      table.removeAll();
    }
    for (int i = 0; i < input.getSortFields().size(); i++) {
      SortRowsField field = input.getSortFields().get(i);
      TableItem ti = new TableItem(table, SWT.NONE);
      ti.setText(0, "" + (i + 1));
      ti.setText(1, field.getFieldName());
      ti.setText(
          2,
          field.isAscending()
              ? BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)
              : BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO));
      ti.setText(
          3,
          field.isCaseSensitive()
              ? BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)
              : BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO));
      ti.setText(
          4,
          field.isCollatorEnabled()
              ? BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)
              : BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO));
      ti.setText(
          5,
          field.getCollatorStrength() == 0
              ? BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_PRIMARY)
              : Integer.toString(field.getCollatorStrength()));
      ti.setText(
          6,
          field.isPreSortedField()
              ? BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)
              : BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO));
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

    // copy info to SortRowsMeta class (input)
    input.setPrefix(wPrefix.getText());
    input.setDirectory(wSortDir.getText());
    input.setSortSize(wSortSize.getText());
    input.setFreeMemoryLimit(wFreeMemory.getText());
    log.logDetailed("Sort rows", "Compression is set to " + wCompress.getSelection());
    input.setCompressFiles(wCompress.getSelection());
    input.setCompressFilesVariable(wCompress.getVariableName());
    input.setOnlyPassingUniqueRows(wUniqueRows.getSelection());

    int nrFields = wFields.nrNonEmpty();

    List<SortRowsField> fields = new ArrayList<>(nrFields);

    for (int i = 0; i < nrFields; i++) {
      TableItem ti = wFields.getNonEmpty(i);
      SortRowsField field = new SortRowsField();
      field.setFieldName(ti.getText(1));
      field.setAscending(
          Utils.isEmpty(ti.getText(2))
              || BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)
                  .equalsIgnoreCase(ti.getText(2)));
      field.setCaseSensitive(
          BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES).equalsIgnoreCase(ti.getText(3)));
      field.setCollatorEnabled(
          BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES).equalsIgnoreCase(ti.getText(4)));
      if (ti.getText(5) == "") {
        field.setCollatorStrength(
            Integer.parseInt(BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_PRIMARY)));
      } else {
        field.setCollatorStrength(Integer.parseInt(ti.getText(5)));
      }
      field.setPreSortedField(
          BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES).equalsIgnoreCase(ti.getText(6)));
      fields.add(field);
    }
    input.setSortFields(fields);
    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        ITableItemInsertListener insertListener =
            (tableItem, v) -> {
              tableItem.setText(2, BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES));
              return true;
            };
        BaseTransformDialog.getFieldsFromPrevious(
            r, wFields, 1, new int[] {1}, new int[] {}, -1, -1, insertListener);
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
