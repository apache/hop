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

package org.apache.hop.pipeline.transforms.concatfields;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/*
 * ConcatFieldsDialog
 *
 */
public class ConcatFieldsDialog extends BaseTransformDialog {
  private static final Class<?> PKG = ConcatFieldsMeta.class;

  private TextVar wTargetFieldName;

  private Text wTargetFieldLength;

  private TextVar wSeparator;

  private TextVar wEnclosure;

  private Button wRemove;

  private Button wForceEnclosure;

  private TableView wFields;

  private final ConcatFieldsMeta input;

  private ColumnInfo[] fieldColumns;

  private final List<String> inputFields = new ArrayList<>();

  public ConcatFieldsDialog(
      Shell parent,
      IVariables variables,
      ConcatFieldsMeta transformMeta,
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

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ConcatFieldsDialog.DialogTitle"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // These buttons go at the very bottom
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.TransformName.Label"));
    wlTransformName.setToolTipText(BaseMessages.getString(PKG, "System.TransformName.Tooltip"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    // TargetFieldName line
    Label wlTargetFieldName = new Label(shell, SWT.RIGHT);
    wlTargetFieldName.setText(
        BaseMessages.getString(PKG, "ConcatFieldsDialog.TargetFieldName.Label"));
    wlTargetFieldName.setToolTipText(
        BaseMessages.getString(PKG, "ConcatFieldsDialog.TargetFieldName.Tooltip"));
    PropsUi.setLook(wlTargetFieldName);
    FormData fdlTargetFieldName = new FormData();
    fdlTargetFieldName.left = new FormAttachment(0, 0);
    fdlTargetFieldName.top = new FormAttachment(lastControl, margin);
    fdlTargetFieldName.right = new FormAttachment(middle, -margin);
    wlTargetFieldName.setLayoutData(fdlTargetFieldName);
    wTargetFieldName = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTargetFieldName);
    FormData fdTargetFieldName = new FormData();
    fdTargetFieldName.left = new FormAttachment(middle, 0);
    fdTargetFieldName.top = new FormAttachment(wlTargetFieldName, 0, SWT.CENTER);
    fdTargetFieldName.right = new FormAttachment(100, 0);
    wTargetFieldName.setLayoutData(fdTargetFieldName);
    lastControl = wTargetFieldName;

    // TargetFieldLength line
    Label wlTargetFieldLength = new Label(shell, SWT.RIGHT);
    wlTargetFieldLength.setText(
        BaseMessages.getString(PKG, "ConcatFieldsDialog.TargetFieldLength.Label"));
    wlTargetFieldLength.setToolTipText(
        BaseMessages.getString(PKG, "ConcatFieldsDialog.TargetFieldLength.Tooltip"));
    PropsUi.setLook(wlTargetFieldLength);
    FormData fdlTargetFieldLength = new FormData();
    fdlTargetFieldLength.left = new FormAttachment(0, 0);
    fdlTargetFieldLength.top = new FormAttachment(wTargetFieldName, margin);
    fdlTargetFieldLength.right = new FormAttachment(middle, -margin);
    wlTargetFieldLength.setLayoutData(fdlTargetFieldLength);
    wTargetFieldLength = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTargetFieldLength);
    FormData fdTargetFieldLength = new FormData();
    fdTargetFieldLength.left = new FormAttachment(middle, 0);
    fdTargetFieldLength.top = new FormAttachment(wlTargetFieldLength, 0, SWT.CENTER);
    fdTargetFieldLength.right = new FormAttachment(100, 0);
    wTargetFieldLength.setLayoutData(fdTargetFieldLength);
    lastControl = wTargetFieldLength;

    // Separator
    Label wlSeparator = new Label(shell, SWT.RIGHT);
    wlSeparator.setText(BaseMessages.getString(PKG, "ConcatFieldsDialog.Separator.Label"));
    PropsUi.setLook(wlSeparator);
    FormData fdlSeparator = new FormData();
    fdlSeparator.left = new FormAttachment(0, 0);
    fdlSeparator.top = new FormAttachment(lastControl, margin);
    fdlSeparator.right = new FormAttachment(middle, -margin);
    wlSeparator.setLayoutData(fdlSeparator);

    Button wbSeparator = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSeparator);
    wbSeparator.setText(BaseMessages.getString(PKG, "ConcatFieldsDialog.Separator.Button"));
    FormData fdbSeparator = new FormData();
    fdbSeparator.right = new FormAttachment(100, 0);
    fdbSeparator.top = new FormAttachment(wlSeparator, 0, SWT.CENTER);
    wbSeparator.setLayoutData(fdbSeparator);
    wbSeparator.addListener(SWT.Selection, se -> wSeparator.getTextWidget().insert("\t"));

    wSeparator = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSeparator);
    FormData fdSeparator = new FormData();
    fdSeparator.left = new FormAttachment(middle, 0);
    fdSeparator.top = new FormAttachment(wlSeparator, 0, SWT.CENTER);
    fdSeparator.right = new FormAttachment(wbSeparator, -margin);
    wSeparator.setLayoutData(fdSeparator);
    lastControl = wbSeparator;

    // Enclosure line...
    Label wlEnclosure = new Label(shell, SWT.RIGHT);
    wlEnclosure.setText(BaseMessages.getString(PKG, "ConcatFieldsDialog.Enclosure.Label"));
    PropsUi.setLook(wlEnclosure);
    FormData fdlEnclosure = new FormData();
    fdlEnclosure.left = new FormAttachment(0, 0);
    fdlEnclosure.top = new FormAttachment(lastControl, margin);
    fdlEnclosure.right = new FormAttachment(middle, -margin);
    wlEnclosure.setLayoutData(fdlEnclosure);
    wEnclosure = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEnclosure);
    FormData fdEnclosure = new FormData();
    fdEnclosure.left = new FormAttachment(middle, 0);
    fdEnclosure.top = new FormAttachment(wlEnclosure, 0, SWT.CENTER);
    fdEnclosure.right = new FormAttachment(100, 0);
    wEnclosure.setLayoutData(fdEnclosure);
    lastControl = wEnclosure;

    // Force enclosure
    Label wlForceEnclosure = new Label(shell, SWT.RIGHT);
    wlForceEnclosure.setText(
        BaseMessages.getString(PKG, "ConcatFieldsDialog.ForceEnclosure.Label"));
    PropsUi.setLook(wlForceEnclosure);
    FormData fdlForceEnclosure = new FormData();
    fdlForceEnclosure.left = new FormAttachment(0, 0);
    fdlForceEnclosure.top = new FormAttachment(lastControl, margin);
    fdlForceEnclosure.right = new FormAttachment(middle, -margin);
    wlForceEnclosure.setLayoutData(fdlForceEnclosure);
    wForceEnclosure = new Button(shell, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wForceEnclosure);
    FormData fdForceEnclosure = new FormData();
    fdForceEnclosure.left = new FormAttachment(middle, 0);
    fdForceEnclosure.top = new FormAttachment(wlForceEnclosure, 0, SWT.CENTER);
    fdForceEnclosure.right = new FormAttachment(100, 0);
    wForceEnclosure.setLayoutData(fdForceEnclosure);
    lastControl = wForceEnclosure;

    // Remove concatenated fields from input...
    Label wlRemove = new Label(shell, SWT.RIGHT);
    wlRemove.setText(BaseMessages.getString(PKG, "ConcatFieldsDialog.Remove.Label"));
    PropsUi.setLook(wlRemove);
    FormData fdlRemove = new FormData();
    fdlRemove.left = new FormAttachment(0, 0);
    fdlRemove.top = new FormAttachment(lastControl, margin);
    fdlRemove.right = new FormAttachment(middle, -margin);
    wlRemove.setLayoutData(fdlRemove);
    wRemove = new Button(shell, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wRemove);
    FormData fdRemove = new FormData();
    fdRemove.left = new FormAttachment(middle, 0);
    fdRemove.top = new FormAttachment(wlRemove, 0, SWT.CENTER);
    fdRemove.right = new FormAttachment(100, 0);
    wRemove.setLayoutData(fdRemove);
    lastControl = wlRemove;

    // ////////////////////////
    // START OF TABS
    // /

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // Fields tab...
    //
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldsTab.setText(BaseMessages.getString(PKG, "ConcatFieldsDialog.FieldsTab.TabTitle"));

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = PropsUi.getFormMargin();
    fieldsLayout.marginHeight = PropsUi.getFormMargin();

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    wFieldsComp.setLayout(fieldsLayout);
    PropsUi.setLook(wFieldsComp);

    wGet = new Button(wFieldsComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));
    wGet.setToolTipText(BaseMessages.getString(PKG, "System.Tooltip.GetFields"));
    wGet.addListener(SWT.Selection, e -> get());

    Button wMinWidth = new Button(wFieldsComp, SWT.PUSH);
    wMinWidth.setText(BaseMessages.getString(PKG, "ConcatFieldsDialog.MinWidth.Button"));
    wMinWidth.setToolTipText(BaseMessages.getString(PKG, "ConcatFieldsDialog.MinWidth.Tooltip"));
    wMinWidth.addListener(SWT.Selection, e -> setMinimalWidth());

    setButtonPositions(new Button[] {wGet, wMinWidth}, margin, null);

    final int FieldsCols = 10;
    final int FieldsRows = input.getOutputFields().size();

    // Prepare a list of possible formats...
    String[] dats = Const.getDateFormats();
    String[] nums = Const.getNumberFormats();
    int totalSize = dats.length + nums.length;
    String[] formats = new String[totalSize];
    System.arraycopy(dats, 0, formats, 0, dats.length);
    System.arraycopy(nums, 0, formats, dats.length + 0, nums.length);

    fieldColumns = new ColumnInfo[FieldsCols];
    fieldColumns[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConcatFieldsDialog.NameColumn.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    fieldColumns[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConcatFieldsDialog.TypeColumn.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            ValueMetaBase.getTypes());
    fieldColumns[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConcatFieldsDialog.FormatColumn.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            formats);
    fieldColumns[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConcatFieldsDialog.LengthColumn.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    fieldColumns[4] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConcatFieldsDialog.PrecisionColumn.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    fieldColumns[5] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConcatFieldsDialog.CurrencyColumn.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    fieldColumns[6] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConcatFieldsDialog.DecimalColumn.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    fieldColumns[7] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConcatFieldsDialog.GroupColumn.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    fieldColumns[8] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConcatFieldsDialog.TrimTypeColumn.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            ValueMetaBase.trimTypeDesc,
            true);
    fieldColumns[9] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ConcatFieldsDialog.NullColumn.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    wFields =
        new TableView(
            variables,
            wFieldsComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            fieldColumns,
            FieldsRows,
            null,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wGet, -margin);
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

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment(0, 0);
    fdFieldsComp.top = new FormAttachment(0, 0);
    fdFieldsComp.right = new FormAttachment(100, 0);
    fdFieldsComp.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(lastControl, 2 * margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // Whenever something changes, set the tooltip to the expanded version:
    wTargetFieldName.addModifyListener(
        e -> wTargetFieldName.setToolTipText(variables.resolve(wTargetFieldName.getText())));

    lsResize =
        event -> {
          Point size = shell.getSize();
          wFields.setSize(size.x - 10, size.y - 50);
          wFields.table.setSize(size.x - 10, size.y - 50);
          wFields.redraw();
        };
    shell.addListener(SWT.Resize, lsResize);

    wTabFolder.setSelection(0);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Something was changed in the row. Handle this change. */
  protected void setComboBoxes() {
    // Add the currentMeta fields...
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    fieldColumns[0].setComboValues(fieldNames);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    // New concat fields
    wTransformName.setText(transformName);
    wTargetFieldName.setText(Const.NVL(input.getExtraFields().getTargetFieldName(), ""));
    wTargetFieldLength.setText("" + input.getExtraFields().getTargetFieldLength());
    wSeparator.setText(Const.NVL(input.getSeparator(), ""));
    wEnclosure.setText(Const.NVL(input.getEnclosure(), ""));
    wForceEnclosure.setSelection(input.isForceEnclosure());
    wRemove.setSelection(input.getExtraFields().isRemoveSelectedFields());

    logDebug("getting fields info...");

    for (int i = 0; i < input.getOutputFields().size(); i++) {
      ConcatField field = input.getOutputFields().get(i);

      TableItem item = wFields.table.getItem(i);
      item.setText(1, Const.NVL(field.getName(), ""));
      item.setText(2, Const.NVL(field.getType(), ""));
      item.setText(3, Const.NVL(field.getFormat(), ""));
      if (field.getLength() >= 0) {
        item.setText(4, "" + field.getLength());
      }
      if (field.getPrecision() >= 0) {
        item.setText(5, "" + field.getPrecision());
      }
      item.setText(6, Const.NVL(field.getCurrencySymbol(), ""));
      item.setText(7, Const.NVL(field.getDecimalSymbol(), ""));
      item.setText(8, Const.NVL(field.getGroupingSymbol(), ""));
      item.setText(9, Const.NVL(field.getTrimType(), ""));
      item.setText(10, Const.NVL(field.getNullString(), ""));
    }

    wFields.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  private void getInfo(ConcatFieldsMeta meta) {
    // New concat fields
    meta.getExtraFields().setTargetFieldName(wTargetFieldName.getText());
    meta.getExtraFields().setTargetFieldLength(Const.toInt(wTargetFieldLength.getText(), 0));
    meta.setSeparator(wSeparator.getText());
    meta.setEnclosure(wEnclosure.getText());
    meta.setForceEnclosure(wForceEnclosure.getSelection());
    meta.getExtraFields().setRemoveSelectedFields(wRemove.getSelection());

    input.getOutputFields().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      ConcatField field = new ConcatField();

      field.setName(item.getText(1));
      field.setType(item.getText(2));
      field.setFormat(item.getText(3));
      field.setLength(Const.toInt(item.getText(4), -1));
      field.setPrecision(Const.toInt(item.getText(5), -1));
      field.setCurrencySymbol(item.getText(6));
      field.setDecimalSymbol(item.getText(7));
      field.setGroupingSymbol(item.getText(8));
      field.setTrimType(item.getText(9));
      field.setNullString(item.getText(10));

      input.getOutputFields().add(field);
    }
  }

  private void ok() {
    if (StringUtil.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo(input);
    input.setChanged();

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        ITableItemInsertListener listener =
            (tableItem, v) -> {
              if (v.isNumber() && v.getLength() > 0) {
                int le = v.getLength();
                int pr = v.getPrecision();

                if (v.getPrecision() <= 0) {
                  pr = 0;
                }

                StringBuilder mask = new StringBuilder();
                mask.append("0".repeat(Math.max(0, le - pr)));
                if (pr > 0) {
                  mask.append(".");
                }
                mask.append("0".repeat(Math.max(0, pr)));
                tableItem.setText(3, mask.toString());
              }
              return true;
            };
        BaseTransformDialog.getFieldsFromPrevious(
            r, wFields, 1, new int[] {1}, new int[] {2}, 4, 5, listener);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"),
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"),
          ke);
    }
  }

  /** Sets the output width to minimal width... */
  public void setMinimalWidth() {
    int nrNonEmptyFields = wFields.nrNonEmpty();
    for (int i = 0; i < nrNonEmptyFields; i++) {
      TableItem item = wFields.getNonEmpty(i);

      item.setText(4, "");
      item.setText(5, "");
      item.setText(9, ValueMetaBase.getTrimTypeDesc(IValueMeta.TRIM_TYPE_BOTH));

      int type = ValueMetaBase.getType(item.getText(2));
      switch (type) {
        case IValueMeta.TYPE_STRING:
          item.setText(3, "");
          break;
        case IValueMeta.TYPE_INTEGER:
          item.setText(3, "0");
          break;
        case IValueMeta.TYPE_NUMBER:
          item.setText(3, "0.#####");
          break;
        case IValueMeta.TYPE_DATE:
          break;
        default:
          break;
      }
    }

    wFields.optWidth(true);
  }
}
