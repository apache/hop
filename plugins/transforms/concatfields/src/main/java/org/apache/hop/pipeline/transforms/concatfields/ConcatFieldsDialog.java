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
import org.apache.hop.ui.core.FormDataBuilder;
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
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Point;
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

  /** enclosure label / text */
  private Label wlEnclosure;

  private Button wForceEnclosure;

  /** skip empty value(empty/null) */
  private Button skipEmptyBtn;

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
    wlTransformName.setLayoutData(
        FormDataBuilder.builder().top(0, margin).left().right(middle, -margin).build());
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTransformName);
    wTransformName.setLayoutData(
        FormDataBuilder.builder()
            .top(wlTransformName, 0, SWT.CENTER)
            .left(middle, 0)
            .right(100, 0)
            .build());
    Control lastControl = wTransformName;

    // TargetFieldName line
    Label wlTargetFieldName = new Label(shell, SWT.RIGHT);
    wlTargetFieldName.setText(
        BaseMessages.getString(PKG, "ConcatFieldsDialog.TargetFieldName.Label"));
    wlTargetFieldName.setToolTipText(
        BaseMessages.getString(PKG, "ConcatFieldsDialog.TargetFieldName.Tooltip"));
    PropsUi.setLook(wlTargetFieldName);
    wlTargetFieldName.setLayoutData(
        FormDataBuilder.builder().top(lastControl, margin).left().right(middle, -margin).build());
    wTargetFieldName = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTargetFieldName);
    wTargetFieldName.setLayoutData(
        FormDataBuilder.builder()
            .top(wlTargetFieldName, 0, SWT.CENTER)
            .left(middle, 0)
            .right(100, 0)
            .build());

    // TargetFieldLength line
    Label wlTargetFieldLength = new Label(shell, SWT.RIGHT);
    wlTargetFieldLength.setText(
        BaseMessages.getString(PKG, "ConcatFieldsDialog.TargetFieldLength.Label"));
    wlTargetFieldLength.setToolTipText(
        BaseMessages.getString(PKG, "ConcatFieldsDialog.TargetFieldLength.Tooltip"));
    PropsUi.setLook(wlTargetFieldLength);
    wlTargetFieldLength.setLayoutData(
        FormDataBuilder.builder()
            .top(wTargetFieldName, margin)
            .left()
            .right(middle, -margin)
            .build());
    wTargetFieldLength = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTargetFieldLength);
    wTargetFieldLength.setLayoutData(
        FormDataBuilder.builder()
            .top(wlTargetFieldLength, 0, SWT.CENTER)
            .left(middle, 0)
            .right(100, 0)
            .build());
    lastControl = wTargetFieldLength;

    // Separator
    Label wlSeparator = new Label(shell, SWT.RIGHT);
    wlSeparator.setText(BaseMessages.getString(PKG, "ConcatFieldsDialog.Separator.Label"));
    PropsUi.setLook(wlSeparator);
    wlSeparator.setLayoutData(
        FormDataBuilder.builder().top(lastControl, margin).left().right(middle, -margin).build());

    Button wbSeparator = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSeparator);
    wbSeparator.setText(BaseMessages.getString(PKG, "ConcatFieldsDialog.Separator.Button"));
    wbSeparator.setLayoutData(
        FormDataBuilder.builder().top(wlSeparator, 0, SWT.CENTER).right(100, 0).build());
    wbSeparator.addListener(SWT.Selection, se -> wSeparator.getTextWidget().insert("\t"));

    wSeparator = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSeparator);
    wSeparator.setLayoutData(
        FormDataBuilder.builder()
            .top(wlSeparator, 0, SWT.CENTER)
            .left(middle, 0)
            .right(wbSeparator, -margin)
            .build());
    lastControl = wbSeparator;

    // Force enclosure
    Label wlForceEnclosure = new Label(shell, SWT.RIGHT);
    wlForceEnclosure.setText(
        BaseMessages.getString(PKG, "ConcatFieldsDialog.ForceEnclosure.Label"));
    PropsUi.setLook(wlForceEnclosure);
    wlForceEnclosure.setLayoutData(
        FormDataBuilder.builder().left().top(lastControl, margin).right(middle, -margin).build());
    wForceEnclosure = new Button(shell, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wForceEnclosure);
    wForceEnclosure.setLayoutData(
        FormDataBuilder.builder().left(middle, 0).top(wlForceEnclosure, 0, SWT.CENTER).build());
    lastControl = wForceEnclosure;
    wForceEnclosure.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            activateEnclosure();
          }
        });

    // Enclosure line...
    wlEnclosure = new Label(shell, SWT.RIGHT);
    wlEnclosure.setText(BaseMessages.getString(PKG, "ConcatFieldsDialog.Enclosure.Label"));
    PropsUi.setLook(wlEnclosure);
    wlEnclosure.setLayoutData(
        FormDataBuilder.builder()
            .left(wForceEnclosure, margin)
            .top(wSeparator, margin)
            .width(180)
            .build());
    wEnclosure = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEnclosure);
    wEnclosure.setLayoutData(
        FormDataBuilder.builder()
            .left(wlEnclosure, margin)
            .top(wSeparator, margin)
            .right(100, 0)
            .build());

    // skip null/empty
    Label skipEmptyLabel = new Label(shell, SWT.RIGHT);
    skipEmptyLabel.setText(BaseMessages.getString(PKG, "ConcatFieldsDialog.SkipEmpty.Label"));
    PropsUi.setLook(skipEmptyLabel);
    skipEmptyLabel.setLayoutData(
        FormDataBuilder.builder().left().top(lastControl, margin).right(middle, -margin).build());
    skipEmptyBtn = new Button(shell, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(skipEmptyBtn);
    skipEmptyBtn.setLayoutData(
        FormDataBuilder.builder()
            .left(middle, 0)
            .top(skipEmptyLabel, 0, SWT.CENTER)
            .right(100, 0)
            .build());
    lastControl = skipEmptyLabel;

    // Remove concatenated fields from input...
    Label wlRemove = new Label(shell, SWT.RIGHT);
    wlRemove.setText(BaseMessages.getString(PKG, "ConcatFieldsDialog.Remove.Label"));
    PropsUi.setLook(wlRemove);
    wlRemove.setLayoutData(
        FormDataBuilder.builder().top(lastControl, margin).left().right(middle, -margin).build());
    wRemove = new Button(shell, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wRemove);
    wRemove.setLayoutData(
        FormDataBuilder.builder()
            .top(wlRemove, 0, SWT.CENTER)
            .left(middle, 0)
            .right(100, 0)
            .build());
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
    System.arraycopy(nums, 0, formats, dats.length, nums.length);

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
    wFields.setLayoutData(
        FormDataBuilder.builder().top().left().right(100, 0).bottom(wGet, -margin).build());

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
    wFieldsComp.setLayoutData(
        FormDataBuilder.builder().top().left().right(100, 0).bottom(100, 0).build());

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);
    wTabFolder.setLayoutData(
        FormDataBuilder.builder()
            .top(lastControl, 2 * margin)
            .left()
            .right(100, 0)
            .bottom(wOk, -2 * margin)
            .build());

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

    activateEnclosure();
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
    skipEmptyBtn.setSelection(input.isSkipValueEmpty());

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
    meta.setSkipValueEmpty(skipEmptyBtn.getSelection());
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
    // return value
    transformName = wTransformName.getText();

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

  private void activateEnclosure() {
    wlEnclosure.setEnabled(wForceEnclosure.getSelection());
    wEnclosure.setEnabled(wForceEnclosure.getSelection());
  }
}
