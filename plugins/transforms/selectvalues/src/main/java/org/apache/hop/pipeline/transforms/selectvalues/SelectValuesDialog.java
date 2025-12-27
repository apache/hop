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

package org.apache.hop.pipeline.transforms.selectvalues;

import static org.apache.hop.core.row.IValueMeta.storageTypeCodes;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class SelectValuesDialog extends BaseTransformDialog {
  private static final Class<?> PKG = SelectValuesMeta.class;
  public static final String CONST_SELECT_VALUES_DIALOG_COLUMN_INFO_LOADING =
      "SelectValuesDialog.ColumnInfo.Loading";
  public static final String CONST_SYSTEM_COMBO_YES = "System.Combo.Yes";
  public static final String CONST_SYSTEM_COMBO_NO = "System.Combo.No";
  public static final String CONST_SELECT_VALUES_DIALOG_COLUMN_INFO_FIELDNAME =
      "SelectValuesDialog.ColumnInfo.Fieldname";

  private CTabFolder wTabFolder;

  private TableView wFields;

  private Button wUnspecified;

  private TableView wRemove;

  private TableView wMeta;

  private final SelectValuesMeta input;

  private final List<ColumnInfo> fieldColumns = new ArrayList<>();

  private String[] charsets = null;

  /** Fields from previous transform */
  private IRowMeta prevFields;

  /**
   * Previous fields are read asynchonous because this might take some time and the user is able to
   * do other things, where he will not need the previous fields
   */
  private boolean bPreviousFieldsLoaded = false;

  private final Map<String, Integer> inputFields;

  public SelectValuesDialog(
      Shell parent,
      IVariables variables,
      SelectValuesMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
    inputFields = new HashMap<>();
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    SelectionListener lsSel =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "SelectValuesDialog.Shell.Label"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "SelectValuesDialog.TransformName.Label"));
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

    // Buttons go at the bottom.  The tabs in between
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    setButtonPositions(new Button[] {wOk, wCancel}, margin, null); // null means bottom of dialog

    // The folders!
    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF SELECT TAB ///
    // ////////////////////////

    CTabItem wSelectTab = new CTabItem(wTabFolder, SWT.NONE);
    wSelectTab.setFont(GuiResource.getInstance().getFontDefault());
    wSelectTab.setText(BaseMessages.getString(PKG, "SelectValuesDialog.SelectTab.TabItem"));

    Composite wSelectComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wSelectComp);

    FormLayout selectLayout = new FormLayout();
    selectLayout.marginWidth = margin;
    selectLayout.marginHeight = margin;
    wSelectComp.setLayout(selectLayout);

    Label wlUnspecified = new Label(wSelectComp, SWT.RIGHT);
    wlUnspecified.setText(BaseMessages.getString(PKG, "SelectValuesDialog.Unspecified.Label"));
    PropsUi.setLook(wlUnspecified);
    FormData fdlUnspecified = new FormData();
    fdlUnspecified.left = new FormAttachment(0, 0);
    fdlUnspecified.right = new FormAttachment(middle, 0);
    fdlUnspecified.bottom = new FormAttachment(100, 0);
    wlUnspecified.setLayoutData(fdlUnspecified);

    wUnspecified = new Button(wSelectComp, SWT.CHECK);
    PropsUi.setLook(wUnspecified);
    FormData fdUnspecified = new FormData();
    fdUnspecified.left = new FormAttachment(middle, margin);
    fdUnspecified.right = new FormAttachment(100, 0);
    fdUnspecified.bottom = new FormAttachment(wlUnspecified, 0, SWT.CENTER);
    wUnspecified.setLayoutData(fdUnspecified);
    wUnspecified.addSelectionListener(lsSel);

    Label wlFields = new Label(wSelectComp, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "SelectValuesDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(0, 0);
    wlFields.setLayoutData(fdlFields);

    final int fieldsCols = 4;
    final int fieldsRows = input.getSelectOption().getSelectFields().size();

    ColumnInfo[] colinf = new ColumnInfo[fieldsCols];
    colinf[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, CONST_SELECT_VALUES_DIALOG_COLUMN_INFO_FIELDNAME),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {
              BaseMessages.getString(PKG, CONST_SELECT_VALUES_DIALOG_COLUMN_INFO_LOADING)
            },
            false);
    colinf[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.RenameTo"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.Length"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.Precision"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    fieldColumns.add(colinf[0]);
    wFields =
        new TableView(
            variables,
            wSelectComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            fieldsRows,
            lsMod,
            props);

    Button wGetSelect = new Button(wSelectComp, SWT.PUSH);
    wGetSelect.setText(BaseMessages.getString(PKG, "SelectValuesDialog.GetSelect.Button"));
    wGetSelect.addListener(SWT.Selection, e -> get());
    FormData fdGetSelect = new FormData();
    fdGetSelect.right = new FormAttachment(100, 0);
    // vertically center the button relative to the table (wFields)
    fdGetSelect.top = new FormAttachment(wFields, 0, SWT.CENTER);
    wGetSelect.setLayoutData(fdGetSelect);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(wGetSelect, -margin);
    fdFields.bottom = new FormAttachment(wlUnspecified, -2 * margin);
    wFields.setLayoutData(fdFields);

    FormData fdSelectComp = new FormData();
    fdSelectComp.left = new FormAttachment(0, 0);
    fdSelectComp.top = new FormAttachment(0, 0);
    fdSelectComp.right = new FormAttachment(100, 0);
    fdSelectComp.bottom = new FormAttachment(100, 0);
    wSelectComp.setLayoutData(fdSelectComp);

    wSelectComp.layout();
    wSelectTab.setControl(wSelectComp);

    // ///////////////////////////////////////////////////////////
    // / END OF SELECT TAB
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////////////////////////////////
    // START OF REMOVE TAB
    // ///////////////////////////////////////////////////////////
    CTabItem wRemoveTab = new CTabItem(wTabFolder, SWT.NONE);
    wRemoveTab.setFont(GuiResource.getInstance().getFontDefault());
    wRemoveTab.setText(BaseMessages.getString(PKG, "SelectValuesDialog.RemoveTab.TabItem"));

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = margin;
    contentLayout.marginHeight = margin;

    Composite wRemoveComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wRemoveComp);
    wRemoveComp.setLayout(contentLayout);

    Label wlRemove = new Label(wRemoveComp, SWT.NONE);
    wlRemove.setText(BaseMessages.getString(PKG, "SelectValuesDialog.Remove.Label"));
    PropsUi.setLook(wlRemove);
    FormData fdlRemove = new FormData();
    fdlRemove.left = new FormAttachment(0, 0);
    fdlRemove.top = new FormAttachment(0, 0);
    wlRemove.setLayoutData(fdlRemove);

    final int RemoveCols = 1;
    final int RemoveRows = input.getSelectOption().getDeleteName().size();

    ColumnInfo[] colrem = new ColumnInfo[RemoveCols];
    colrem[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, CONST_SELECT_VALUES_DIALOG_COLUMN_INFO_FIELDNAME),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {
              BaseMessages.getString(PKG, CONST_SELECT_VALUES_DIALOG_COLUMN_INFO_LOADING)
            },
            false);
    fieldColumns.add(colrem[0]);
    wRemove =
        new TableView(
            variables,
            wRemoveComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colrem,
            RemoveRows,
            lsMod,
            props);

    Button wGetRemove = new Button(wRemoveComp, SWT.PUSH);
    wGetRemove.setText(BaseMessages.getString(PKG, "SelectValuesDialog.GetRemove.Button"));
    wGetRemove.addListener(SWT.Selection, e -> get());
    FormData fdGetRemove = new FormData();
    fdGetRemove.right = new FormAttachment(100, 0);
    fdGetRemove.top = new FormAttachment(50, 0);
    wGetRemove.setLayoutData(fdGetRemove);

    FormData fdRemove = new FormData();
    fdRemove.left = new FormAttachment(0, 0);
    fdRemove.top = new FormAttachment(wlRemove, margin);
    fdRemove.right = new FormAttachment(wGetRemove, -margin);
    fdRemove.bottom = new FormAttachment(100, 0);
    wRemove.setLayoutData(fdRemove);

    FormData fdRemoveComp = new FormData();
    fdRemoveComp.left = new FormAttachment(0, 0);
    fdRemoveComp.top = new FormAttachment(0, 0);
    fdRemoveComp.right = new FormAttachment(100, 0);
    fdRemoveComp.bottom = new FormAttachment(100, 0);
    wRemoveComp.setLayoutData(fdRemoveComp);

    wRemoveComp.layout();
    wRemoveTab.setControl(wRemoveComp);

    // ///////////////////////////////////////////////////////////
    // / END OF REMOVE TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF META TAB ///
    // ////////////////////////

    CTabItem wMetaTab = new CTabItem(wTabFolder, SWT.NONE);
    wMetaTab.setFont(GuiResource.getInstance().getFontDefault());
    wMetaTab.setText(BaseMessages.getString(PKG, "SelectValuesDialog.MetaTab.TabItem"));

    Composite wMetaComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wMetaComp);

    FormLayout metaLayout = new FormLayout();
    metaLayout.marginWidth = margin;
    metaLayout.marginHeight = margin;
    wMetaComp.setLayout(metaLayout);

    Label wlMeta = new Label(wMetaComp, SWT.NONE);
    wlMeta.setText(BaseMessages.getString(PKG, "SelectValuesDialog.Meta.Label"));
    PropsUi.setLook(wlMeta);
    FormData fdlMeta = new FormData();
    fdlMeta.left = new FormAttachment(0, 0);
    fdlMeta.top = new FormAttachment(0, 0);
    wlMeta.setLayoutData(fdlMeta);

    final int MetaRows = input.getSelectOption().getMeta().size();

    ColumnInfo[] colmeta =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, CONST_SELECT_VALUES_DIALOG_COLUMN_INFO_FIELDNAME),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, CONST_SELECT_VALUES_DIALOG_COLUMN_INFO_LOADING)
              },
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.Renameto"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getAllValueMetaNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.Length"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.Precision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.Storage.Label"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES),
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO),
              }),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.Format"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              3),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.DateLenient"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES),
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO),
              }),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.DateFormatLocale"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              EnvUtil.getLocaleList()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.DateFormatTimeZone"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              EnvUtil.getTimeZones()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.LenientStringToNumber"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES),
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO),
              }),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.Encoding"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              getCharsets(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.Decimal"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.Grouping"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.Currency"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.RoundingType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaNumber.roundingTypeDesc),
        };
    colmeta[5].setToolTip(
        BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.Storage.Tooltip"));
    fieldColumns.add(colmeta[0]);
    wMeta =
        new TableView(
            variables,
            wMetaComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colmeta,
            MetaRows,
            lsMod,
            props);

    Button wGetMeta = new Button(wMetaComp, SWT.PUSH);
    wGetMeta.setText(BaseMessages.getString(PKG, "SelectValuesDialog.GetMeta.Button"));
    wGetMeta.addListener(SWT.Selection, e -> get());
    FormData fdGetMeta = new FormData();
    fdGetMeta.right = new FormAttachment(100, 0);
    fdGetMeta.top = new FormAttachment(50, 0);
    wGetMeta.setLayoutData(fdGetMeta);

    FormData fdMeta = new FormData();
    fdMeta.left = new FormAttachment(0, 0);
    fdMeta.top = new FormAttachment(wlMeta, margin);
    fdMeta.right = new FormAttachment(wGetMeta, -margin);
    fdMeta.bottom = new FormAttachment(100, 0);
    wMeta.setLayoutData(fdMeta);

    FormData fdMetaComp = new FormData();
    fdMetaComp.left = new FormAttachment(0, 0);
    fdMetaComp.top = new FormAttachment(0, 0);
    fdMetaComp.right = new FormAttachment(100, 0);
    fdMetaComp.bottom = new FormAttachment(100, 0);
    wMetaComp.setLayoutData(fdMetaComp);

    wMetaComp.layout();
    wMetaTab.setControl(wMetaComp);

    // ///////////////////////////////////////////////////////////
    // / END OF META TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // ///////////////////////////////////////////////////////////
    // / END OF TAB FOLDER
    // ///////////////////////////////////////////////////////////

    //
    // Search the fields in the background
    //
    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);
              prevFields = row;
              // Remember these fields...
              for (int i = 0; i < row.size(); i++) {
                inputFields.put(row.getValueMeta(i).getName(), i);
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
    setComboValues();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void setComboValues() {
    Runnable fieldLoader =
        () -> {
          try {
            prevFields = pipelineMeta.getPrevTransformFields(variables, transformName);
          } catch (HopException e) {
            prevFields = new RowMeta();
            String msg =
                BaseMessages.getString(PKG, "SelectValuesDialog.DoMapping.UnableToFindInput");
            logError(msg);
          }
          String[] prevTransformFieldNames =
              prevFields != null ? prevFields.getFieldNames() : new String[0];
          Arrays.sort(prevTransformFieldNames);
          bPreviousFieldsLoaded = true;
          for (ColumnInfo colInfo : fieldColumns) {
            colInfo.setComboValues(prevTransformFieldNames);
          }
        };
    shell.getDisplay().asyncExec(fieldLoader);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wTabFolder.setSelection(0); // Default

    /*
     * Select fields
     */
    if (input.getSelectOption().getSelectFields() != null
        && !input.getSelectOption().getSelectFields().isEmpty()) {
      for (int i = 0; i < input.getSelectOption().getSelectFields().size(); i++) {
        SelectField selectField = input.getSelectOption().getSelectFields().get(i);
        TableItem item = wFields.table.getItem(i);
        if (selectField.getName() != null) {
          item.setText(1, selectField.getName());
        }
        if (selectField.getRename() != null
            && !selectField.getRename().equals(selectField.getName())) {
          item.setText(2, selectField.getRename());
        }
        item.setText(3, selectField.getLength() < 0 ? "" : "" + selectField.getLength());
        item.setText(4, selectField.getPrecision() < 0 ? "" : "" + selectField.getPrecision());
      }
      wFields.setRowNums();
      wFields.optWidth(true);
      wTabFolder.setSelection(0);
    }
    wUnspecified.setSelection(input.getSelectOption().isSelectingAndSortingUnspecifiedFields());

    /*
     * Remove certain fields...
     */
    if (input.getSelectOption().getDeleteName() != null
        && !input.getSelectOption().getDeleteName().isEmpty()) {
      for (int i = 0; i < input.getSelectOption().getDeleteName().size(); i++) {
        DeleteField deleteName = input.getSelectOption().getDeleteName().get(i);
        TableItem item = wRemove.table.getItem(i);
        if (deleteName != null) {
          item.setText(1, deleteName.getName());
        }
      }
      wRemove.setRowNums();
      wRemove.optWidth(true);
      wTabFolder.setSelection(1);
    }

    /*
     * Change the meta-data of certain fields
     */
    if (!Utils.isEmpty(input.getSelectOption().getMeta())) {
      for (int i = 0; i < input.getSelectOption().getMeta().size(); i++) {
        SelectMetadataChange change = input.getSelectOption().getMeta().get(i);

        TableItem item = wMeta.table.getItem(i);
        int index = 1;
        item.setText(index++, Const.NVL(change.getName(), ""));
        if (change.getRename() != null && !change.getRename().equals(change.getName())) {
          item.setText(index++, change.getRename());
        } else {
          index++;
        }
        item.setText(index++, change.getType() != null ? change.getType() : "-");
        item.setText(index++, change.getLength() < 0 ? "" : "" + change.getLength());
        item.setText(index++, change.getPrecision() < 0 ? "" : "" + change.getPrecision());
        item.setText(
            index++,
            change.getStorageType().equals(storageTypeCodes[IValueMeta.STORAGE_TYPE_NORMAL])
                ? BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)
                : BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO));
        item.setText(index++, Const.NVL(change.getConversionMask(), ""));
        item.setText(
            index++,
            change.isDateFormatLenient()
                ? BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)
                : BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO));
        item.setText(
            index++,
            change.getDateFormatLocale() == null ? "" : change.getDateFormatLocale().toString());
        item.setText(
            index++,
            change.getDateFormatTimeZone() == null
                ? ""
                : change.getDateFormatTimeZone().toString());
        item.setText(
            index++,
            change.isLenientStringToNumber()
                ? BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)
                : BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO));
        item.setText(index++, Const.NVL(change.getEncoding(), ""));
        item.setText(index++, Const.NVL(change.getDecimalSymbol(), ""));
        item.setText(index++, Const.NVL(change.getGroupingSymbol(), ""));
        item.setText(index++, Const.NVL(change.getCurrencySymbol(), ""));
        // Prevent setting the default to Half Even.
        if (StringUtils.isNotEmpty(change.getRoundingType())) {
          item.setText(
              index, Const.NVL(ValueMetaBase.getRoundingTypeDesc(change.getRoundingType()), ""));
        }
      }
      wMeta.setRowNums();
      wMeta.optWidth(true);
      wTabFolder.setSelection(2);
    }

    wTransformName.setFocus();
    wTransformName.selectAll();
  }

  private String[] getCharsets() {
    if (charsets == null) {
      Collection<Charset> charsetCol = Charset.availableCharsets().values();
      charsets = new String[charsetCol.size()];
      int i = 0;
      for (Charset charset : charsetCol) {
        charsets[i++] = charset.displayName();
      }
    }
    return charsets;
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

    // copy info to meta class (input)

    int nrFields = wFields.nrNonEmpty();
    int nrremove = wRemove.nrNonEmpty();
    int nrmeta = wMeta.nrNonEmpty();

    resetSelectOptions();
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wFields.getNonEmpty(i);

      var currentSelectFieldItem = new SelectField();
      currentSelectFieldItem.setName(item.getText(1));
      currentSelectFieldItem.setRename(item.getText(2));
      if (currentSelectFieldItem.getRename() == null
          || currentSelectFieldItem.getName().isEmpty()) {
        currentSelectFieldItem.setRename(currentSelectFieldItem.getName());
      }
      currentSelectFieldItem.setLength(Const.toInt(item.getText(3), -2));
      currentSelectFieldItem.setPrecision(Const.toInt(item.getText(4), -2));

      if (currentSelectFieldItem.getLength() < -2) {
        currentSelectFieldItem.setLength(-2);
      }
      if (currentSelectFieldItem.getPrecision() < -2) {
        currentSelectFieldItem.setPrecision(-2);
      }
      input.getSelectOption().getSelectFields().add(currentSelectFieldItem);
    }
    input.getSelectOption().setSelectingAndSortingUnspecifiedFields(wUnspecified.getSelection());

    for (int i = 0; i < nrremove; i++) {
      TableItem item = wRemove.getNonEmpty(i);
      var currentItemToDelete = new DeleteField();
      currentItemToDelete.setName(item.getText(1));
      input.getSelectOption().getDeleteName().add(currentItemToDelete);
    }

    for (int i = 0; i < nrmeta; i++) {
      SelectMetadataChange change = new SelectMetadataChange();

      TableItem item = wMeta.getNonEmpty(i);

      int index = 1;
      change.setName(item.getText(index++));
      change.setRename(item.getText(index++));
      if (Utils.isEmpty(change.getRename())) {
        change.setRename(change.getName());
      }
      change.setType(item.getText(index++));

      change.setLength(Const.toInt(item.getText(index++), -2));
      change.setPrecision(Const.toInt(item.getText(index++), -2));

      if (change.getLength() < -2) {
        change.setLength(-2);
      }
      if (change.getPrecision() < -2) {
        change.setPrecision(-2);
      }
      if (BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)
          .equalsIgnoreCase(item.getText(index++))) {
        change.setStorageType(storageTypeCodes[IValueMeta.STORAGE_TYPE_NORMAL]);
      }

      change.setConversionMask(item.getText(index++));
      // If DateFormatLenient is anything but Yes (including blank) then it is false
      change.setDateFormatLenient(
          item.getText(index++)
                  .equalsIgnoreCase(BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES))
              ? true
              : false);
      change.setDateFormatLocale(item.getText(index++));
      change.setDateFormatTimeZone(item.getText(index++));
      change.setLenientStringToNumber(
          item.getText(index++)
                  .equalsIgnoreCase(BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES))
              ? true
              : false);
      change.setEncoding(item.getText(index++));
      change.setDecimalSymbol(item.getText(index++));
      change.setGroupingSymbol(item.getText(index++));
      change.setCurrencySymbol(item.getText(index++));
      change.setRoundingType(ValueMetaBase.getRoundingTypeCode(item.getText(index)));

      input.getSelectOption().getMeta().add(change);
    }
    dispose();
  }

  private void resetSelectOptions() {
    input.getSelectOption().getDeleteName().clear();
    input.getSelectOption().getSelectFields().clear();
    input.getSelectOption().getMeta().clear();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        switch (wTabFolder.getSelectionIndex()) {
          case 0:
            BaseTransformDialog.getFieldsFromPrevious(
                r, wFields, 1, new int[] {1}, new int[] {}, -1, -1, null);
            break;
          case 1:
            BaseTransformDialog.getFieldsFromPrevious(
                r, wRemove, 1, new int[] {1}, new int[] {}, -1, -1, null);
            break;
          case 2:
            BaseTransformDialog.getFieldsFromPrevious(
                r, wMeta, 1, new int[] {1}, new int[] {3}, 4, 5, null);
            break;
          default:
            break;
        }
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "SelectValuesDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "SelectValuesDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll(inputFields);

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>(keySet);

    String[] fieldNames = entries.toArray(new String[entries.size()]);

    if (PropsUi.getInstance().isSortFieldByName()) {
      Const.sortStrings(fieldNames);
    }

    bPreviousFieldsLoaded = true;
    for (ColumnInfo colInfo : fieldColumns) {
      colInfo.setComboValues(fieldNames);
    }
  }
}
