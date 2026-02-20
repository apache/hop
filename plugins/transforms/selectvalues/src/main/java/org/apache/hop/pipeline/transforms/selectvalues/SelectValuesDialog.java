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
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.FormDataBuilder;
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

  // Separate lists for different tabs to show appropriate fields at each stage
  private ColumnInfo selectFieldColumn; // Select & Alter: shows original input fields
  private final List<ColumnInfo> removeFieldColumns =
      new ArrayList<>(); // Remove: shows output of Select & Alter
  private final List<ColumnInfo> metaFieldColumns =
      new ArrayList<>(); // Metadata: shows output of Select & Alter minus Remove

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
    createShell(BaseMessages.getString(PKG, "SelectValuesDialog.Shell.Label"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    SelectionListener lsSel =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

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
    wlUnspecified.setLayoutData(FormDataBuilder.builder().left().bottom(100, 0).build());

    wUnspecified = new Button(wSelectComp, SWT.CHECK);
    PropsUi.setLook(wUnspecified);
    wUnspecified.setLayoutData(
        FormDataBuilder.builder().left(wlUnspecified, margin).bottom(100, 5).build());
    wUnspecified.addSelectionListener(lsSel);
    // Update combo boxes when "Include unspecified fields" checkbox is toggled
    wUnspecified.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            if (bPreviousFieldsLoaded) {
              shell
                  .getDisplay()
                  .asyncExec(
                      () -> {
                        if (!shell.isDisposed() && bPreviousFieldsLoaded) {
                          setComboBoxes();
                        }
                      });
            }
          }
        });
    // Label: select & Alter.  Button: Get fields to select
    Label wlFields = new Label(wSelectComp, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "SelectValuesDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    wlFields.setLayoutData(FormDataBuilder.builder().top().left().build());

    Button wGetSelect = new Button(wSelectComp, SWT.PUSH);
    wGetSelect.setText(BaseMessages.getString(PKG, "SelectValuesDialog.GetSelect.Button"));
    wGetSelect.addListener(SWT.Selection, e -> get());
    wGetSelect.setLayoutData(FormDataBuilder.builder().top().right().build());

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
    selectFieldColumn =
        colinf[0]; // Save reference to Select & Alter column (should always show original fields)
    wFields =
        new TableView(
            variables,
            wSelectComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            fieldsRows,
            lsMod,
            props);

    wFields.setLayoutData(
        FormDataBuilder.builder()
            .top(wlFields, margin)
            .left()
            .right()
            .bottom(wlUnspecified, -2 * margin)
            .build());
    wSelectComp.setLayoutData(
        FormDataBuilder.builder().top().left().right().bottom(100, 0).build());

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

    Button wGetRemove = new Button(wRemoveComp, SWT.PUSH);
    wGetRemove.setText(BaseMessages.getString(PKG, "SelectValuesDialog.GetRemove.Button"));
    wGetRemove.addListener(SWT.Selection, e -> get());
    setButtonPositions(new Button[] {wGetRemove}, margin, null);

    Label wlRemove = new Label(wRemoveComp, SWT.NONE);
    wlRemove.setText(BaseMessages.getString(PKG, "SelectValuesDialog.Remove.Label"));
    PropsUi.setLook(wlRemove);
    wlRemove.setLayoutData(FormDataBuilder.builder().top().left().build());

    Button wGetRemove = new Button(wRemoveComp, SWT.PUSH);
    wGetRemove.setText(BaseMessages.getString(PKG, "SelectValuesDialog.GetRemove.Button"));
    wGetRemove.addListener(SWT.Selection, e -> get());
    wGetRemove.setLayoutData(FormDataBuilder.builder().top().right().build());

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
    removeFieldColumns.add(colrem[0]); // Remove tab should show renamed fields from Select & Alter
    wRemove =
        new TableView(
            variables,
            wRemoveComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colrem,
            RemoveRows,
            lsMod,
            props);
    wRemove.setLayoutData(
        FormDataBuilder.builder().top(wlRemove, margin).left().right().bottom(100, 0).build());
    wRemoveComp.setLayoutData(FormDataBuilder.builder().top().left().bottom(100, 0).build());

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

    Button wGetMeta = new Button(wMetaComp, SWT.PUSH);
    wGetMeta.setText(BaseMessages.getString(PKG, "SelectValuesDialog.GetMeta.Button"));
    wGetMeta.addListener(SWT.Selection, e -> get());
    setButtonPositions(new Button[] {wGetMeta}, margin, null);

    Label wlMeta = new Label(wMetaComp, SWT.NONE);
    wlMeta.setText(BaseMessages.getString(PKG, "SelectValuesDialog.Meta.Label"));
    PropsUi.setLook(wlMeta);
    wlMeta.setLayoutData(FormDataBuilder.builder().top().left().build());

    Button wGetMeta = new Button(wMetaComp, SWT.PUSH);
    wGetMeta.setText(BaseMessages.getString(PKG, "SelectValuesDialog.GetMeta.Button"));
    wGetMeta.addListener(SWT.Selection, e -> get());
    wGetMeta.setLayoutData(FormDataBuilder.builder().top().right().build());

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
              BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES),
              BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO)),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.Format"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              3),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.DateLenient"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES),
              BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO)),
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
              BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES),
              BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO)),
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
              ValueMetaBase.roundingTypeDesc),
        };
    colmeta[5].setToolTip(
        BaseMessages.getString(PKG, "SelectValuesDialog.ColumnInfo.Storage.Tooltip"));
    fieldColumns.add(colmeta[0]);
    // Metadata tab should show fields remaining after Remove
    metaFieldColumns.add(colmeta[0]);
    wMeta =
        new TableView(
            variables,
            wMetaComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colmeta,
            MetaRows,
            lsMod,
            props);

    wMeta.setLayoutData(
        FormDataBuilder.builder().top(wlMeta, margin).left().right().bottom(100, 0).build());
    wMetaComp.setLayoutData(
        FormDataBuilder.builder().top().left().right(100, 0).bottom(100, 0).build());

    wMetaComp.layout();
    wMetaTab.setControl(wMetaComp);

    // ///////////////////////////////////////////////////////////
    // / END OF META TAB
    // ///////////////////////////////////////////////////////////
    wTabFolder.setLayoutData(
        FormDataBuilder.builder()
            .top(wTransformName, margin)
            .left()
            .right(100, 0)
            .bottom(wOk, -2 * margin)
            .build());

    // Add a listener to update combo boxes when switching tabs
    // This ensures Remove and Metadata tabs see any field renamings from Select & Alter tab
    wTabFolder.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            int tabIndex = wTabFolder.getSelectionIndex();
            // Update combo boxes when switching to Remove (1) or Metadata (2) tabs
            if (bPreviousFieldsLoaded && (tabIndex == 1 || tabIndex == 2)) {
              // Use asyncExec to ensure update happens after tab switch completes
              shell
                  .getDisplay()
                  .asyncExec(
                      () -> {
                        if (!shell.isDisposed() && bPreviousFieldsLoaded) {
                          setComboBoxes();
                        }
                      });
            }
          }
        });

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
              // Must use asyncExec to access SWT widgets from background thread
              shell
                  .getDisplay()
                  .asyncExec(
                      () -> {
                        if (!shell.isDisposed()) {
                          setComboBoxes();
                        }
                      });
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    getData();
    input.setChanged(changed);
    setComboValues();

    // After getData() sets the initial tab, ensure combo boxes are updated for that tab
    // This handles the case where the dialog opens on Remove or Metadata tab
    shell
        .getDisplay()
        .asyncExec(
            () -> {
              if (!shell.isDisposed() && bPreviousFieldsLoaded) {
                setComboBoxes();
              }
            });
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void setComboValues() {
    Runnable fieldLoader =
        () -> {
          try {
            prevFields = pipelineMeta.getPrevTransformFields(variables, transformName);
            // Populate inputFields map if not already done by the background thread
            if (inputFields.isEmpty() && prevFields != null) {
              for (int i = 0; i < prevFields.size(); i++) {
                inputFields.put(prevFields.getValueMeta(i).getName(), i);
              }
            }
          } catch (HopException e) {
            prevFields = new RowMeta();
            String msg =
                BaseMessages.getString(PKG, "SelectValuesDialog.DoMapping.UnableToFindInput");
            logError(msg);
          }
          bPreviousFieldsLoaded = true;
          // Use setComboBoxes() to properly set values for each tab
          // (Select & Alter gets original fields, Remove/Metadata get renamed/filtered fields)
          setComboBoxes();
        };
    shell.getDisplay().asyncExec(fieldLoader);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wTabFolder.setSelection(0);

    /*
     * Select fields
     */
    List<SelectField> fields = input.getSelectOption().getSelectFields();
    if (!Utils.isEmpty(fields)) {
      for (int i = 0; i < fields.size(); i++) {
        SelectField selectField = fields.get(i);
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
    }
    wUnspecified.setSelection(input.getSelectOption().isSelectingAndSortingUnspecifiedFields());

    /*
     * Remove certain fields...
     */
    List<DeleteField> deleteFields = input.getSelectOption().getDeleteName();
    if (!Utils.isEmpty(deleteFields)) {
      for (int i = 0; i < deleteFields.size(); i++) {
        DeleteField deleteName = deleteFields.get(i);
        TableItem item = wRemove.table.getItem(i);
        if (deleteName != null) {
          item.setText(1, deleteName.getName());
        }
      }
      wRemove.setRowNums();
      wRemove.optWidth(true);
    }

    /*
     * Change the meta-data of certain fields
     */
    List<SelectMetadataChange> meta = input.getSelectOption().getMeta();
    if (!Utils.isEmpty(meta)) {
      for (int i = 0; i < meta.size(); i++) {
        SelectMetadataChange change = meta.get(i);

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
            index++, change.getDateFormatLocale() == null ? "" : change.getDateFormatLocale());
        item.setText(
            index++, change.getDateFormatTimeZone() == null ? "" : change.getDateFormatTimeZone());
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
    }
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
              .equalsIgnoreCase(BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)));
      change.setDateFormatLocale(item.getText(index++));
      change.setDateFormatTimeZone(item.getText(index++));
      change.setLenientStringToNumber(
          item.getText(index++)
              .equalsIgnoreCase(BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)));
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
    final Map<String, Integer> inputFieldsMap = new HashMap<>();

    // Add the currentMeta fields (original input fields from previous transform)
    inputFieldsMap.putAll(inputFields);

    // Prepare field names for Select & Alter tab (always shows ALL original input fields)
    Set<String> inputKeySet = inputFieldsMap.keySet();
    List<String> inputEntries = new ArrayList<>(inputKeySet);
    String[] inputFieldNames = inputEntries.toArray(new String[inputEntries.size()]);

    if (PropsUi.getInstance().isSortFieldByName()) {
      Const.sortStrings(inputFieldNames);
    }

    // Update Select & Alter tab combo box with original input fields
    if (selectFieldColumn != null) {
      selectFieldColumn.setComboValues(inputFieldNames);
    }

    // Now prepare field names for Remove and Metadata tabs
    // These tabs should see the output of Select & Alter (with renaming and filtering)
    final Map<String, Integer> outputFields = new HashMap<>();
    outputFields.putAll(inputFieldsMap);

    if (wFields != null) {
      int nrFields = wFields.nrNonEmpty();

      // If there are fields specified in Select & Alter tab
      if (nrFields > 0) {
        Map<String, Integer> selectedFields = new HashMap<>();

        for (int i = 0; i < nrFields; i++) {
          TableItem item = wFields.getNonEmpty(i);
          String originalName = item.getText(1);
          String renamedName = item.getText(2);

          // Skip if no field name is specified
          if (originalName == null || originalName.trim().isEmpty()) {
            continue;
          }

          // Determine the output field name (renamed or original)
          String outputName =
              (renamedName != null
                      && !renamedName.trim().isEmpty()
                      && !renamedName.equals(originalName))
                  ? renamedName
                  : originalName;

          // Add the output field name to the available fields for Remove/Metadata tabs
          if (inputFieldsMap.containsKey(originalName)) {
            selectedFields.put(outputName, inputFieldsMap.get(originalName));
          } else {
            // Field might not exist in input, but add it anyway for combo box
            selectedFields.put(outputName, i);
          }
        }

        // If "Include unspecified fields" is checked, also include non-selected fields
        if (wUnspecified != null && wUnspecified.getSelection()) {
          // Add any fields that weren't explicitly selected
          for (Map.Entry<String, Integer> entry : inputFieldsMap.entrySet()) {
            boolean alreadySelected = false;
            for (int i = 0; i < nrFields; i++) {
              TableItem item = wFields.getNonEmpty(i);
              String originalName = item.getText(1);
              if (originalName != null
                  && !originalName.trim().isEmpty()
                  && entry.getKey().equals(originalName)) {
                alreadySelected = true;
                break;
              }
            }
            if (!alreadySelected) {
              selectedFields.put(entry.getKey(), entry.getValue());
            }
          }
        }

        // Replace output fields with the selected/renamed fields
        outputFields.clear();
        outputFields.putAll(selectedFields);
      }
    }

    // Prepare field names for Remove tab (output of Select & Alter)
    Set<String> outputKeySet = outputFields.keySet();
    List<String> outputEntries = new ArrayList<>(outputKeySet);
    String[] outputFieldNames = outputEntries.toArray(new String[outputEntries.size()]);

    if (PropsUi.getInstance().isSortFieldByName()) {
      Const.sortStrings(outputFieldNames);
    }

    // Update Remove tab combo boxes with output fields from Select & Alter
    for (ColumnInfo colInfo : removeFieldColumns) {
      colInfo.setComboValues(outputFieldNames);
    }

    // Now prepare field names for Metadata tab (output of Select & Alter minus Remove)
    final Map<String, Integer> metadataFields = new HashMap<>();
    metadataFields.putAll(outputFields);

    // Remove any fields that are specified in the Remove tab
    if (wRemove != null) {
      int nrRemove = wRemove.nrNonEmpty();
      for (int i = 0; i < nrRemove; i++) {
        TableItem item = wRemove.getNonEmpty(i);
        String removedFieldName = item.getText(1);
        if (removedFieldName != null && !removedFieldName.trim().isEmpty()) {
          metadataFields.remove(removedFieldName);
        }
      }
    }

    // Prepare field names for Metadata tab
    Set<String> metadataKeySet = metadataFields.keySet();
    List<String> metadataEntries = new ArrayList<>(metadataKeySet);
    String[] metadataFieldNames = metadataEntries.toArray(new String[metadataEntries.size()]);

    if (PropsUi.getInstance().isSortFieldByName()) {
      Const.sortStrings(metadataFieldNames);
    }

    // Update Metadata tab combo boxes with fields remaining after Remove
    for (ColumnInfo colInfo : metaFieldColumns) {
      colInfo.setComboValues(metadataFieldNames);
    }

    bPreviousFieldsLoaded = true;

    // Force a refresh of the table widgets to ensure combo boxes display updated values
    // Just redrawing isn't enough - we need to dispose any cached combo editors
    if (wRemove != null
        && !wRemove.isDisposed()
        && wRemove.getEditor() != null
        && wRemove.getEditor().getEditor() != null
        && !wRemove.getEditor().getEditor().isDisposed()) {
      // Dispose the active editor (if any) to force recreation with new values
      wRemove.getEditor().getEditor().dispose();
    }
    if (wMeta != null
        && !wMeta.isDisposed()
        && wMeta.getEditor() != null
        && wMeta.getEditor().getEditor() != null
        && !wMeta.getEditor().getEditor().isDisposed()) {
      // Dispose the active editor (if any) to force recreation with new values
      wMeta.getEditor().getEditor().dispose();
    }
  }
}
