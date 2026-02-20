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

package org.apache.hop.pipeline.transforms.tablecompare;

import java.util.Arrays;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.LabelCombo;
import org.apache.hop.ui.core.widget.LabelText;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

public class TableCompareDialog extends BaseTransformDialog {
  private static final Class<?> PKG = TableCompare.class;

  private final TableCompareMeta input;

  /** all fields from the previous transforms */
  private IRowMeta prevFields = null;

  private MetaSelectionLine<DatabaseMeta> wReferenceDB;
  private LabelCombo wReferenceSchema;
  private LabelCombo wReferenceTable;
  private LabelCombo wReferenceCte;

  private MetaSelectionLine<DatabaseMeta> wCompareDB;
  private LabelCombo wCompareSchema;
  private LabelCombo wCompareTable;
  private LabelCombo wCompareCte;

  private LabelCombo wKeyFields;
  private LabelCombo wExcludeFields;
  private LabelText wNrErrors;

  private LabelText wNrRecordsReference;
  private LabelText wNrRecordsCompare;
  private LabelText wNrErrorsLeftJoin;
  private LabelText wNrErrorsInnerJoin;
  private LabelText wNrErrorsRightJoin;

  private LabelCombo wKeyDesc;
  private LabelCombo wReferenceValue;
  private LabelCombo wCompareValue;

  public TableCompareDialog(
      Shell parent,
      IVariables variables,
      TableCompareMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "TableCompareDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    SelectionAdapter lsSelection =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        };
    changed = input.hasChanged();

    ScrolledComposite sc = new ScrolledComposite(shell, SWT.H_SCROLL | SWT.V_SCROLL);
    CTabFolder wTabFolder = new CTabFolder(sc, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // /////////////////////////////
    // START OF REFERENCE TAB
    // /////////////////////////////

    CTabItem wReferenceTab = new CTabItem(wTabFolder, SWT.NONE);
    wReferenceTab.setFont(GuiResource.getInstance().getFontDefault());
    wReferenceTab.setText(
        BaseMessages.getString(PKG, "TableComparisonDialog.ReferenceTab.TabTitle"));

    FormLayout referenceLayout = new FormLayout();
    referenceLayout.marginWidth = 3;
    referenceLayout.marginHeight = 3;

    Composite wReferenceComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wReferenceComp);
    wReferenceComp.setLayout(referenceLayout);

    // Reference DB + schema + table
    DatabaseMeta refDatabaseMeta =
        pipelineMeta.findDatabase(input.getReferenceConnection(), variables);
    wReferenceDB =
        addConnectionLine(
            wReferenceComp,
            null,
            refDatabaseMeta,
            lsMod,
            BaseMessages.getString(PKG, "TableCompareDialog.ReferenceDB.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.ReferenceDB.Tooltip"));
    wReferenceDB.addSelectionListener(lsSelection);

    Control lastControl = wReferenceDB;

    wReferenceSchema =
        new LabelCombo(
            wReferenceComp,
            BaseMessages.getString(PKG, "TableCompareDialog.ReferenceSchemaField.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.ReferenceSchemaField.Tooltip"));
    PropsUi.setLook(wReferenceSchema);
    FormData fdReferenceSchema = new FormData();
    fdReferenceSchema.left = new FormAttachment(0, 0);
    fdReferenceSchema.top = new FormAttachment(lastControl, margin);
    fdReferenceSchema.right = new FormAttachment(100, 0);
    wReferenceSchema.setLayoutData(fdReferenceSchema);
    lastControl = wReferenceSchema;

    wReferenceTable =
        new LabelCombo(
            wReferenceComp,
            BaseMessages.getString(PKG, "TableCompareDialog.ReferenceTableField.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.ReferenceTableField.Tooltip"));
    PropsUi.setLook(wReferenceTable);
    FormData fdReferenceTable = new FormData();
    fdReferenceTable.left = new FormAttachment(0, 0);
    fdReferenceTable.top = new FormAttachment(lastControl, margin);
    fdReferenceTable.right = new FormAttachment(100, 0);
    wReferenceTable.setLayoutData(fdReferenceTable);
    lastControl = wReferenceTable;

    wReferenceCte =
        new LabelCombo(
            wReferenceComp,
            BaseMessages.getString(PKG, "TableCompareDialog.ReferenceCteField.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.ReferenceCteField.Tooltip"));
    PropsUi.setLook(wReferenceCte);
    FormData fdReferenceCte = new FormData();
    fdReferenceCte.left = new FormAttachment(0, 0);
    fdReferenceCte.top = new FormAttachment(lastControl, margin);
    fdReferenceCte.right = new FormAttachment(100, 0);
    wReferenceCte.setLayoutData(fdReferenceCte);

    FormData fdReferenceComp = new FormData();
    fdReferenceComp.left = new FormAttachment(0, 0);
    fdReferenceComp.top = new FormAttachment(0, 0);
    fdReferenceComp.right = new FormAttachment(100, 0);
    fdReferenceComp.bottom = new FormAttachment(100, 0);
    wReferenceComp.setLayoutData(fdReferenceComp);

    wReferenceComp.layout();
    wReferenceTab.setControl(wReferenceComp);

    // ///////////////////////////////////////////////////////////
    // / END OF REFERENCE TAB
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////
    // START OF COMPARISON TAB
    // /////////////////////////////

    CTabItem wComparisonTab = new CTabItem(wTabFolder, SWT.NONE);
    wComparisonTab.setFont(GuiResource.getInstance().getFontDefault());
    wComparisonTab.setText(
        BaseMessages.getString(PKG, "TableComparisonDialog.ComparisonTab.TabTitle"));

    FormLayout comparisonLayout = new FormLayout();
    comparisonLayout.marginWidth = 3;
    comparisonLayout.marginHeight = 3;

    Composite wComparisonComp = new Composite(wTabFolder, SWT.NONE);
    wComparisonComp.setLayout(comparisonLayout);
    PropsUi.setLook(wComparisonComp);

    // Comparison DB + schema + table
    DatabaseMeta compDatabaseMeta =
        pipelineMeta.findDatabase(input.getCompareConnection(), variables);
    wCompareDB =
        addConnectionLine(
            wComparisonComp,
            null,
            compDatabaseMeta,
            lsMod,
            BaseMessages.getString(PKG, "TableCompareDialog.CompareDB.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.CompareDB.Tooltip"));
    wCompareDB.addSelectionListener(lsSelection);
    lastControl = wCompareDB;

    wCompareSchema =
        new LabelCombo(
            wComparisonComp,
            BaseMessages.getString(PKG, "TableCompareDialog.CompareSchemaField.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.CompareSchemaField.Tooltip"));
    PropsUi.setLook(wCompareSchema);
    FormData fdCompareSchema = new FormData();
    fdCompareSchema.left = new FormAttachment(0, 0);
    fdCompareSchema.top = new FormAttachment(lastControl, margin);
    fdCompareSchema.right = new FormAttachment(100, 0);
    wCompareSchema.setLayoutData(fdCompareSchema);
    lastControl = wCompareSchema;

    wCompareTable =
        new LabelCombo(
            wComparisonComp,
            BaseMessages.getString(PKG, "TableCompareDialog.CompareTableField.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.CompareTableField.Tooltip"));
    PropsUi.setLook(wCompareTable);
    FormData fdCompareTable = new FormData();
    fdCompareTable.left = new FormAttachment(0, 0);
    fdCompareTable.top = new FormAttachment(lastControl, margin);
    fdCompareTable.right = new FormAttachment(100, 0);
    wCompareTable.setLayoutData(fdCompareTable);
    lastControl = wCompareTable;

    wCompareCte =
        new LabelCombo(
            wComparisonComp,
            BaseMessages.getString(PKG, "TableCompareDialog.CompareCteField.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.CompareCteField.Tooltip"));
    PropsUi.setLook(wCompareCte);
    FormData fdCompareCte = new FormData();
    fdCompareCte.left = new FormAttachment(0, 0);
    fdCompareCte.top = new FormAttachment(lastControl, margin);
    fdCompareCte.right = new FormAttachment(100, 0);
    wCompareCte.setLayoutData(fdCompareCte);

    FormData fdComparisonComp = new FormData();
    fdComparisonComp.left = new FormAttachment(0, 0);
    fdComparisonComp.top = new FormAttachment(0, 0);
    fdComparisonComp.right = new FormAttachment(100, 0);
    fdComparisonComp.bottom = new FormAttachment(100, 0);
    wComparisonComp.setLayoutData(fdComparisonComp);

    wComparisonComp.layout();
    wComparisonTab.setControl(wComparisonComp);

    // ///////////////////////////////////////////////////////////
    // / END OF COMPARISON TAB
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////
    // START OF OTHER FIELDS TAB
    // /////////////////////////////

    CTabItem wOtherFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wOtherFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wOtherFieldsTab.setText(
        BaseMessages.getString(PKG, "TableComparisonDialog.OtherFieldsTab.TabTitle"));

    FormLayout otherFieldsLayout = new FormLayout();
    otherFieldsLayout.marginWidth = 3;
    otherFieldsLayout.marginHeight = 3;

    Composite wOtherFieldsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wOtherFieldsComp);
    wOtherFieldsComp.setLayout(otherFieldsLayout);

    wKeyFields =
        new LabelCombo(
            wOtherFieldsComp,
            BaseMessages.getString(PKG, "TableCompareDialog.KeyFieldsField.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.KeyFieldsField.Tooltip"));
    PropsUi.setLook(wKeyFields);
    FormData fdKeyFields = new FormData();
    fdKeyFields.left = new FormAttachment(0, 0);
    fdKeyFields.top = new FormAttachment(0, margin);
    fdKeyFields.right = new FormAttachment(100, 0);
    wKeyFields.setLayoutData(fdKeyFields);
    lastControl = wKeyFields;

    wExcludeFields =
        new LabelCombo(
            wOtherFieldsComp,
            BaseMessages.getString(PKG, "TableCompareDialog.ExcludeFieldsField.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.ExcludeFieldsField.Tooltip"));
    PropsUi.setLook(wExcludeFields);
    FormData fdExcludeFields = new FormData();
    fdExcludeFields.left = new FormAttachment(0, 0);
    fdExcludeFields.top = new FormAttachment(lastControl, margin);
    fdExcludeFields.right = new FormAttachment(100, 0);
    wExcludeFields.setLayoutData(fdExcludeFields);
    lastControl = wExcludeFields;

    wKeyDesc =
        new LabelCombo(
            wOtherFieldsComp,
            BaseMessages.getString(PKG, "TableCompareDialog.KeyDescField.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.KeyDescField.Tooltip"));
    PropsUi.setLook(wKeyDesc);
    FormData fdKeyDesc = new FormData();
    fdKeyDesc.left = new FormAttachment(0, 0);
    fdKeyDesc.top = new FormAttachment(lastControl, margin * 3);
    fdKeyDesc.right = new FormAttachment(100, 0);
    wKeyDesc.setLayoutData(fdKeyDesc);
    lastControl = wKeyDesc;

    wReferenceValue =
        new LabelCombo(
            wOtherFieldsComp,
            BaseMessages.getString(PKG, "TableCompareDialog.ReferenceValueField.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.ReferenceValueField.Tooltip"));
    PropsUi.setLook(wReferenceValue);
    FormData fdReferenceValue = new FormData();
    fdReferenceValue.left = new FormAttachment(0, 0);
    fdReferenceValue.top = new FormAttachment(lastControl, margin);
    fdReferenceValue.right = new FormAttachment(100, 0);
    wReferenceValue.setLayoutData(fdReferenceValue);
    lastControl = wReferenceValue;

    wCompareValue =
        new LabelCombo(
            wOtherFieldsComp,
            BaseMessages.getString(PKG, "TableCompareDialog.CompareValueField.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.CompareValueField.Tooltip"));
    PropsUi.setLook(wCompareValue);
    FormData fdCompareValue = new FormData();
    fdCompareValue.left = new FormAttachment(0, 0);
    fdCompareValue.top = new FormAttachment(lastControl, margin);
    fdCompareValue.right = new FormAttachment(100, 0);
    wCompareValue.setLayoutData(fdCompareValue);

    FormData fdOtherFieldsComp = new FormData();
    fdOtherFieldsComp.left = new FormAttachment(0, 0);
    fdOtherFieldsComp.top = new FormAttachment(0, 0);
    fdOtherFieldsComp.right = new FormAttachment(100, 0);
    fdOtherFieldsComp.bottom = new FormAttachment(100, 0);
    wOtherFieldsComp.setLayoutData(fdOtherFieldsComp);

    wOtherFieldsComp.layout();
    wOtherFieldsTab.setControl(wOtherFieldsComp);

    // ///////////////////////////////////////////////////////////
    // / END OF OTHER FIELDS TAB
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////
    // START OF ADDITIONAL FIELDS TAB
    // /////////////////////////////

    CTabItem wAdditionalFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdditionalFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wAdditionalFieldsTab.setText(
        BaseMessages.getString(PKG, "TableComparisonDialog.AdditionalFieldsTab.TabTitle"));

    FormLayout additionalFieldsLayout = new FormLayout();
    additionalFieldsLayout.marginWidth = 3;
    additionalFieldsLayout.marginHeight = 3;

    Composite wAdditionalFieldsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wAdditionalFieldsComp);
    wAdditionalFieldsComp.setLayout(additionalFieldsLayout);

    // The nr of errors field
    //
    wNrErrors =
        new LabelText(
            wAdditionalFieldsComp,
            BaseMessages.getString(PKG, "TableCompareDialog.NrErrorsField.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.NrErrorsField.Tooltip"));
    PropsUi.setLook(wNrErrors);
    FormData fdNrErrors = new FormData();
    fdNrErrors.left = new FormAttachment(0, 0);
    fdNrErrors.top = new FormAttachment(0, margin * 3);
    fdNrErrors.right = new FormAttachment(100, 0);
    wNrErrors.setLayoutData(fdNrErrors);
    lastControl = wNrErrors;

    // The nr of records in the reference table
    //
    wNrRecordsReference =
        new LabelText(
            wAdditionalFieldsComp,
            BaseMessages.getString(PKG, "TableCompareDialog.NrRecordsReferenceField.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.NrRecordsReferenceField.Tooltip"));
    PropsUi.setLook(wNrRecordsReference);
    FormData fdNrRecordsReference = new FormData();
    fdNrRecordsReference.left = new FormAttachment(0, 0);
    fdNrRecordsReference.top = new FormAttachment(lastControl, margin);
    fdNrRecordsReference.right = new FormAttachment(100, 0);
    wNrRecordsReference.setLayoutData(fdNrRecordsReference);
    lastControl = wNrRecordsReference;

    // The nr of records in the Compare table
    //
    wNrRecordsCompare =
        new LabelText(
            wAdditionalFieldsComp,
            BaseMessages.getString(PKG, "TableCompareDialog.NrRecordsCompareField.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.NrRecordsCompareField.Tooltip"));
    PropsUi.setLook(wNrRecordsCompare);
    FormData fdNrRecordsCompare = new FormData();
    fdNrRecordsCompare.left = new FormAttachment(0, 0);
    fdNrRecordsCompare.top = new FormAttachment(lastControl, margin);
    fdNrRecordsCompare.right = new FormAttachment(100, 0);
    wNrRecordsCompare.setLayoutData(fdNrRecordsCompare);
    lastControl = wNrRecordsCompare;

    // The nr of errors in the left join
    //
    wNrErrorsLeftJoin =
        new LabelText(
            wAdditionalFieldsComp,
            BaseMessages.getString(PKG, "TableCompareDialog.NrErrorsLeftJoinField.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.NrErrorsLeftJoinField.Tooltip"));
    PropsUi.setLook(wNrErrorsLeftJoin);
    FormData fdNrErrorsLeftJoin = new FormData();
    fdNrErrorsLeftJoin.left = new FormAttachment(0, 0);
    fdNrErrorsLeftJoin.top = new FormAttachment(lastControl, margin);
    fdNrErrorsLeftJoin.right = new FormAttachment(100, 0);
    wNrErrorsLeftJoin.setLayoutData(fdNrErrorsLeftJoin);
    lastControl = wNrErrorsLeftJoin;

    // The nr of errors in the Inner join
    //
    wNrErrorsInnerJoin =
        new LabelText(
            wAdditionalFieldsComp,
            BaseMessages.getString(PKG, "TableCompareDialog.NrErrorsInnerJoinField.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.NrErrorsInnerJoinField.Tooltip"));
    PropsUi.setLook(wNrErrorsInnerJoin);
    FormData fdNrErrorsInnerJoin = new FormData();
    fdNrErrorsInnerJoin.left = new FormAttachment(0, 0);
    fdNrErrorsInnerJoin.top = new FormAttachment(lastControl, margin);
    fdNrErrorsInnerJoin.right = new FormAttachment(100, 0);
    wNrErrorsInnerJoin.setLayoutData(fdNrErrorsInnerJoin);
    lastControl = wNrErrorsInnerJoin;

    // The nr of errors in the Right join
    //
    wNrErrorsRightJoin =
        new LabelText(
            wAdditionalFieldsComp,
            BaseMessages.getString(PKG, "TableCompareDialog.NrErrorsRightJoinField.Label"),
            BaseMessages.getString(PKG, "TableCompareDialog.NrErrorsRightJoinField.Tooltip"));
    PropsUi.setLook(wNrErrorsRightJoin);
    FormData fdNrErrorsRightJoin = new FormData();
    fdNrErrorsRightJoin.left = new FormAttachment(0, 0);
    fdNrErrorsRightJoin.top = new FormAttachment(lastControl, margin);
    fdNrErrorsRightJoin.right = new FormAttachment(100, 0);
    wNrErrorsRightJoin.setLayoutData(fdNrErrorsRightJoin);
    lastControl = wNrErrorsRightJoin;

    FormData fdAdditionalFieldsComp = new FormData();
    fdAdditionalFieldsComp.left = new FormAttachment(0, 0);
    fdAdditionalFieldsComp.top = new FormAttachment(0, 0);
    fdAdditionalFieldsComp.right = new FormAttachment(100, 0);
    fdAdditionalFieldsComp.bottom = new FormAttachment(100, 0);
    wAdditionalFieldsComp.setLayoutData(fdAdditionalFieldsComp);

    wAdditionalFieldsComp.layout();
    wAdditionalFieldsTab.setControl(wAdditionalFieldsComp);

    // ///////////////////////////////////////////////////////////
    // / END OF ADDITIONAL FIELDS TAB
    // ///////////////////////////////////////////////////////////
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(0, 0);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(100, 0);
    wTabFolder.setLayoutData(fdTabFolder);

    FormData fdSc = new FormData();
    fdSc.left = new FormAttachment(0, 0);
    fdSc.top = new FormAttachment(wSpacer, 0);
    fdSc.right = new FormAttachment(100, 0);
    fdSc.bottom = new FormAttachment(wOk, -margin);
    sc.setLayoutData(fdSc);

    sc.setContent(wTabFolder);

    // ///////////////////////////////////////////////////////////
    // / END OF CONTENT TAB
    // ///////////////////////////////////////////////////////////

    wTabFolder.setSelection(0);

    getData();

    input.setChanged(changed);
    // determine scrollable area
    sc.setMinSize(wTabFolder.computeSize(SWT.DEFAULT, SWT.DEFAULT));
    sc.setExpandHorizontal(true);
    sc.setExpandVertical(true);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void setComboValues() {
    Runnable fieldLoader =
        new Runnable() {
          @Override
          public void run() {

            try {
              prevFields = pipelineMeta.getPrevTransformFields(variables, transformName);

            } catch (HopException e) {
              String msg =
                  BaseMessages.getString(PKG, "TableCompareDialog.DoMapping.UnableToFindInput");
              log.logError(toString(), msg);
            }
            String[] prevTransformFieldNames = prevFields.getFieldNames();
            if (prevTransformFieldNames != null) {
              Arrays.sort(prevTransformFieldNames);

              wReferenceSchema.setItems(prevTransformFieldNames);
              wReferenceTable.setItems(prevTransformFieldNames);
              wReferenceCte.setItems(prevTransformFieldNames);
              wCompareSchema.setItems(prevTransformFieldNames);
              wCompareTable.setItems(prevTransformFieldNames);
              wCompareCte.setItems(prevTransformFieldNames);
              wKeyFields.setItems(prevTransformFieldNames);
              wExcludeFields.setItems(prevTransformFieldNames);
              wKeyDesc.setItems(prevTransformFieldNames);
              wReferenceValue.setItems(prevTransformFieldNames);
              wCompareValue.setItems(prevTransformFieldNames);
            }
          }
        };
    shell.getDisplay().asyncExec(fieldLoader);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {

    wReferenceDB.setText(
        input.getReferenceConnection() != null ? input.getReferenceConnection() : "");
    wReferenceSchema.setText(Const.NVL(input.getReferenceSchemaField(), ""));
    wReferenceTable.setText(Const.NVL(input.getReferenceTableField(), ""));
    wReferenceCte.setText(Const.NVL(input.getReferenceCteField(), ""));
    wCompareDB.setText(input.getCompareConnection() != null ? input.getCompareConnection() : "");
    wCompareSchema.setText(Const.NVL(input.getCompareSchemaField(), ""));
    wCompareTable.setText(Const.NVL(input.getCompareTableField(), ""));
    wCompareCte.setText(Const.NVL(input.getCompareCteField(), ""));
    wKeyFields.setText(Const.NVL(input.getKeyFieldsField(), ""));
    wExcludeFields.setText(Const.NVL(input.getExcludeFieldsField(), ""));

    wNrErrors.setText(Const.NVL(input.getNrErrorsField(), ""));
    wNrRecordsReference.setText(Const.NVL(input.getNrRecordsReferenceField(), ""));
    wNrRecordsCompare.setText(Const.NVL(input.getNrRecordsCompareField(), ""));
    wNrErrorsLeftJoin.setText(Const.NVL(input.getNrErrorsLeftJoinField(), ""));
    wNrErrorsInnerJoin.setText(Const.NVL(input.getNrErrorsInnerJoinField(), ""));
    wNrErrorsRightJoin.setText(Const.NVL(input.getNrErrorsRightJoinField(), ""));

    wKeyDesc.setText(Const.NVL(input.getKeyDescriptionField(), ""));
    wReferenceValue.setText(Const.NVL(input.getValueReferenceField(), ""));
    wCompareValue.setText(Const.NVL(input.getValueCompareField(), ""));

    setComboValues();
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

    input.setReferenceConnection(wReferenceDB.getText());
    input.setReferenceSchemaField(wReferenceSchema.getText());
    input.setReferenceTableField(wReferenceTable.getText());
    input.setReferenceCteField(wReferenceCte.getText());
    input.setCompareConnection(wCompareDB.getText());
    input.setCompareSchemaField(wCompareSchema.getText());
    input.setCompareTableField(wCompareTable.getText());
    input.setCompareCteField(wCompareCte.getText());
    input.setKeyFieldsField(wKeyFields.getText());
    input.setExcludeFieldsField(wExcludeFields.getText());

    input.setNrErrorsField(wNrErrors.getText());
    input.setNrRecordsReferenceField(wNrRecordsReference.getText());
    input.setNrRecordsCompareField(wNrRecordsCompare.getText());
    input.setNrErrorsLeftJoinField(wNrErrorsLeftJoin.getText());
    input.setNrErrorsInnerJoinField(wNrErrorsInnerJoin.getText());
    input.setNrErrorsRightJoinField(wNrErrorsRightJoin.getText());

    input.setKeyDescriptionField(wKeyDesc.getText());
    input.setValueReferenceField(wReferenceValue.getText());
    input.setValueCompareField(wCompareValue.getText());

    try {
      IHopMetadataSerializer<DatabaseMeta> serializer =
          metadataProvider.getSerializer(DatabaseMeta.class);
      String realRefName = variables.resolve(input.getReferenceConnection());
      DatabaseMeta refDatabaseMeta = serializer.load(realRefName);
      if (refDatabaseMeta == null) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(
            BaseMessages.getString(
                PKG, "TableCompareDialog.InvalidConnection.ReferenceConnection.DialogMessage"));
        mb.setText(
            BaseMessages.getString(
                PKG, "TableCompareDialog.InvalidConnection.ReferenceConnection.DialogTitle"));
        mb.open();
      }

      String realCmpName = variables.resolve(input.getCompareConnection());
      DatabaseMeta compDatabaseMeta = serializer.load(realCmpName);
      if (compDatabaseMeta == null) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(
            BaseMessages.getString(
                PKG, "TableCompareDialog.InvalidConnection.ComparisonConnection.DialogMessage"));
        mb.setText(
            BaseMessages.getString(
                PKG, "TableCompareDialog.InvalidConnection.ComparisonConnection.DialogTitle"));
        mb.open();
      }
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error validating database connection", e);
    }

    dispose();
  }
}
