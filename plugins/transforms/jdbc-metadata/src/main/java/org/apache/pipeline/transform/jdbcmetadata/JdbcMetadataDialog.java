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

package org.apache.pipeline.transform.jdbcmetadata;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
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
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class JdbcMetadataDialog extends BaseTransformDialog {
  private static final Class<?> PKG = JdbcMetadataMeta.class; // for i18n purposes
  private JdbcMetadataMeta input;

  private int middle = props.getMiddlePct();
  private int margin = PropsUi.getMargin();
  private boolean dialogChanged;
  private ModifyListener lsMod;
  private Composite metadataComposite;
  private MetaSelectionLine<DatabaseMeta> wConnection;
  private Label alwaysPassInputRowLabel;
  private Button alwaysPassInputRowButton;
  private Label methodLabel;
  private Combo methodCombo;
  private Label argumentSourceLabel;
  private Button argumentSourceFields;
  private Label removeArgumentFieldsLabel;
  private Button removeArgumentFieldsButton;
  private TableView outputFieldsTableView;

  public JdbcMetadataDialog(
      Shell parent,
      IVariables variables,
      JdbcMetadataMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  private final String[] emptyFieldList = new String[0];

  private String[] getFieldListForCombo() {
    String[] items;
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      items = r.getFieldNames();
    } catch (HopException exception) {
      items = emptyFieldList;
    }
    return items;
  }

  /** Remove the UI to enter method arguments The current values are stored and returned. */
  private List<String> removeArgumentsUI() {
    Control[] controls = metadataComposite.getChildren();
    List<String> currentValues = new ArrayList<>();
    for (Control control : controls) {
      if (control == alwaysPassInputRowLabel
          || control == alwaysPassInputRowButton
          || control == methodLabel
          || control == methodCombo
          || control == argumentSourceLabel
          || control == argumentSourceFields
          || control == removeArgumentFieldsLabel
          || control == removeArgumentFieldsButton) continue;
      if (control instanceof ComboVar comboVar) {
        if (!control.isDisposed()
            && (comboVar.getText() != null || !comboVar.getText().isEmpty())) {
          currentValues.add(((ComboVar) control).getText());
        }
      }
      if (!control.isDisposed()) {
        logDebug("removeArgumentsUI - Disposing control!");
        control.dispose();
        logDebug("removeArgumentsUI - number of children in parent composite: " + controls.length);
      }
    }
    return currentValues;
  }

  /**
   * Create the UI to enter one argument.
   *
   * @param argumentDescriptor
   * @param lastControl
   * @param items
   * @return The combobox where the user enters the argument.
   */
  // private ComboVar createArgumentUI(
  private ComboVar createArgumentUI(
      Object[] argumentDescriptor, Control lastControl, String[] items) {
    String argumentName = (String) argumentDescriptor[0];
    Label label = new Label(metadataComposite, SWT.RIGHT);
    label.setText(BaseMessages.getString(PKG, "JdbcMetadata.arguments." + argumentName + ".Label"));
    label.setToolTipText(
        BaseMessages.getString(PKG, "JdbcMetadata.arguments." + argumentName + ".Tooltip"));
    PropsUi.setLook(label);
    FormData labelFormData = new FormData();
    labelFormData.left = new FormAttachment(0, 0);
    labelFormData.right = new FormAttachment(middle, -margin);
    labelFormData.top = new FormAttachment(lastControl, margin);
    label.setLayoutData(labelFormData);

    ComboVar comboVar =
        new ComboVar(variables, metadataComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(comboVar);
    FormData comboVarFormData = new FormData();
    comboVarFormData.left = new FormAttachment(middle, 0);
    comboVarFormData.right = new FormAttachment(100, 0);
    comboVarFormData.top = new FormAttachment(lastControl, margin);
    comboVar.setLayoutData(comboVarFormData);
    comboVar.setItems(items);

    return comboVar;
  }

  /** Create UI to enter arguments. Return a new set of arguments to store in the meta object */
  private List<String> createArgumentsUI(Object[] argumentDescriptors, List<String> currentValues) {
    logDebug(
        "createArgumentsUI, currentValues = "
            + (currentValues == null ? "null" : currentValues.size()));
    Object[] argumentDescriptor;
    int argc = argumentDescriptors.length;
    List<String> newArguments = new ArrayList<>(argc);
    Control lastControl = removeArgumentFieldsButton;
    String[] items = argumentSourceFields.getSelection() ? getFieldListForCombo() : emptyFieldList;
    for (int i = 0; i < argc; i++) {
      argumentDescriptor = (Object[]) argumentDescriptors[i];
      ComboVar comboVar = createArgumentUI(argumentDescriptor, lastControl, items);
      lastControl = comboVar;

      // copy the old argument values to the new arguments array
      if (i >= currentValues.size()) continue;
      String argumentValue = currentValues.get(i);
      newArguments.add(i, argumentValue);
      if (argumentValue == null) continue;
      comboVar.setText(argumentValue);
    }
    return newArguments;
  }

  /** fill the fields table with output fields. */
  private void populateFieldsTable(Object[] methodDescriptor) {
    logDebug("populateFieldsTable 1");
    List<OutputField> outputFields = getOutputFields();
    outputFieldsTableView.clearAll();
    IValueMeta[] fields = (IValueMeta[]) methodDescriptor[2];
    int n = fields.length;
    Table table = outputFieldsTableView.table;
    table.setItemCount(n);
    TableItem tableItem;
    String fieldName;
    IValueMeta field;
    outputFieldsTableView.optWidth(true, n);
    int m = (outputFields == null) ? 0 : outputFields.size();
    OutputField outputField;
    for (int i = 0; i < n; i++) {
      field = fields[i];
      tableItem = table.getItem(i);
      fieldName = field.getName();
      tableItem.setText(1, fieldName);
      // initially, the field is renamed unto itself.
      tableItem.setText(2, fieldName);
      // now see if the meta object renamed this field.
      for (int j = 0; j < m; j++) {
        outputField = outputFields.get(j);
        if (!fieldName.equals(outputField.getName())) continue;
        tableItem.setText(2, outputField.getRename());
        break;
      }
    }
  }

  private void populateFieldsTable() {
    logDebug("populateFieldsTable 2");
    populateFieldsTable(JdbcMetadataMeta.getMethodDescriptor(methodCombo.getSelectionIndex()));
  }

  private void updateOutputFields(List<OutputField> outputFields) {
    logDebug("updateOutputFields " + outputFields);
    if (outputFields == null) return;
    outputFieldsTableView.clearAll();
    OutputField outputField;
    int n = outputFields.size();
    Table table = outputFieldsTableView.table;
    table.setItemCount(n);
    TableItem tableItem;
    String text;
    for (int i = 0; i < n; i++) {
      outputField = outputFields.get(i);
      tableItem = table.getItem(i);

      if (outputField == null) continue;
      text = outputField.getName();
      tableItem.setText(1, text == null ? "" : text);

      if (outputField.getRename() == null) continue;
      text = outputField.getRename();
      tableItem.setText(2, text == null ? "" : text);
    }
  }

  /**
   * When the method is updated, we need the ui to change to allow the user to enter arguments. This
   * takes care of that.
   */
  private void methodUpdated(List<String> argumentValues) {
    logDebug(
        "methodUpdated, argumentValues = "
            + (argumentValues == null ? "null" : argumentValues.size()));

    // first, remove the controls for the previous set of arguments
    List<String> currentValues = removeArgumentsUI();
    logDebug("currentValue = " + currentValues.size());
    if (argumentValues == null) {
      argumentValues = new ArrayList(currentValues.size());
      currentValues.addAll(argumentValues);
    }

    // setup controls for the current set of arguments
    int index = methodCombo.getSelectionIndex();
    Object[] methodDescriptor = (Object[]) JdbcMetadataMeta.methodDescriptors[index];
    Object[] argumentDescriptors = (Object[]) methodDescriptor[1];

    List<String> newArguments = createArgumentsUI(argumentDescriptors, argumentValues);
    // update the arguments in the meta object
    input.setArguments(newArguments);
    metadataComposite.layout();
  }

  private void methodUpdated() {
    logDebug("Parameterless methodUpdated called.");
    methodUpdated(null);
  }

  public String open() {
    dialogChanged = false;
    Shell parent = getParent();

    // SWT code for preparing the dialog
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    changed = input.hasChanged();

    lsMod = e -> input.setChanged();
    SelectionListener lsSelection =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        };
    // ------------------------------------------------------- //
    // SWT code for building the actual settings dialog        //
    // ------------------------------------------------------- //
    Control lastControl;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "JdbcMetadata.Name"));

    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.TransformName.Label"));
    wlTransformName.setToolTipText(BaseMessages.getString(PKG, "System.TransformName.Tooltip"));
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

    lastControl = wTransformName;

    wConnection = addConnectionLine(shell, lastControl, input.getConnection(), lsMod);
    wConnection.addSelectionListener(lsSelection);

    lastControl = wConnection;

    // pass the row checkbox
    alwaysPassInputRowLabel = new Label(shell, SWT.RIGHT);
    alwaysPassInputRowLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.passRow.Label"));
    alwaysPassInputRowLabel.setToolTipText(
        BaseMessages.getString(PKG, "JdbcMetadata.passRow.Tooltip"));
    PropsUi.setLook(alwaysPassInputRowLabel);
    FormData alwaysPassInputRowLabelFormData = new FormData();
    alwaysPassInputRowLabelFormData.left = new FormAttachment(0, 0);
    alwaysPassInputRowLabelFormData.right = new FormAttachment(middle, -margin);
    alwaysPassInputRowLabelFormData.top = new FormAttachment(lastControl, margin);
    alwaysPassInputRowLabel.setLayoutData(alwaysPassInputRowLabelFormData);

    alwaysPassInputRowButton = new Button(shell, SWT.CHECK);
    PropsUi.setLook(alwaysPassInputRowButton);
    FormData alwaysPassInputRowButtonFormData = new FormData();
    alwaysPassInputRowButtonFormData.left = new FormAttachment(middle, 0);
    alwaysPassInputRowButtonFormData.right = new FormAttachment(100, 0);
    alwaysPassInputRowButtonFormData.top = new FormAttachment(lastControl, margin);
    alwaysPassInputRowButton.setLayoutData(alwaysPassInputRowButtonFormData);

    lastControl = alwaysPassInputRowButton;

    //
    CTabFolder cTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(cTabFolder, Props.WIDGET_STYLE_TAB);

    // Metadata tab
    CTabItem metadataTab = new CTabItem(cTabFolder, SWT.NONE);
    metadataTab.setText(BaseMessages.getString(PKG, "JdbcMetadata.MetaDataTab.Label"));
    metadataTab.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.MetaDataTab.Tooltip"));

    FormLayout metadataTabLayout = new FormLayout();
    metadataTabLayout.marginWidth = Const.FORM_MARGIN;
    metadataTabLayout.marginHeight = Const.FORM_MARGIN;

    metadataComposite = new Composite(cTabFolder, SWT.NONE);
    PropsUi.setLook(metadataComposite);
    metadataComposite.setLayout(metadataTabLayout);

    // method
    methodLabel = new Label(metadataComposite, SWT.RIGHT);
    methodLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.metadataMethod.Label"));
    methodLabel.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.metadataMethod.Tooltip"));
    PropsUi.setLook(methodLabel);
    FormData methodLabelFormData = new FormData();
    methodLabelFormData.left = new FormAttachment(0, 0);
    methodLabelFormData.right = new FormAttachment(middle, -margin);
    methodLabelFormData.top = new FormAttachment(0, margin);
    methodLabel.setLayoutData(methodLabelFormData);

    methodCombo = new Combo(metadataComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(methodCombo);
    methodCombo.addModifyListener(lsMod);
    FormData methodComboFormData = new FormData();
    methodComboFormData.left = new FormAttachment(middle, 0);
    methodComboFormData.right = new FormAttachment(100, 0);
    methodComboFormData.top = new FormAttachment(lastControl, margin);
    methodCombo.setLayoutData(methodComboFormData);

    Object[] methodDescriptor;
    String methodName;
    for (int i = 0; i < JdbcMetadataMeta.methodDescriptors.length; i++) {
      methodDescriptor = (Object[]) JdbcMetadataMeta.methodDescriptors[i];
      methodName = (String) methodDescriptor[0];
      methodCombo.add(BaseMessages.getString(PKG, "JdbcMetadata.methods." + methodName));
    }

    SelectionListener methodComboSelectionListener =
        new SelectionListener() {
          @Override
          public void widgetDefaultSelected(SelectionEvent selectionEvent) {
            // Do nothing
          }

          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            logDebug("methodCombo changed, calling parameterless methodUpdated");
            methodUpdated();
            populateFieldsTable();
            input.setChanged();
          }
        };
    methodCombo.addSelectionListener(methodComboSelectionListener);
    lastControl = methodCombo;

    // argument source
    argumentSourceLabel = new Label(metadataComposite, SWT.RIGHT);
    argumentSourceLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.argumentSource.Label"));
    argumentSourceLabel.setToolTipText(
        BaseMessages.getString(PKG, "JdbcMetadata.argumentSource.Tooltip"));
    PropsUi.setLook(argumentSourceLabel);
    FormData argumentSourceLabelFormData = new FormData();
    argumentSourceLabelFormData.left = new FormAttachment(0, 0);
    argumentSourceLabelFormData.right = new FormAttachment(middle, -margin);
    argumentSourceLabelFormData.top = new FormAttachment(lastControl, margin);
    argumentSourceLabel.setLayoutData(argumentSourceLabelFormData);

    argumentSourceFields = new Button(metadataComposite, SWT.CHECK);
    PropsUi.setLook(argumentSourceFields);
    FormData argumentSourceFieldsFormData = new FormData();
    argumentSourceFieldsFormData.left = new FormAttachment(middle, 0);
    argumentSourceFieldsFormData.right = new FormAttachment(100, 0);
    argumentSourceFieldsFormData.top = new FormAttachment(lastControl, margin);
    argumentSourceFields.setLayoutData(argumentSourceFieldsFormData);
    SelectionListener argumentSourceFieldsSelectionListener =
        new SelectionListener() {
          @Override
          public void widgetDefaultSelected(SelectionEvent selectionEvent) {
            // Do nothing
          }

          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            Control[] controls = metadataComposite.getChildren();
            boolean selection = argumentSourceFields.getSelection();
            removeArgumentFieldsButton.setSelection(selection);
            removeArgumentFieldsButton.setEnabled(selection);
            String[] items = selection ? getFieldListForCombo() : emptyFieldList;

            for (Control control : controls) {
              if (!(control instanceof ComboVar)) continue;
              ComboVar comboVar = (ComboVar) control;
              comboVar.setItems(items);
            }

            input.setChanged();
          }
        };
    argumentSourceFields.addSelectionListener(argumentSourceFieldsSelectionListener);

    lastControl = argumentSourceFields;

    // remove arguments
    removeArgumentFieldsLabel = new Label(metadataComposite, SWT.RIGHT);
    removeArgumentFieldsLabel.setText(
        BaseMessages.getString(PKG, "JdbcMetadata.removeArgumentFields.Label"));
    removeArgumentFieldsLabel.setToolTipText(
        BaseMessages.getString(PKG, "JdbcMetadata.removeArgumentFields.Tooltip"));
    PropsUi.setLook(removeArgumentFieldsLabel);
    FormData removeArgumentFieldsLabelFormData = new FormData();
    removeArgumentFieldsLabelFormData.left = new FormAttachment(0, 0);
    removeArgumentFieldsLabelFormData.right = new FormAttachment(middle, -margin);
    removeArgumentFieldsLabelFormData.top = new FormAttachment(lastControl, margin);
    removeArgumentFieldsLabel.setLayoutData(removeArgumentFieldsLabelFormData);

    removeArgumentFieldsButton = new Button(metadataComposite, SWT.CHECK);
    PropsUi.setLook(removeArgumentFieldsButton);
    FormData removeArgumentFieldsButtonFormData = new FormData();
    removeArgumentFieldsButtonFormData.left = new FormAttachment(middle, 0);
    removeArgumentFieldsButtonFormData.right = new FormAttachment(100, 0);
    removeArgumentFieldsButtonFormData.top = new FormAttachment(lastControl, margin);
    removeArgumentFieldsButton.setLayoutData(removeArgumentFieldsButtonFormData);
    removeArgumentFieldsButton.addSelectionListener(lsSelection);

    // layout the metdata tab
    FormData metadataTabFormData = new FormData();
    metadataTabFormData.left = new FormAttachment(0, 0);
    metadataTabFormData.top = new FormAttachment(0, 0);
    metadataTabFormData.right = new FormAttachment(100, 0);
    metadataTabFormData.bottom = new FormAttachment(100, 0);
    metadataComposite.setLayoutData(metadataTabFormData);
    metadataComposite.layout();
    metadataTab.setControl(metadataComposite);

    // Fields tab
    CTabItem fieldsTab = new CTabItem(cTabFolder, SWT.NONE);
    fieldsTab.setText(BaseMessages.getString(PKG, "JdbcMetadata.FieldsTab.Label"));
    fieldsTab.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.FieldsTab.Tooltip"));

    FormLayout fieldsTabLayout = new FormLayout();
    fieldsTabLayout.marginWidth = Const.FORM_MARGIN;
    fieldsTabLayout.marginHeight = Const.FORM_MARGIN;

    Composite fieldsComposite = new Composite(cTabFolder, SWT.NONE);
    PropsUi.setLook(fieldsComposite);
    fieldsComposite.setLayout(fieldsTabLayout);

    // add UI for the fields tab.
    Label outputFieldsTableViewLabel = new Label(fieldsComposite, SWT.NONE);
    outputFieldsTableViewLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.FieldsTab.Label"));
    outputFieldsTableViewLabel.setToolTipText(
        BaseMessages.getString(PKG, "JdbcMetadata.FieldsTab.Tooltip"));
    PropsUi.setLook(outputFieldsTableViewLabel);
    FormData outputFieldsTableViewLabelFormData = new FormData();
    outputFieldsTableViewLabelFormData.left = new FormAttachment(0, 0);
    outputFieldsTableViewLabelFormData.top = new FormAttachment(0, margin);
    outputFieldsTableViewLabel.setLayoutData(outputFieldsTableViewLabelFormData);

    ColumnInfo[] columnInfo =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "JdbcMetadata.FieldName.Label"),
              ColumnInfo.COLUMN_TYPE_NONE),
          new ColumnInfo(
              BaseMessages.getString(PKG, "JdbcMetadata.OutputFieldName.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT)
        };
    outputFieldsTableView =
        new TableView(
            variables,
            fieldsComposite,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            columnInfo,
            10,
            lsMod,
            props);

    Button getFieldsButton = new Button(fieldsComposite, SWT.PUSH);
    getFieldsButton.setText(BaseMessages.getString(PKG, "JdbcMetadata.getFieldsButton.Label"));
    getFieldsButton.setToolTipText(
        BaseMessages.getString(PKG, "JdbcMetadata.getFieldsButton.Tooltip"));
    FormData getFieldsButtonFormData = new FormData();
    getFieldsButtonFormData.top = new FormAttachment(outputFieldsTableViewLabel, margin);
    getFieldsButtonFormData.right = new FormAttachment(100, 0);
    getFieldsButton.setLayoutData(getFieldsButtonFormData);
    getFieldsButton.addSelectionListener(
        new SelectionListener() {

          @Override
          public void widgetDefaultSelected(SelectionEvent arg0) {
            // Do nothing
          }

          @Override
          public void widgetSelected(SelectionEvent arg0) {
            populateFieldsTable();
            input.setChanged();
          }
        });

    FormData outputFieldsTableViewFormData = new FormData();
    outputFieldsTableViewFormData.left = new FormAttachment(0, 0);
    outputFieldsTableViewFormData.top = new FormAttachment(outputFieldsTableViewLabel, margin);
    outputFieldsTableViewFormData.right = new FormAttachment(getFieldsButton, -margin);
    outputFieldsTableViewFormData.bottom = new FormAttachment(100, -2 * margin);
    outputFieldsTableView.setLayoutData(outputFieldsTableViewFormData);

    // layout the fields tab
    FormData fieldsTabFormData = new FormData();
    fieldsTabFormData.left = new FormAttachment(0, 0);
    fieldsTabFormData.top = new FormAttachment(0, 0);
    fieldsTabFormData.right = new FormAttachment(100, 0);
    fieldsTabFormData.bottom = new FormAttachment(100, 0);
    fieldsComposite.setLayoutData(metadataTabFormData);
    fieldsComposite.layout();
    fieldsTab.setControl(fieldsComposite);

    // OK and cancel buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // Tabfolder
    FormData cTabFolderFormData = new FormData();
    cTabFolderFormData.left = new FormAttachment(0, 0);
    cTabFolderFormData.top = new FormAttachment(alwaysPassInputRowButton, margin);
    cTabFolderFormData.right = new FormAttachment(100, 0);
    cTabFolderFormData.bottom = new FormAttachment(wOk, -margin);
    cTabFolder.setLayoutData(cTabFolderFormData);
    cTabFolder.setSelection(0);

    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());

    setSize();
    populateDialog();
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void setMethod(String method) {
    int index = JdbcMetadataMeta.getMethodDescriptorIndex(method);
    if (index == -1) throw new IllegalArgumentException("Index for method " + method + " is -1.");
    methodCombo.select(index);
    logDebug("setMethod called, calling parameterless method updated");
  }

  /**
   * This helper method puts the step configuration stored in the meta object and puts it into the
   * dialog controls.
   */
  private void populateDialog() {
    wTransformName.selectAll();
    String value;

    value = input.getConnection();
    if (value != null) wConnection.setText(value);

    alwaysPassInputRowButton.setSelection(input.isAlwaysPassInputRow());

    value = input.getMethodName();
    if (value != null) setMethod(value);

    argumentSourceFields.setSelection(input.isArgumentSourceFields());
    if (!Utils.isEmpty(input.getArguments())) {
      methodUpdated(input.getArguments());
    } else {
      // If no arguments' values are saved build the arguments' list without assigning any values
      methodUpdated();
    }

    logDebug("Calling methodUpdated from populate dialog.");
    if (!Utils.isEmpty(input.getOutputFields())) {
      updateOutputFields(input.getOutputFields());
    }

    removeArgumentFieldsButton.setSelection(input.isRemoveArgumentFields());
    removeArgumentFieldsButton.setEnabled(input.isArgumentSourceFields());
  }

  /** Called when the user cancels the dialog. */
  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  /** Called when the user confirms the dialog */
  private void ok() {

    transformName = wTransformName.getText();
    // Save settings to the meta object
    input.setConnection(wConnection.getText());
    input.setAlwaysPassInputRow(alwaysPassInputRowButton.getSelection());
    input.setMethodName(JdbcMetadataMeta.getMethodName(methodCombo.getSelectionIndex()));
    input.setArgumentSourceFields(argumentSourceFields.getSelection());
    input.setArguments(getArguments());
    input.setRemoveArgumentFields(removeArgumentFieldsButton.getSelection());
    input.setOutputFields(getOutputFields());

    dispose();
  }

  private List<String> getArguments() {
    List<String> arguments = new ArrayList<>();
    Control[] controls = metadataComposite.getChildren();
    String text;
    for (Control control : controls) {
      if (!(control instanceof ComboVar)) continue;
      ComboVar comboVar = (ComboVar) control;
      text = comboVar.getText();
      arguments.add(text);
    }
    return arguments;
  }

  private List<OutputField> getOutputFields() {
    Table table = outputFieldsTableView.table;
    List<OutputField> outputFields = new ArrayList<>();
    TableItem tableItem;
    for (int i = 0; i < table.getItemCount(); i++) {
      tableItem = table.getItem(i);
      OutputField item = new OutputField(tableItem.getText(1), tableItem.getText(2));
      outputFields.add(i, item);
    }
    return outputFields;
  }
}
