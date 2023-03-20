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

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.ArrayList;
import java.util.List;

public class JdbcMetadataDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = JdbcMetadataMeta.class; // for i18n purposes
  private JdbcMetadataMeta meta;

  private int middle = props.getMiddlePct();
  private int margin = PropsUi.getMargin();

  private boolean dialogChanged;
  private ModifyListener lsMod;

  private Composite metadataComposite;
  //
  private MetaSelectionLine<DatabaseMeta>
      wConnectionSource; // text field holding the name of the field containing the connection name
  private CCombo connectionField;
  // text field holding the name of the field containing the driver name
  private ComboVar jdbcDriverField;
  // text field holding the name of the field containing the url
  private ComboVar jdbcUrlField;
  // text field holding the name of the field containing the user
  private ComboVar jdbcUserField;
  // text field holding the name of the field containing the password
  private ComboVar jdbcPasswordField;
  //

  private Label alwaysPassInputRowLabel;
  //
  private Button alwaysPassInputRowButton;
  //
  private Label methodLabel;
  //
  // private CCombo methodCombo;
  private Combo methodCombo;

  private Label argumentSourceLabel;
  //
  private Button argumentSourceFields;
  //
  private Label removeArgumentFieldsLabel;
  //
  private Button removeArgumentFieldsButton;
  //
  private TableView outputFieldsTableView;

  public JdbcMetadataDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    meta = (JdbcMetadataMeta) in;
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

  private void connectionSourceUpdated() {
    int selectedIndex = wConnectionSource.getSelectionIndex();
    String option = JdbcMetadataMeta.connectionSourceOptions[selectedIndex];
    boolean connectionComboEnabled, connectionFieldEnabled, otherFieldsEnabled;
    otherFieldsEnabled = connectionComboEnabled = connectionFieldEnabled = false;
    String[] fields = emptyFieldList;
    if (JdbcMetadataMeta.connectionSourceOptionConnection.equals(option)) {
      connectionComboEnabled = true;
    } else {
      if (JdbcMetadataMeta.connectionSourceOptionConnectionField.equals(option)) {
        connectionFieldEnabled = true;
        connectionField.setItems(getFieldListForCombo());
      } else {
        otherFieldsEnabled = true;
        if (JdbcMetadataMeta.connectionSourceOptionJDBCFields.equals(option)) {
          fields = getFieldListForCombo();
        }
      }
    }
    wConnectionSource.setEnabled(connectionComboEnabled);
    connectionField.setEnabled(connectionFieldEnabled);
    jdbcDriverField.setEnabled(otherFieldsEnabled);
    jdbcDriverField.setItems(fields);
    jdbcUrlField.setEnabled(otherFieldsEnabled);
    jdbcUrlField.setItems(fields);
    jdbcUserField.setEnabled(otherFieldsEnabled);
    jdbcUserField.setItems(fields);
    jdbcPasswordField.setEnabled(otherFieldsEnabled);
    jdbcPasswordField.setItems(fields);
  }

  /** Remove the UI to enter method arguments The current values are stored and returned. */
  private List<String> removeArgumentsUI() {
    Control[] controls = metadataComposite.getChildren();
    logBasic(
            "[removeArgumentsUI] metadataComposite start - Children # -> "
                    + controls.length);
    List<String> currentValues = new ArrayList<String>();
    for (Control control : controls) {
      if (control == alwaysPassInputRowLabel
          || control == alwaysPassInputRowButton
          || control == methodLabel
          || control == methodCombo
          || control == argumentSourceLabel
          || control == argumentSourceFields
          || control == removeArgumentFieldsLabel
          || control == removeArgumentFieldsButton) continue;
      if (control instanceof Combo ) {
        if (control.isDisposed()) {
          logBasic("removeArgumentsUI - trying to access a disposed control!");
        }
        if (!control.isDisposed() && (((Combo) control).getText() != null || ((Combo) control).getText().length()>0)) {
          currentValues.add(((Combo) control).getText());
          logBasic("removeArgumentsUI - control.getText(): " + ((Combo) control).getText());
        }
      }
      if (!control.isDisposed()) {
        logBasic("removeArgumentsUI - Disposing control!");
        control.dispose();
        logBasic("removeArgumentsUI - number of children in parent composite: " + controls.length);
      }
    }

    logBasic(
        "[removeArgumentsUI] metadataComposite end - Children # -> "
            + metadataComposite.getChildren().length);
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
  private Combo createArgumentUI(
      Object[] argumentDescriptor, Control lastControl, String[] items) {
    String argumentName = (String) argumentDescriptor[0];
    Label label = new Label(metadataComposite, SWT.RIGHT);
    label.setText(BaseMessages.getString(PKG, "JdbcMetadata.arguments." + argumentName + ".Label"));
    label.setToolTipText(
        BaseMessages.getString(PKG, "JdbcMetadata.arguments." + argumentName + ".Tooltip"));
    props.setLook(label);
    FormData labelFormData = new FormData();
    labelFormData.left = new FormAttachment(0, 0);
    labelFormData.right = new FormAttachment(middle, -margin);
    labelFormData.top = new FormAttachment(lastControl, margin);
    label.setLayoutData(labelFormData);

//    ComboVar comboVar =
//        new ComboVar(variables, metadataComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    Combo comboVar =
        new Combo (metadataComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(comboVar);
    FormData comboVarFormData = new FormData();
    comboVarFormData.left = new FormAttachment(middle, 0);
    comboVarFormData.right = new FormAttachment(100, 0);
    comboVarFormData.top = new FormAttachment(lastControl, margin);
    comboVar.setLayoutData(comboVarFormData);
    comboVar.setItems(items);

    // comboVar.addModifyListener(lsMod);

    return comboVar;
  }
  /** Create UI to enter arguments. Return a new set of arguments to store in the meta object */
  private List<String> createArgumentsUI(Object[] argumentDescriptors, List<String> currentValues) {
    logBasic(
        "createArgumentsUI, currentValues = "
            + (currentValues == null ? "null" : currentValues.size()));
    Object[] argumentDescriptor;
    int argc = argumentDescriptors.length;
    List<String> newArguments = new ArrayList<String>(argc);
    Control lastControl = removeArgumentFieldsButton;
    String[] items = argumentSourceFields.getSelection() ? getFieldListForCombo() : emptyFieldList;
    for (int i = 0; i < argc; i++) {
      argumentDescriptor = (Object[]) argumentDescriptors[i];
      // ComboVar comboVar = createArgumentUI(argumentDescriptor, lastControl, items);
      Combo comboVar = createArgumentUI(argumentDescriptor, lastControl, items);
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
    logBasic("populateFieldsTable 1");
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
    logBasic("populateFieldsTable 2");
    populateFieldsTable(JdbcMetadataMeta.getMethodDescriptor(methodCombo.getSelectionIndex()));
  }

  private void updateOutputFields(List<OutputField> outputFields) {
    logBasic("updateOutputFields " + outputFields);
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
    logBasic(
        "methodUpdated, argumentValues = "
            + (argumentValues == null ? "null" : argumentValues.size()));
    // first, remove the controls for the previous set of arguments
    List<String> currentValues = removeArgumentsUI();
    logBasic("currentValue = " + currentValues.size());
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
    meta.setArguments(newArguments);
    // show / hide the argument source ui depending on whether we have arguments
    boolean visible = newArguments.size() > 0;
    /* (SR) Temporarily removed
        argumentSourceFields.setVisible(visible);
        argumentSourceLabel.setVisible(visible);
        removeArgumentFieldsLabel.setVisible(visible);
        removeArgumentFieldsButton.setVisible(visible);
    */
    metadataComposite.layout();
  }

  private void methodUpdated() {
    logBasic("Parameterless methodUpdated called.");
    methodUpdated(null);
  }

  public String open() {
    dialogChanged = false;
    // store some convenient SWT variables
    Shell parent = getParent();

    // SWT code for preparing the dialog
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, meta);

    changed = meta.hasChanged();

    lsMod = e -> meta.setChanged();
    SelectionListener lsSelection =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            meta.setChanged();
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
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);

    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    lastControl = wTransformName;

    // pass the row checkbox
    alwaysPassInputRowLabel = new Label(shell, SWT.RIGHT);
    alwaysPassInputRowLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.passRow.Label"));
    alwaysPassInputRowLabel.setToolTipText(
        BaseMessages.getString(PKG, "JdbcMetadata.passRow.Tooltip"));
    props.setLook(alwaysPassInputRowLabel);
    FormData alwaysPassInputRowLabelFormData = new FormData();
    alwaysPassInputRowLabelFormData.left = new FormAttachment(0, 0);
    alwaysPassInputRowLabelFormData.right = new FormAttachment(middle, -margin);
    alwaysPassInputRowLabelFormData.top = new FormAttachment(lastControl, margin);
    alwaysPassInputRowLabel.setLayoutData(alwaysPassInputRowLabelFormData);

    alwaysPassInputRowButton = new Button(shell, SWT.CHECK);
    props.setLook(alwaysPassInputRowButton);
    FormData alwaysPassInputRowButtonFormData = new FormData();
    alwaysPassInputRowButtonFormData.left = new FormAttachment(middle, 0);
    alwaysPassInputRowButtonFormData.right = new FormAttachment(100, 0);
    alwaysPassInputRowButtonFormData.top = new FormAttachment(lastControl, margin);
    alwaysPassInputRowButton.setLayoutData(alwaysPassInputRowButtonFormData);

    lastControl = alwaysPassInputRowButton;

    //
    CTabFolder cTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(cTabFolder, Props.WIDGET_STYLE_TAB);

    // Connection tab
    CTabItem connectionTab = new CTabItem(cTabFolder, SWT.NONE);
    connectionTab.setText(BaseMessages.getString(PKG, "JdbcMetadata.ConnectionTab.Label"));
    connectionTab.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.ConnectionTab.Tooltip"));

    FormLayout connectionTabLayout = new FormLayout();
    connectionTabLayout.marginWidth = PropsUi.getFormMargin();
    connectionTabLayout.marginHeight = PropsUi.getFormMargin();

    Composite connectionComposite = new Composite(cTabFolder, SWT.NONE);
    props.setLook(connectionComposite);
    connectionComposite.setLayout(connectionTabLayout);

    // Connection line
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(meta.getConnectionName(), variables);
    wConnectionSource =
        addConnectionLine(
            connectionComposite,
            wTransformName,
            databaseMeta,
            lsMod,
            BaseMessages.getString(PKG, "JdbcMetadata.connectionSource.Label"),
            BaseMessages.getString(PKG, "JdbcMetadata.connectionSource.Tooltip"));
    wConnectionSource.addSelectionListener(lsSelection);
    lastControl = wConnectionSource;

    // connection name field
    Label connectionFieldLabel = new Label(connectionComposite, SWT.RIGHT);
    connectionFieldLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.connectionField.Label"));
    connectionFieldLabel.setToolTipText(
        BaseMessages.getString(PKG, "JdbcMetadata.connectionField.Tooltip"));
    props.setLook(connectionFieldLabel);
    FormData connectionFieldLabelFormData = new FormData();
    connectionFieldLabelFormData.left = new FormAttachment(0, 0);
    connectionFieldLabelFormData.right = new FormAttachment(middle, -margin);
    connectionFieldLabelFormData.top = new FormAttachment(lastControl, margin);
    connectionFieldLabel.setLayoutData(connectionFieldLabelFormData);

    connectionField = new CCombo(connectionComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(connectionField);
    connectionField.addModifyListener(lsMod);
    FormData connectionFieldFormData = new FormData();
    connectionFieldFormData.left = new FormAttachment(middle, 0);
    connectionFieldFormData.right = new FormAttachment(100, 0);
    connectionFieldFormData.top = new FormAttachment(lastControl, margin);
    connectionField.setLayoutData(connectionFieldFormData);

    lastControl = connectionField;

    // jdbc driver field
    Label jdbcDriverLabel = new Label(connectionComposite, SWT.RIGHT);
    jdbcDriverLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.driverField.Label"));
    jdbcDriverLabel.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.driverField.Tooltip"));
    props.setLook(jdbcDriverLabel);
    FormData jdbcDriverLabelFormData = new FormData();
    jdbcDriverLabelFormData.left = new FormAttachment(0, 0);
    jdbcDriverLabelFormData.right = new FormAttachment(middle, -margin);
    jdbcDriverLabelFormData.top = new FormAttachment(lastControl, margin);
    jdbcDriverLabel.setLayoutData(jdbcDriverLabelFormData);

    jdbcDriverField =
        new ComboVar(variables, connectionComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(jdbcDriverField);
    jdbcDriverField.addModifyListener(lsMod);
    FormData jdbcDriverFieldFormData = new FormData();
    jdbcDriverFieldFormData.left = new FormAttachment(middle, 0);
    jdbcDriverFieldFormData.right = new FormAttachment(100, 0);
    jdbcDriverFieldFormData.top = new FormAttachment(lastControl, margin);
    jdbcDriverField.setLayoutData(jdbcDriverFieldFormData);

    lastControl = jdbcDriverField;

    // jdbc url field
    Label jdbcUrlLabel = new Label(connectionComposite, SWT.RIGHT);
    jdbcUrlLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.urlField.Label"));
    jdbcUrlLabel.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.urlField.Tooltip"));
    props.setLook(jdbcUrlLabel);
    FormData jdbcUrlLabelFormData = new FormData();
    jdbcUrlLabelFormData.left = new FormAttachment(0, 0);
    jdbcUrlLabelFormData.right = new FormAttachment(middle, -margin);
    jdbcUrlLabelFormData.top = new FormAttachment(lastControl, margin);
    jdbcUrlLabel.setLayoutData(jdbcUrlLabelFormData);

    jdbcUrlField = new ComboVar(variables, connectionComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(jdbcUrlField);
    jdbcUrlField.addModifyListener(lsMod);
    FormData jdbcUrlFieldFormData = new FormData();
    jdbcUrlFieldFormData.left = new FormAttachment(middle, 0);
    jdbcUrlFieldFormData.right = new FormAttachment(100, 0);
    jdbcUrlFieldFormData.top = new FormAttachment(lastControl, margin);
    jdbcUrlField.setLayoutData(jdbcUrlFieldFormData);

    lastControl = jdbcUrlField;

    // jdbc user field
    Label jdbcUserLabel = new Label(connectionComposite, SWT.RIGHT);
    jdbcUserLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.userField.Label"));
    jdbcUserLabel.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.userField.Tooltip"));
    props.setLook(jdbcUserLabel);
    FormData jdbcUserLabelFormData = new FormData();
    jdbcUserLabelFormData.left = new FormAttachment(0, 0);
    jdbcUserLabelFormData.right = new FormAttachment(middle, -margin);
    jdbcUserLabelFormData.top = new FormAttachment(lastControl, margin);
    jdbcUserLabel.setLayoutData(jdbcUserLabelFormData);

    jdbcUserField =
        new ComboVar(variables, connectionComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(jdbcUserField);
    jdbcUserField.addModifyListener(lsMod);
    FormData jdbcUserFieldFormData = new FormData();
    jdbcUserFieldFormData.left = new FormAttachment(middle, 0);
    jdbcUserFieldFormData.right = new FormAttachment(100, 0);
    jdbcUserFieldFormData.top = new FormAttachment(lastControl, margin);
    jdbcUserField.setLayoutData(jdbcUserFieldFormData);

    lastControl = jdbcUserField;

    // jdbc password field
    Label jdbcPasswordLabel = new Label(connectionComposite, SWT.RIGHT);
    jdbcPasswordLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.passwordField.Label"));
    jdbcPasswordLabel.setToolTipText(
        BaseMessages.getString(PKG, "JdbcMetadata.passwordField.Tooltip"));
    props.setLook(jdbcPasswordLabel);
    FormData jdbcPasswordLabelFormData = new FormData();
    jdbcPasswordLabelFormData.left = new FormAttachment(0, 0);
    jdbcPasswordLabelFormData.right = new FormAttachment(middle, -margin);
    jdbcPasswordLabelFormData.top = new FormAttachment(lastControl, margin);
    jdbcPasswordLabel.setLayoutData(jdbcPasswordLabelFormData);

    jdbcPasswordField =
        new ComboVar(variables, connectionComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(jdbcPasswordField);
    jdbcPasswordField.addModifyListener(lsMod);
    FormData jdbcPasswordFieldFormData = new FormData();
    jdbcPasswordFieldFormData.left = new FormAttachment(middle, 0);
    jdbcPasswordFieldFormData.right = new FormAttachment(100, 0);
    jdbcPasswordFieldFormData.top = new FormAttachment(lastControl, margin);
    jdbcPasswordField.setLayoutData(jdbcPasswordFieldFormData);

    lastControl = jdbcPasswordField;

    // layout the connection tab
    FormData connectionTabFormData = new FormData();
    connectionTabFormData.left = new FormAttachment(0, 0);
    connectionTabFormData.top = new FormAttachment(0, 0);
    connectionTabFormData.right = new FormAttachment(100, 0);
    connectionTabFormData.bottom = new FormAttachment(100, 0);
    connectionComposite.setLayoutData(connectionTabFormData);
    connectionComposite.layout();
    connectionTab.setControl(connectionComposite);

    // Metadata tab
    CTabItem metadataTab = new CTabItem(cTabFolder, SWT.NONE);
    metadataTab.setText(BaseMessages.getString(PKG, "JdbcMetadata.MetaDataTab.Label"));
    metadataTab.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.MetaDataTab.Tooltip"));

    FormLayout metadataTabLayout = new FormLayout();
    connectionTabLayout.marginWidth = Const.FORM_MARGIN;
    connectionTabLayout.marginHeight = Const.FORM_MARGIN;

    metadataComposite = new Composite(cTabFolder, SWT.NONE);
    props.setLook(metadataComposite);
    metadataComposite.setLayout(metadataTabLayout);

    // method
    methodLabel = new Label(metadataComposite, SWT.RIGHT);
    methodLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.metadataMethod.Label"));
    methodLabel.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.metadataMethod.Tooltip"));
    props.setLook(methodLabel);
    FormData methodLabelFormData = new FormData();
    methodLabelFormData.left = new FormAttachment(0, 0);
    methodLabelFormData.right = new FormAttachment(middle, -margin);
    methodLabelFormData.top = new FormAttachment(0, margin);
    methodLabel.setLayoutData(methodLabelFormData);

    methodCombo = new Combo(metadataComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(methodCombo);
    // methodCombo.setEditable(false);
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
          public void widgetDefaultSelected(SelectionEvent selectionEvent) {}

          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            logBasic("methodCombo changed, calling parameterless methodUpdated");
            methodUpdated();
            populateFieldsTable();
            meta.setChanged();
          }
        };
    methodCombo.addSelectionListener(methodComboSelectionListener);
    lastControl = methodCombo;

    // argument source
    argumentSourceLabel = new Label(metadataComposite, SWT.RIGHT);
    argumentSourceLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.argumentSource.Label"));
    argumentSourceLabel.setToolTipText(
        BaseMessages.getString(PKG, "JdbcMetadata.argumentSource.Tooltip"));
    props.setLook(argumentSourceLabel);
    FormData argumentSourceLabelFormData = new FormData();
    argumentSourceLabelFormData.left = new FormAttachment(0, 0);
    argumentSourceLabelFormData.right = new FormAttachment(middle, -margin);
    argumentSourceLabelFormData.top = new FormAttachment(lastControl, margin);
    argumentSourceLabel.setLayoutData(argumentSourceLabelFormData);

    argumentSourceFields = new Button(metadataComposite, SWT.CHECK);
    props.setLook(argumentSourceFields);
    FormData argumentSourceFieldsFormData = new FormData();
    argumentSourceFieldsFormData.left = new FormAttachment(middle, 0);
    argumentSourceFieldsFormData.right = new FormAttachment(100, 0);
    argumentSourceFieldsFormData.top = new FormAttachment(lastControl, margin);
    argumentSourceFields.setLayoutData(argumentSourceFieldsFormData);
    SelectionListener argumentSourceFieldsSelectionListener =
        new SelectionListener() {
          @Override
          public void widgetDefaultSelected(SelectionEvent selectionEvent) {}

          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            Control[] controls = metadataComposite.getChildren();
            boolean selection = argumentSourceFields.getSelection();
            removeArgumentFieldsButton.setEnabled(selection);
            String[] items = selection ? getFieldListForCombo() : emptyFieldList;
            for (Control control : controls) {
              if (!(control instanceof ComboVar)) continue;
              ComboVar comboVar = (ComboVar) control;
              comboVar.setItems(items);
            }
            meta.setChanged();
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
    props.setLook(removeArgumentFieldsLabel);
    FormData removeArgumentFieldsLabelFormData = new FormData();
    removeArgumentFieldsLabelFormData.left = new FormAttachment(0, 0);
    removeArgumentFieldsLabelFormData.right = new FormAttachment(middle, -margin);
    removeArgumentFieldsLabelFormData.top = new FormAttachment(lastControl, margin);
    removeArgumentFieldsLabel.setLayoutData(removeArgumentFieldsLabelFormData);

    removeArgumentFieldsButton = new Button(metadataComposite, SWT.CHECK);
    props.setLook(removeArgumentFieldsButton);
    FormData removeArgumentFieldsButtonFormData = new FormData();
    removeArgumentFieldsButtonFormData.left = new FormAttachment(middle, 0);
    removeArgumentFieldsButtonFormData.right = new FormAttachment(100, 0);
    removeArgumentFieldsButtonFormData.top = new FormAttachment(lastControl, margin);
    removeArgumentFieldsButton.setLayoutData(removeArgumentFieldsButtonFormData);

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
    props.setLook(fieldsComposite);
    fieldsComposite.setLayout(fieldsTabLayout);

    // add UI for the fields tab.
    Label outputFieldsTableViewLabel = new Label(fieldsComposite, SWT.NONE);
    outputFieldsTableViewLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.FieldsTab.Label"));
    outputFieldsTableViewLabel.setToolTipText(
        BaseMessages.getString(PKG, "JdbcMetadata.FieldsTab.Tooltip"));
    props.setLook(outputFieldsTableViewLabel);
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
          public void widgetDefaultSelected(SelectionEvent arg0) {}

          @Override
          public void widgetSelected(SelectionEvent arg0) {
            populateFieldsTable();
            meta.setChanged();
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
    // default listener (for hitting "enter")
    //    lsDef =
    //        new SelectionAdapter() {
    //          public void widgetDefaultSelected(SelectionEvent e) {
    //            ok();
    //          }
    //        };
    //    wTransformName.addSelectionListener(lsDef);
    //    jdbcDriverField.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window and cancel the dialog properly
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    setSize();
    populateDialog();
    meta.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void selectConnectionSource(String connectionSourceOption) {
    int index = JdbcMetadataMeta.getConnectionSourceOptionIndex(connectionSourceOption);
    wConnectionSource.select(index);
    connectionSourceUpdated();
  }

  private void setMethod(String method) {
    int index = JdbcMetadataMeta.getMethodDescriptorIndex(method);
    if (index == -1) throw new IllegalArgumentException("Index for method " + method + " is -1.");
    methodCombo.select(index);
    logBasic("setMethod called, calling parameterless method updated");
  }

  /**
   * This helper method puts the step configuration stored in the meta object and puts it into the
   * dialog controls.
   */
  private void populateDialog() {
    wTransformName.selectAll();
    String value;

    value = meta.getConnectionSource();
    selectConnectionSource(value);

    value = meta.getConnectionName();
    if (value != null) wConnectionSource.setText(value);

    value = meta.getConnectionField();
    if (value != null) connectionField.setText(value);

    value = meta.getJdbcDriverField();
    if (value != null) jdbcDriverField.setText(value);

    value = meta.getJdbcUrlField();
    if (value != null) jdbcUrlField.setText(value);

    value = meta.getJdbcUserField();
    if (value != null) jdbcUserField.setText(value);

    value = meta.getJdbcPasswordField();
    if (value != null) jdbcPasswordField.setText(value);

    alwaysPassInputRowButton.setSelection(meta.isAlwaysPassInputRow());

    value = meta.getMethodName();
    if (value != null) setMethod(value);

    argumentSourceFields.setSelection(meta.isArgumentSourceFields());
    if (meta.getArguments() != null && meta.getArguments().size() > 0) {
      methodUpdated(meta.getArguments());
    }

    logBasic("Calling methodUpdated from populate dialog.");
    if (meta.getOutputFields() != null && meta.getOutputFields().size() > 0) {
      updateOutputFields(meta.getOutputFields());
    }

    removeArgumentFieldsButton.setSelection(meta.isRemoveArgumentFields());
    removeArgumentFieldsButton.setEnabled(meta.isArgumentSourceFields());
  }

  /** Called when the user cancels the dialog. */
  private void cancel() {
    transformName = null;
    meta.setChanged(changed);
    dispose();
  }

  /** Called when the user confirms the dialog */
  private void ok() {

    transformName = wTransformName.getText();
    // Save settings to the meta object
    meta.setConnectionSource(
        JdbcMetadataMeta.connectionSourceOptions[wConnectionSource.getSelectionIndex()]);
    meta.setConnectionName(wConnectionSource.getText());
    meta.setConnectionField(connectionField.getText());
    meta.setJdbcDriverField(jdbcDriverField.getText());
    meta.setJdbcUrlField(jdbcUrlField.getText());
    meta.setJdbcUserField(jdbcUserField.getText());
    meta.setJdbcPasswordField(jdbcPasswordField.getText());
    meta.setAlwaysPassInputRow(alwaysPassInputRowButton.getSelection());
    meta.setMethodName(JdbcMetadataMeta.getMethodName(methodCombo.getSelectionIndex()));
    meta.setArgumentSourceFields(argumentSourceFields.getSelection());
    meta.setArguments(getArguments());
    meta.setRemoveArgumentFields(removeArgumentFieldsButton.getSelection());
    meta.setOutputFields(getOutputFields());

    meta.setChanged(dialogChanged || changed);
    // close the SWT dialog window
    dispose();
  }

  private List<String> getArguments() {
    logBasic("getArguments");
    List<String> arguments = new ArrayList<String>();
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