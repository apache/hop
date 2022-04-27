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

package org.apache.hop.pipeline.transforms.workflowexecutor;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.*;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WorkflowExecutorDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = WorkflowExecutorMeta.class; // For Translator

  private static final int FIELD_DESCRIPTION = 1;
  private static final int FIELD_NAME = 2;

  private WorkflowExecutorMeta workflowExecutorMeta;

  private TextVar wPath;

  protected Label wlRunConfiguration;
  protected ComboVar wRunConfiguration;

  private CTabFolder wTabFolder;

  private WorkflowMeta executorWorkflowMeta = null;

  protected boolean jobModified;

  private Button wInheritAll;

  private TableView wWorkflowExecutorParameters;

  private Label wlGroupSize;
  private TextVar wGroupSize;
  private Label wlGroupField;
  private CCombo wGroupField;
  private Label wlGroupTime;
  private TextVar wGroupTime;

  private CCombo wExecutionResultTarget;
  private TableItem tiExecutionTimeField;
  private TableItem tiExecutionResultField;
  private TableItem tiExecutionNrErrorsField;
  private TableItem tiExecutionLinesReadField;
  private TableItem tiExecutionLinesWrittenField;
  private TableItem tiExecutionLinesInputField;
  private TableItem tiExecutionLinesOutputField;
  private TableItem tiExecutionLinesRejectedField;
  private TableItem tiExecutionLinesUpdatedField;
  private TableItem tiExecutionLinesDeletedField;
  private TableItem tiExecutionFilesRetrievedField;
  private TableItem tiExecutionExitStatusField;
  private TableItem tiExecutionLogTextField;
  private TableItem tiExecutionLogChannelIdField;

  private ColumnInfo[] parameterColumns;

  private CCombo wResultFilesTarget;

  private TextVar wResultFileNameField;

  private CCombo wResultRowsTarget;

  private TableView wResultRowsFields;

  private final int middle = props.getMiddlePct();
  private final int margin = props.getMargin();

  private HopWorkflowFileType<WorkflowMeta> fileType =
      HopGui.getDataOrchestrationPerspective().getWorkflowFileType();

  public WorkflowExecutorDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname) {
    super(parent, variables, (BaseTransformMeta) in, tr, sname);
    workflowExecutorMeta = (WorkflowExecutorMeta) in;
    jobModified = false;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, workflowExecutorMeta);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "WorkflowExecutorDialog.Shell.Title"));

    Label wicon = new Label(shell, SWT.RIGHT);
    wicon.setImage(getImage());
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment(0, 0);
    fdlicon.right = new FormAttachment(100, 0);
    wicon.setLayoutData(fdlicon);
    props.setLook(wicon);

    // Some buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    positionBottomButtons(shell, new Button[] {wOk, wCancel}, props.getMargin(), null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.TransformName.Label"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(wicon, 0, SWT.CENTER);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.right = new FormAttachment(wicon, -margin);
    fdTransformName.left = new FormAttachment(wlTransformName, margin);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    wTransformName.setLayoutData(fdTransformName);

    Label spacer = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.top = new FormAttachment(wicon, 0);
    fdSpacer.right = new FormAttachment(100, 0);
    spacer.setLayoutData(fdSpacer);

    Label wlPath = new Label(shell, SWT.RIGHT);
    props.setLook(wlPath);
    wlPath.setText(BaseMessages.getString(PKG, "WorkflowExecutorDialog.Workflow.Label"));
    FormData fdlJobformation = new FormData();
    fdlJobformation.left = new FormAttachment(0, 0);
    fdlJobformation.top = new FormAttachment(spacer, 20);
    fdlJobformation.right = new FormAttachment(middle, -margin);
    wlPath.setLayoutData(fdlJobformation);

    Button wbBrowse = new Button(shell, SWT.PUSH);
    props.setLook(wbBrowse);
    wbBrowse.setText(BaseMessages.getString(PKG, "WorkflowExecutorDialog.Browse.Label"));
    FormData fdBrowse = new FormData();
    fdBrowse.right = new FormAttachment(100, 0);
    fdBrowse.top = new FormAttachment(wlPath, Const.isOSX() ? 0 : 5);
    wbBrowse.setLayoutData(fdBrowse);
    wbBrowse.addListener(SWT.Selection, e -> selectWorkflowFile());

    wPath = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPath);
    FormData fdPath = new FormData();
    fdPath.left = new FormAttachment(wlPath, margin);
    fdPath.top = new FormAttachment(wlPath, 0, SWT.CENTER);
    fdPath.right = new FormAttachment(wbBrowse, -margin);
    wPath.setLayoutData(fdPath);

    wlRunConfiguration = new Label(shell, SWT.RIGHT);
    wlRunConfiguration.setText(
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.RunConfiguration.Label"));
    props.setLook(wlRunConfiguration);
    FormData fdlRunConfiguration = new FormData();
    fdlRunConfiguration.left = new FormAttachment(0, 0);
    fdlRunConfiguration.top = new FormAttachment(wPath, margin);
    fdlRunConfiguration.right = new FormAttachment(middle, -margin);
    wlRunConfiguration.setLayoutData(fdlRunConfiguration);

    wRunConfiguration = new ComboVar(variables, shell, SWT.LEFT | SWT.BORDER);
    props.setLook(wlRunConfiguration);
    FormData fdRunConfiguration = new FormData();
    fdRunConfiguration.left = new FormAttachment(middle, margin);
    fdRunConfiguration.top = new FormAttachment(wlRunConfiguration, 0, SWT.CENTER);
    fdRunConfiguration.right = new FormAttachment(wbBrowse, -margin);
    wRunConfiguration.setLayoutData(fdRunConfiguration);
    props.setLook(wRunConfiguration);

    //
    // Add a tab folder for the parameters and various input and output
    // streams
    //
    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);
    wTabFolder.setUnselectedCloseVisible(true);

    Label hSpacer = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdhSpacer = new FormData();
    fdhSpacer.left = new FormAttachment(0, 0);
    fdhSpacer.bottom = new FormAttachment(wCancel, -15);
    fdhSpacer.right = new FormAttachment(100, 0);
    hSpacer.setLayoutData(fdhSpacer);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wRunConfiguration, 20);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(hSpacer, -15);
    wTabFolder.setLayoutData(fdTabFolder);

    // Add the tabs...
    //
    addParametersTab();
    addExecutionResultTab();
    addRowGroupTab();
    addResultRowsTab();
    addResultFilesTab();

    getData();
    workflowExecutorMeta.setChanged(changed);
    wTabFolder.setSelection(0);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  protected Image getImage() {
    return SwtSvgImageUtil.getImage(
        shell.getDisplay(),
        getClass().getClassLoader(),
        "ui/images/workflowexecutor.svg",
        ConstUi.LARGE_ICON_SIZE,
        ConstUi.LARGE_ICON_SIZE);
  }

  private void selectWorkflowFile() {
    String curFile = variables.resolve(wPath.getText());

    String parentFolder = null;
    try {
      parentFolder =
          HopVfs.getFileObject(variables.resolve(pipelineMeta.getFilename()))
              .getParent()
              .toString();
    } catch (Exception e) {
      // Take no action
    }

    try {
      String filename =
          BaseDialog.presentFileDialog(
              shell,
              wPath,
              variables,new String[0], new String[0],
//              fileType.getFilterExtensions(),
//              fileType.getFilterNames(),
              true);
      if (filename != null) {

        loadWorkflowFile(filename);
        if (parentFolder != null && filename.startsWith(parentFolder)) {
          filename =
              filename.replace(
                  parentFolder, "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER + "}");
        }
        wPath.setText(filename);
      }
      if (filename != null) {
        replaceNameWithBaseFilename(filename);
      }
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "WorkflowExecutorDialog.ErrorLoadingWorkflow.DialogTitle"),
          BaseMessages.getString(PKG, "WorkflowExecutorDialog.ErrorLoadingWorkflow.DialogMessage"),
          e);
    }
  }

  private void loadWorkflowFile(String fname) throws HopException {
    executorWorkflowMeta = new WorkflowMeta(variables.resolve(fname));
    executorWorkflowMeta.clearChanged();
  }

  private void loadWorkflow() throws HopException {
    String filename = wPath.getText();
    if (Utils.isEmpty(filename)) {
      return;
    }
    if (!filename.endsWith(".hwf")) {
      filename = filename + ".hwf";
      wPath.setText(filename);
    }
    loadWorkflowFile(filename);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wPath.setText(Const.NVL(workflowExecutorMeta.getFilename(), ""));

    try {
      List<String> runConfigurations =
          metadataProvider.getSerializer(WorkflowRunConfiguration.class).listObjectNames();

      try {
        ExtensionPointHandler.callExtensionPoint(
            HopGui.getInstance().getLog(),
            variables,
            HopExtensionPoint.HopGuiRunConfiguration.id,
            new Object[] {runConfigurations, WorkflowMeta.XML_TAG});
      } catch (HopException e) {
        // Ignore errors
      }

      wRunConfiguration.setItems(runConfigurations.toArray(new String[0]));
      wRunConfiguration.setText(Const.NVL(workflowExecutorMeta.getRunConfigurationName(), ""));

      if (Utils.isEmpty(workflowExecutorMeta.getRunConfigurationName())) {
        wRunConfiguration.select(0);
      } else {
        wRunConfiguration.setText(workflowExecutorMeta.getRunConfigurationName());
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting workflow run configurations", e);
    }

    // TODO: throw in a separate thread.
    //
    try {
      String[] prevTransforms = pipelineMeta.getTransformNames();
      Arrays.sort(prevTransforms);
      wExecutionResultTarget.setItems(prevTransforms);
      wResultFilesTarget.setItems(prevTransforms);
      wResultRowsTarget.setItems(prevTransforms);

      String[] inputFields =
          pipelineMeta.getPrevTransformFields(variables, transformMeta).getFieldNames();
      parameterColumns[1].setComboValues(inputFields);
      wGroupField.setItems(inputFields);
    } catch (Exception e) {
      log.logError("couldn't get previous transform list", e);
    }

    wGroupSize.setText(Const.NVL(workflowExecutorMeta.getGroupSize(), ""));
    wGroupTime.setText(Const.NVL(workflowExecutorMeta.getGroupTime(), ""));
    wGroupField.setText(Const.NVL(workflowExecutorMeta.getGroupField(), ""));

    wExecutionResultTarget.setText(
        workflowExecutorMeta.getExecutionResultTargetTransformMeta() == null
            ? ""
            : workflowExecutorMeta.getExecutionResultTargetTransformMeta().getName());
    tiExecutionTimeField.setText(
        FIELD_NAME, Const.NVL(workflowExecutorMeta.getExecutionTimeField(), ""));
    tiExecutionResultField.setText(
        FIELD_NAME, Const.NVL(workflowExecutorMeta.getExecutionResultField(), ""));
    tiExecutionNrErrorsField.setText(
        FIELD_NAME, Const.NVL(workflowExecutorMeta.getExecutionNrErrorsField(), ""));
    tiExecutionLinesReadField.setText(
        FIELD_NAME, Const.NVL(workflowExecutorMeta.getExecutionLinesReadField(), ""));
    tiExecutionLinesWrittenField.setText(
        FIELD_NAME, Const.NVL(workflowExecutorMeta.getExecutionLinesWrittenField(), ""));
    tiExecutionLinesInputField.setText(
        FIELD_NAME, Const.NVL(workflowExecutorMeta.getExecutionLinesInputField(), ""));
    tiExecutionLinesOutputField.setText(
        FIELD_NAME, Const.NVL(workflowExecutorMeta.getExecutionLinesOutputField(), ""));
    tiExecutionLinesRejectedField.setText(
        FIELD_NAME, Const.NVL(workflowExecutorMeta.getExecutionLinesRejectedField(), ""));
    tiExecutionLinesUpdatedField.setText(
        FIELD_NAME, Const.NVL(workflowExecutorMeta.getExecutionLinesUpdatedField(), ""));
    tiExecutionLinesDeletedField.setText(
        FIELD_NAME, Const.NVL(workflowExecutorMeta.getExecutionLinesDeletedField(), ""));
    tiExecutionFilesRetrievedField.setText(
        FIELD_NAME, Const.NVL(workflowExecutorMeta.getExecutionFilesRetrievedField(), ""));
    tiExecutionExitStatusField.setText(
        FIELD_NAME, Const.NVL(workflowExecutorMeta.getExecutionExitStatusField(), ""));
    tiExecutionLogTextField.setText(
        FIELD_NAME, Const.NVL(workflowExecutorMeta.getExecutionLogTextField(), ""));
    tiExecutionLogChannelIdField.setText(
        FIELD_NAME, Const.NVL(workflowExecutorMeta.getExecutionLogChannelIdField(), ""));

    // result files
    //
    wResultFilesTarget.setText(
        workflowExecutorMeta.getResultFilesTargetTransformMeta() == null
            ? ""
            : workflowExecutorMeta.getResultFilesTargetTransformMeta().getName());
    wResultFileNameField.setText(Const.NVL(workflowExecutorMeta.getResultFilesFileNameField(), ""));

    // Result rows
    //
    wResultRowsTarget.setText(
        workflowExecutorMeta.getResultRowsTargetTransformMeta() == null
            ? ""
            : workflowExecutorMeta.getResultRowsTargetTransformMeta().getName());
    for (int i = 0; i < workflowExecutorMeta.getResultRowsField().length; i++) {
      TableItem item = new TableItem(wResultRowsFields.table, SWT.NONE);
      item.setText(1, Const.NVL(workflowExecutorMeta.getResultRowsField()[i], ""));
      item.setText(
          2, ValueMetaFactory.getValueMetaName(workflowExecutorMeta.getResultRowsType()[i]));
      int length = workflowExecutorMeta.getResultRowsLength()[i];
      item.setText(3, length < 0 ? "" : Integer.toString(length));
      int precision = workflowExecutorMeta.getResultRowsPrecision()[i];
      item.setText(4, precision < 0 ? "" : Integer.toString(precision));
    }
    wResultRowsFields.removeEmptyRows();
    wResultRowsFields.setRowNums();
    wResultRowsFields.optWidth(true);

    wTabFolder.setSelection(0);

    try {
      loadWorkflow();
    } catch (Throwable t) {
      // Ignore errors
    }

    setFlags();

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void addParametersTab() {
    CTabItem wParametersTab = new CTabItem(wTabFolder, SWT.NONE);
    wParametersTab.setText(BaseMessages.getString(PKG, "WorkflowExecutorDialog.Parameters.Title"));
    wParametersTab.setToolTipText(
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.Parameters.Tooltip"));

    Composite wParametersComposite = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wParametersComposite);

    FormLayout parameterTabLayout = new FormLayout();
    parameterTabLayout.marginWidth = 15;
    parameterTabLayout.marginHeight = 15;
    wParametersComposite.setLayout(parameterTabLayout);

    // Add a button: get parameters
    //
    Button wGetParameters = new Button(wParametersComposite, SWT.PUSH);
    wGetParameters.setText(
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.Parameters.GetParameters"));
    props.setLook(wGetParameters);
    FormData fdGetParameters = new FormData();
    fdGetParameters.bottom = new FormAttachment(100, 0);
    fdGetParameters.right = new FormAttachment(100, 0);
    wGetParameters.setLayoutData(fdGetParameters);
    wGetParameters.setSelection(workflowExecutorMeta.getParameters().isInheritingAllVariables());
    wGetParameters.addListener(
        SWT.Selection, e -> getParametersFromWorkflow()); // null : reload file

    // Add a button: map parameters
    //
    Button wMapParameters = new Button(wParametersComposite, SWT.PUSH);
    wMapParameters.setText(
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.Parameters.MapParameters"));
    props.setLook(wMapParameters);
    FormData fdMapParameters = new FormData();
    fdMapParameters.bottom = new FormAttachment(100, 0);
    fdMapParameters.right = new FormAttachment(wGetParameters, -props.getMargin());
    wMapParameters.setLayoutData(fdMapParameters);
    wMapParameters.setSelection(workflowExecutorMeta.getParameters().isInheritingAllVariables());
    wMapParameters.addListener(SWT.Selection, e -> mapFieldsToWorkflowParameters());

    // Now add a table view with the 3 columns to specify: variable name, input field & optional
    // static input
    //
    parameterColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "WorkflowExecutorDialog.Parameters.column.Variable"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "WorkflowExecutorDialog.Parameters.column.Field"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "WorkflowExecutorDialog.Parameters.column.Input"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };
    parameterColumns[1].setUsingVariables(true);

    WorkflowExecutorParameters parameters = workflowExecutorMeta.getParameters();
    wWorkflowExecutorParameters =
        new TableView(
            variables,
            wParametersComposite,
            SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER,
            parameterColumns,
            parameters.getVariable().length,
            null,
            props);
    props.setLook(wWorkflowExecutorParameters);
    FormData fdJobExecutors = new FormData();
    fdJobExecutors.left = new FormAttachment(0, 0);
    fdJobExecutors.right = new FormAttachment(100, 0);
    fdJobExecutors.top = new FormAttachment(0, 0);
    fdJobExecutors.bottom = new FormAttachment(wGetParameters, -10);
    wWorkflowExecutorParameters.setLayoutData(fdJobExecutors);

    for (int i = 0; i < parameters.getVariable().length; i++) {
      TableItem tableItem = wWorkflowExecutorParameters.table.getItem(i);
      tableItem.setText(1, Const.NVL(parameters.getVariable()[i], ""));
      tableItem.setText(2, Const.NVL(parameters.getField()[i], ""));
      tableItem.setText(3, Const.NVL(parameters.getInput()[i], ""));
    }
    wWorkflowExecutorParameters.setRowNums();
    wWorkflowExecutorParameters.optWidth(true);

    // Add a checkbox: inherit all variables...
    //
    wInheritAll = new Button(wParametersComposite, SWT.CHECK);
    wInheritAll.setText(
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.Parameters.InheritAll"));
    props.setLook(wInheritAll);
    FormData fdInheritAll = new FormData();
    fdInheritAll.left = new FormAttachment(0, 0);
    fdInheritAll.top = new FormAttachment(wWorkflowExecutorParameters, 15);
    wInheritAll.setLayoutData(fdInheritAll);
    wInheritAll.setSelection(workflowExecutorMeta.getParameters().isInheritingAllVariables());

    FormData fdParametersComposite = new FormData();
    fdParametersComposite.left = new FormAttachment(0, 0);
    fdParametersComposite.top = new FormAttachment(0, 0);
    fdParametersComposite.right = new FormAttachment(100, 0);
    fdParametersComposite.bottom = new FormAttachment(100, 0);
    wParametersComposite.setLayoutData(fdParametersComposite);

    wParametersComposite.layout();
    wParametersTab.setControl(wParametersComposite);
  }

  protected void getParametersFromWorkflow() {
    try {
      loadWorkflow();
      String[] parameters = executorWorkflowMeta.listParameters();
      for (String parameter : parameters) {
        TableItem item = new TableItem(wWorkflowExecutorParameters.table, SWT.NONE);
        item.setText(1, Const.NVL(parameter, ""));
      }
      wWorkflowExecutorParameters.removeEmptyRows();
      wWorkflowExecutorParameters.setRowNums();
      wWorkflowExecutorParameters.optWidth(true);

    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "WorkflowExecutorDialog.ErrorLoadingSpecifiedJob.Title"),
          BaseMessages.getString(PKG, "WorkflowExecutorDialog.ErrorLoadingSpecifiedJob.Message"),
          e);
    }
  }

  protected void mapFieldsToWorkflowParameters() {
    try {
      // The field names
      //
      IRowMeta inputFields = pipelineMeta.getPrevTransformFields(variables, transformMeta);
      String[] inputFieldNames = inputFields.getFieldNames();

      loadWorkflow();
      String[] parameters = executorWorkflowMeta.listParameters();

      // Get the current mapping...
      //
      List<SourceToTargetMapping> mappings = new ArrayList<>();
      for (TableItem item : wWorkflowExecutorParameters.getNonEmptyItems()) {
        int sourceIndex = Const.indexOfString(item.getText(1), parameters);
        int targetIndex = Const.indexOfString(item.getText(2), inputFieldNames);
        if (sourceIndex >= 0 && targetIndex >= 0) {
          SourceToTargetMapping mapping = new SourceToTargetMapping(sourceIndex, targetIndex);
          mappings.add(mapping);
        }
      }

      // Now we can ask for the mapping...
      //
      EnterMappingDialog enterMappingDialog =
          new EnterMappingDialog(shell, inputFieldNames, parameters, mappings);
      mappings = enterMappingDialog.open();
      if (mappings != null) {
        wWorkflowExecutorParameters.removeAll();

        for (SourceToTargetMapping mapping : mappings) {
          TableItem item = new TableItem(wWorkflowExecutorParameters.table, SWT.NONE);
          item.setText(1, Const.NVL(mapping.getTargetString(parameters), ""));
          item.setText(2, Const.NVL(mapping.getSourceString(inputFieldNames), ""));
          item.setText(3, "");
        }

        wWorkflowExecutorParameters.removeEmptyRows();
        wWorkflowExecutorParameters.setRowNums();
        wWorkflowExecutorParameters.optWidth(true);
      }
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "WorkflowExecutorDialog.ErrorLoadingSpecifiedJob.Title"),
          BaseMessages.getString(PKG, "WorkflowExecutorDialog.ErrorLoadingSpecifiedJob.Message"),
          e);
    }
  }

  private void addRowGroupTab() {

    final CTabItem wTab = new CTabItem(wTabFolder, SWT.NONE);
    wTab.setText(BaseMessages.getString(PKG, "WorkflowExecutorDialog.RowGroup.Title"));
    wTab.setToolTipText(BaseMessages.getString(PKG, "WorkflowExecutorDialog.RowGroup.Tooltip"));

    Composite wInputComposite = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wInputComposite);

    FormLayout tabLayout = new FormLayout();
    tabLayout.marginWidth = 15;
    tabLayout.marginHeight = 15;
    wInputComposite.setLayout(tabLayout);

    // Group size
    //
    wlGroupSize = new Label(wInputComposite, SWT.RIGHT);
    props.setLook(wlGroupSize);
    wlGroupSize.setText(BaseMessages.getString(PKG, "WorkflowExecutorDialog.GroupSize.Label"));
    FormData fdlGroupSize = new FormData();
    fdlGroupSize.top = new FormAttachment(0, 0);
    fdlGroupSize.left = new FormAttachment(0, 0);
    fdlGroupSize.right = new FormAttachment(middle, -margin);
    wlGroupSize.setLayoutData(fdlGroupSize);

    wGroupSize = new TextVar(variables, wInputComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wGroupSize);
    FormData fdGroupSize = new FormData();
    fdGroupSize.width = 250;
    fdGroupSize.top = new FormAttachment(wlGroupSize, 0, SWT.CENTER);
    fdGroupSize.left = new FormAttachment(middle, margin);
    fdGroupSize.right = new FormAttachment(100, 0);
    wGroupSize.setLayoutData(fdGroupSize);
    // Enable/Disable fields base on the size of the group
    wGroupSize.addListener(SWT.Modify, e -> setFlags());

    // Group field
    //
    wlGroupField = new Label(wInputComposite, SWT.RIGHT);
    props.setLook(wlGroupField);
    wlGroupField.setText(BaseMessages.getString(PKG, "WorkflowExecutorDialog.GroupField.Label"));
    FormData fdlGroupField = new FormData();
    fdlGroupField.top = new FormAttachment(wGroupSize, 10);
    fdlGroupField.left = new FormAttachment(0, 0);
    fdlGroupField.right = new FormAttachment(middle, -margin);
    wlGroupField.setLayoutData(fdlGroupField);

    wGroupField = new CCombo(wInputComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wGroupField);
    FormData fdGroupField = new FormData();
    fdGroupField.width = 250;
    fdGroupField.top = new FormAttachment(wlGroupField, 0, SWT.CENTER);
    fdGroupField.left = new FormAttachment(middle, margin);
    fdGroupField.right = new FormAttachment(100, 0);
    wGroupField.setLayoutData(fdGroupField);
    // Enable/Disable widgets when this field changes
    wGroupField.addListener(SWT.Modify, e -> setFlags());

    // Group time
    //
    wlGroupTime = new Label(wInputComposite, SWT.RIGHT);
    props.setLook(wlGroupTime);
    wlGroupTime.setText(BaseMessages.getString(PKG, "WorkflowExecutorDialog.GroupTime.Label"));
    FormData fdlGroupTime = new FormData();
    fdlGroupTime.top = new FormAttachment(wGroupField, 10);
    fdlGroupTime.left = new FormAttachment(0, 0);
    fdlGroupTime.right = new FormAttachment(middle, -margin);
    wlGroupTime.setLayoutData(fdlGroupTime);

    wGroupTime = new TextVar(variables, wInputComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wGroupTime);
    FormData fdGroupTime = new FormData();
    fdGroupTime.width = 250;
    fdGroupTime.top = new FormAttachment(wlGroupTime, 0, SWT.CENTER);
    fdGroupTime.left = new FormAttachment(middle, margin);
    fdGroupTime.right = new FormAttachment(100, 0);
    wGroupTime.setLayoutData(fdGroupTime);

    wTab.setControl(wInputComposite);
    wTabFolder.setSelection(wTab);
  }

  private void addExecutionResultTab() {

    final CTabItem wTab = new CTabItem(wTabFolder, SWT.NONE);
    wTab.setText(BaseMessages.getString(PKG, "WorkflowExecutorDialog.ExecutionResults.Title"));
    wTab.setToolTipText(
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ExecutionResults.Tooltip"));

    ScrolledComposite scrolledComposite =
        new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    scrolledComposite.setLayout(new FillLayout());

    Composite wInputComposite = new Composite(scrolledComposite, SWT.NONE);
    props.setLook(wInputComposite);

    FormLayout tabLayout = new FormLayout();
    tabLayout.marginWidth = 15;
    tabLayout.marginHeight = 15;
    wInputComposite.setLayout(tabLayout);

    Label wlExecutionResultTarget = new Label(wInputComposite, SWT.RIGHT);
    props.setLook(wlExecutionResultTarget);
    wlExecutionResultTarget.setText(
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ExecutionResultTarget.Label"));
    FormData fdlExecutionResultTarget = new FormData();
    fdlExecutionResultTarget.top = new FormAttachment(0, 0);
    fdlExecutionResultTarget.left = new FormAttachment(0, 0);
    fdlExecutionResultTarget.right = new FormAttachment(middle, -margin);
    wlExecutionResultTarget.setLayoutData(fdlExecutionResultTarget);

    wExecutionResultTarget = new CCombo(wInputComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wExecutionResultTarget);
    FormData fdExecutionResultTarget = new FormData();
    fdExecutionResultTarget.right = new FormAttachment(100, 0);
    fdExecutionResultTarget.top = new FormAttachment(wlExecutionResultTarget, 0, SWT.CENTER);
    fdExecutionResultTarget.left = new FormAttachment(wlExecutionResultTarget, margin);
    wExecutionResultTarget.setLayoutData(fdExecutionResultTarget);

    ColumnInfo[] executionResultColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "WorkflowExecutorMeta.ExecutionResults.FieldDescription.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "WorkflowExecutorMeta.ExecutionResults.FieldName.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false)
        };
    executionResultColumns[1].setUsingVariables(true);

    TableView wExecutionResults =
        new TableView(
            variables,
            wInputComposite,
            SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER,
            executionResultColumns,
            14,
            true,
            null,
            props,
            false);
    props.setLook(wExecutionResults);
    FormData fdExecutionResults = new FormData();
    fdExecutionResults.left = new FormAttachment(0);
    fdExecutionResults.right = new FormAttachment(100);
    fdExecutionResults.top = new FormAttachment(wExecutionResultTarget, 10);
    fdExecutionResults.bottom = new FormAttachment(100);
    wExecutionResults.setLayoutData(fdExecutionResults);
    wExecutionResults.getTable().addListener(SWT.Resize, new ColumnsResizer(0, 50, 50));

    int index = 0;
    tiExecutionTimeField = wExecutionResults.table.getItem(index++);
    tiExecutionResultField = wExecutionResults.table.getItem(index++);
    tiExecutionNrErrorsField = wExecutionResults.table.getItem(index++);
    tiExecutionLinesReadField = wExecutionResults.table.getItem(index++);
    tiExecutionLinesWrittenField = wExecutionResults.table.getItem(index++);
    tiExecutionLinesInputField = wExecutionResults.table.getItem(index++);
    tiExecutionLinesOutputField = wExecutionResults.table.getItem(index++);
    tiExecutionLinesRejectedField = wExecutionResults.table.getItem(index++);
    tiExecutionLinesUpdatedField = wExecutionResults.table.getItem(index++);
    tiExecutionLinesDeletedField = wExecutionResults.table.getItem(index++);
    tiExecutionFilesRetrievedField = wExecutionResults.table.getItem(index++);
    tiExecutionExitStatusField = wExecutionResults.table.getItem(index++);
    tiExecutionLogTextField = wExecutionResults.table.getItem(index++);
    tiExecutionLogChannelIdField = wExecutionResults.table.getItem(index++);

    tiExecutionTimeField.setText(
        FIELD_DESCRIPTION,
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ExecutionTimeField.Label"));
    tiExecutionResultField.setText(
        FIELD_DESCRIPTION,
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ExecutionResultField.Label"));
    tiExecutionNrErrorsField.setText(
        FIELD_DESCRIPTION,
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ExecutionNrErrorsField.Label"));
    tiExecutionLinesReadField.setText(
        FIELD_DESCRIPTION,
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ExecutionLinesReadField.Label"));
    tiExecutionLinesWrittenField.setText(
        FIELD_DESCRIPTION,
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ExecutionLinesWrittenField.Label"));
    tiExecutionLinesInputField.setText(
        FIELD_DESCRIPTION,
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ExecutionLinesInputField.Label"));
    tiExecutionLinesOutputField.setText(
        FIELD_DESCRIPTION,
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ExecutionLinesOutputField.Label"));
    tiExecutionLinesRejectedField.setText(
        FIELD_DESCRIPTION,
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ExecutionLinesRejectedField.Label"));
    tiExecutionLinesUpdatedField.setText(
        FIELD_DESCRIPTION,
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ExecutionLinesUpdatedField.Label"));
    tiExecutionLinesDeletedField.setText(
        FIELD_DESCRIPTION,
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ExecutionLinesDeletedField.Label"));
    tiExecutionFilesRetrievedField.setText(
        FIELD_DESCRIPTION,
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ExecutionFilesRetrievedField.Label"));
    tiExecutionExitStatusField.setText(
        FIELD_DESCRIPTION,
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ExecutionExitStatusField.Label"));
    tiExecutionLogTextField.setText(
        FIELD_DESCRIPTION,
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ExecutionLogTextField.Label"));
    tiExecutionLogChannelIdField.setText(
        FIELD_DESCRIPTION,
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ExecutionLogChannelIdField.Label"));

    wWorkflowExecutorParameters.setRowNums();
    wWorkflowExecutorParameters.optWidth(true);

    wInputComposite.pack();
    Rectangle bounds = wInputComposite.getBounds();

    scrolledComposite.setContent(wInputComposite);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setMinWidth(bounds.width);
    scrolledComposite.setMinHeight(bounds.height);

    wTab.setControl(scrolledComposite);
    wTabFolder.setSelection(wTab);
  }

  private void addResultFilesTab() {
    final CTabItem wTab = new CTabItem(wTabFolder, SWT.NONE);
    wTab.setText(BaseMessages.getString(PKG, "WorkflowExecutorDialog.ResultFiles.Title"));
    wTab.setToolTipText(BaseMessages.getString(PKG, "WorkflowExecutorDialog.ResultFiles.Tooltip"));

    ScrolledComposite scrolledComposite =
        new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    scrolledComposite.setLayout(new FillLayout());

    Composite wInputComposite = new Composite(scrolledComposite, SWT.NONE);
    props.setLook(wInputComposite);

    FormLayout tabLayout = new FormLayout();
    tabLayout.marginWidth = 15;
    tabLayout.marginHeight = 15;
    wInputComposite.setLayout(tabLayout);

    Label wlResultFilesTarget = new Label(wInputComposite, SWT.RIGHT);
    props.setLook(wlResultFilesTarget);
    wlResultFilesTarget.setText(
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ResultFilesTarget.Label"));
    FormData fdlResultFilesTarget = new FormData();
    fdlResultFilesTarget.top = new FormAttachment(0, 0);
    fdlResultFilesTarget.left = new FormAttachment(0, 0);
    fdlResultFilesTarget.right = new FormAttachment(middle, -margin);
    wlResultFilesTarget.setLayoutData(fdlResultFilesTarget);

    wResultFilesTarget = new CCombo(wInputComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wResultFilesTarget);
    FormData fdResultFilesTarget = new FormData();
    fdResultFilesTarget.width = 250;
    fdResultFilesTarget.top = new FormAttachment(wlResultFilesTarget, 0, SWT.CENTER);
    fdResultFilesTarget.left = new FormAttachment(middle, margin);
    fdResultFilesTarget.right = new FormAttachment(100, 0);
    wResultFilesTarget.setLayoutData(fdResultFilesTarget);

    // ResultFileNameField
    //
    Label wlResultFileNameField = new Label(wInputComposite, SWT.RIGHT);
    props.setLook(wlResultFileNameField);
    wlResultFileNameField.setText(
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ResultFileNameField.Label"));
    FormData fdlResultFileNameField = new FormData();
    fdlResultFileNameField.top = new FormAttachment(wResultFilesTarget, 10);
    fdlResultFileNameField.left = new FormAttachment(0, 0); // First one in the left
    fdlResultFileNameField.right = new FormAttachment(middle, -margin);
    wlResultFileNameField.setLayoutData(fdlResultFileNameField);

    wResultFileNameField =
        new TextVar(variables, wInputComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wResultFileNameField);
    FormData fdResultFileNameField = new FormData();
    fdResultFileNameField.width = 250;
    fdResultFileNameField.top = new FormAttachment(wlResultFileNameField, 0, SWT.CENTER);
    fdResultFileNameField.left = new FormAttachment(middle, margin);
    fdResultFileNameField.right = new FormAttachment(100, 0);
    wResultFileNameField.setLayoutData(fdResultFileNameField);

    wInputComposite.pack();
    Rectangle bounds = wInputComposite.getBounds();

    scrolledComposite.setContent(wInputComposite);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setMinWidth(bounds.width);
    scrolledComposite.setMinHeight(bounds.height);

    wTab.setControl(scrolledComposite);
    wTabFolder.setSelection(wTab);
  }

  private void addResultRowsTab() {

    final CTabItem wTab = new CTabItem(wTabFolder, SWT.NONE);
    wTab.setText(BaseMessages.getString(PKG, "WorkflowExecutorDialog.ResultRows.Title"));
    wTab.setToolTipText(BaseMessages.getString(PKG, "WorkflowExecutorDialog.ResultRows.Tooltip"));

    ScrolledComposite scrolledComposite =
        new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    scrolledComposite.setLayout(new FillLayout());

    Composite wInputComposite = new Composite(scrolledComposite, SWT.NONE);
    props.setLook(wInputComposite);

    FormLayout tabLayout = new FormLayout();
    tabLayout.marginWidth = 15;
    tabLayout.marginHeight = 15;
    wInputComposite.setLayout(tabLayout);

    Label wlResultRowsTarget = new Label(wInputComposite, SWT.RIGHT);
    props.setLook(wlResultRowsTarget);
    wlResultRowsTarget.setText(
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ResultRowsTarget.Label"));
    FormData fdlResultRowsTarget = new FormData();
    fdlResultRowsTarget.top = new FormAttachment(0, 0);
    fdlResultRowsTarget.left = new FormAttachment(0, 0);
    fdlResultRowsTarget.right = new FormAttachment(middle, -margin);
    wlResultRowsTarget.setLayoutData(fdlResultRowsTarget);

    wResultRowsTarget = new CCombo(wInputComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wResultRowsTarget);
    FormData fdResultRowsTarget = new FormData();
    fdResultRowsTarget.width = 250;
    fdResultRowsTarget.top = new FormAttachment(wlResultRowsTarget, 0, SWT.CENTER);
    fdResultRowsTarget.left = new FormAttachment(middle, margin);
    fdResultRowsTarget.right = new FormAttachment(100, 0);
    wResultRowsTarget.setLayoutData(fdResultRowsTarget);

    Label wlResultFields = new Label(wInputComposite, SWT.NONE);
    wlResultFields.setText(
        BaseMessages.getString(PKG, "WorkflowExecutorDialog.ResultFields.Label"));
    props.setLook(wlResultFields);
    FormData fdlResultFields = new FormData();
    fdlResultFields.left = new FormAttachment(0, 0);
    fdlResultFields.top = new FormAttachment(wResultRowsTarget, 10);
    wlResultFields.setLayoutData(fdlResultFields);

    int nrRows =
        (workflowExecutorMeta.getResultRowsField() != null
            ? workflowExecutorMeta.getResultRowsField().length
            : 1);

    ColumnInfo[] ciResultFields =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "WorkflowExecutorDialog.ColumnInfo.Field"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "WorkflowExecutorDialog.ColumnInfo.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "WorkflowExecutorDialog.ColumnInfo.Length"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "WorkflowExecutorDialog.ColumnInfo.Precision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };

    wResultRowsFields =
        new TableView(
            variables,
            wInputComposite,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciResultFields,
            nrRows,
            false,
            null,
            props,
            false);

    FormData fdResultFields = new FormData();
    fdResultFields.left = new FormAttachment(0, 0);
    fdResultFields.top = new FormAttachment(wlResultFields, 5);
    fdResultFields.right = new FormAttachment(100, 0);
    fdResultFields.bottom = new FormAttachment(100, 0);
    wResultRowsFields.setLayoutData(fdResultFields);
    wResultRowsFields.getTable().addListener(SWT.Resize, new ColumnsResizer(0, 25, 25, 25, 25));

    wInputComposite.pack();
    Rectangle bounds = wInputComposite.getBounds();

    scrolledComposite.setContent(wInputComposite);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setMinWidth(bounds.width);
    scrolledComposite.setMinHeight(bounds.height);

    wTab.setControl(scrolledComposite);
    wTabFolder.setSelection(wTab);
  }

  private void setFlags() {
    // Enable/disable fields...
    //
    if (wlGroupSize == null
        || wlGroupField == null
        || wGroupField == null
        || wlGroupTime == null
        || wGroupTime == null) {
      return;
    }
    boolean enableSize = Const.toInt(variables.resolve(wGroupSize.getText()), -1) >= 0;
    boolean enableField = !Utils.isEmpty(wGroupField.getText());

    wlGroupSize.setEnabled(true);
    wGroupSize.setEnabled(true);
    wlGroupField.setEnabled(!enableSize);
    wGroupField.setEnabled(!enableSize);
    wlGroupTime.setEnabled(!enableSize && !enableField);
    wGroupTime.setEnabled(!enableSize && !enableField);
  }

  private void cancel() {
    transformName = null;
    workflowExecutorMeta.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    try {
      loadWorkflow();
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "WorkflowExecutorDialog.ErrorLoadingSpecifiedJob.Title"),
          BaseMessages.getString(PKG, "WorkflowExecutorDialog.ErrorLoadingSpecifiedJob.Message"),
          e);
    }

    workflowExecutorMeta.setFilename(wPath.getText());
    workflowExecutorMeta.setRunConfigurationName(wRunConfiguration.getText());

    // Load the information on the tabs, optionally do some
    // verifications...
    //
    collectInformation();

    // Set the input transforms for input mappings
    workflowExecutorMeta.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());

    workflowExecutorMeta.setChanged(true);

    dispose();
  }

  private void collectInformation() {
    // The parameters...
    //
    WorkflowExecutorParameters parameters = workflowExecutorMeta.getParameters();

    int nrLines = wWorkflowExecutorParameters.nrNonEmpty();
    String[] variables = new String[nrLines];
    String[] fields = new String[nrLines];
    String[] input = new String[nrLines];
    parameters.setVariable(variables);
    parameters.setField(fields);
    parameters.setInput(input);
    for (int i = 0; i < nrLines; i++) {
      TableItem item = wWorkflowExecutorParameters.getNonEmpty(i);
      variables[i] = item.getText(1);
      fields[i] = item.getText(2);
      input[i] = item.getText(3);
    }
    parameters.setInheritingAllVariables(wInheritAll.getSelection());

    // The group definition
    //
    workflowExecutorMeta.setGroupSize(wGroupSize.getText());
    workflowExecutorMeta.setGroupField(wGroupField.getText());
    workflowExecutorMeta.setGroupTime(wGroupTime.getText());

    workflowExecutorMeta.setExecutionResultTargetTransform(wExecutionResultTarget.getText());
    workflowExecutorMeta.setExecutionResultTargetTransformMeta(
        pipelineMeta.findTransform(wExecutionResultTarget.getText()));
    workflowExecutorMeta.setExecutionTimeField(tiExecutionTimeField.getText(FIELD_NAME));
    workflowExecutorMeta.setExecutionResultField(tiExecutionResultField.getText(FIELD_NAME));
    workflowExecutorMeta.setExecutionNrErrorsField(tiExecutionNrErrorsField.getText(FIELD_NAME));
    workflowExecutorMeta.setExecutionLinesReadField(tiExecutionLinesReadField.getText(FIELD_NAME));
    workflowExecutorMeta.setExecutionLinesWrittenField(
        tiExecutionLinesWrittenField.getText(FIELD_NAME));
    workflowExecutorMeta.setExecutionLinesInputField(
        tiExecutionLinesInputField.getText(FIELD_NAME));
    workflowExecutorMeta.setExecutionLinesOutputField(
        tiExecutionLinesOutputField.getText(FIELD_NAME));
    workflowExecutorMeta.setExecutionLinesRejectedField(
        tiExecutionLinesRejectedField.getText(FIELD_NAME));
    workflowExecutorMeta.setExecutionLinesUpdatedField(
        tiExecutionLinesUpdatedField.getText(FIELD_NAME));
    workflowExecutorMeta.setExecutionLinesDeletedField(
        tiExecutionLinesDeletedField.getText(FIELD_NAME));
    workflowExecutorMeta.setExecutionFilesRetrievedField(
        tiExecutionFilesRetrievedField.getText(FIELD_NAME));
    workflowExecutorMeta.setExecutionExitStatusField(
        tiExecutionExitStatusField.getText(FIELD_NAME));
    workflowExecutorMeta.setExecutionLogTextField(tiExecutionLogTextField.getText(FIELD_NAME));
    workflowExecutorMeta.setExecutionLogChannelIdField(
        tiExecutionLogChannelIdField.getText(FIELD_NAME));

    workflowExecutorMeta.setResultFilesTargetTransform(wResultFilesTarget.getText());
    workflowExecutorMeta.setResultFilesTargetTransformMeta(
        pipelineMeta.findTransform(wResultFilesTarget.getText()));

    workflowExecutorMeta.setResultFilesFileNameField(wResultFileNameField.getText());

    // Result row info
    //
    workflowExecutorMeta.setResultRowsTargetTransform(wResultRowsTarget.getText());
    workflowExecutorMeta.setResultRowsTargetTransformMeta(
        pipelineMeta.findTransform(wResultRowsTarget.getText()));
    int nrFields = wResultRowsFields.nrNonEmpty();
    workflowExecutorMeta.setResultRowsField(new String[nrFields]);
    workflowExecutorMeta.setResultRowsType(new int[nrFields]);
    workflowExecutorMeta.setResultRowsLength(new int[nrFields]);
    workflowExecutorMeta.setResultRowsPrecision(new int[nrFields]);

    // CHECKSTYLE:Indentation:OFF
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wResultRowsFields.getNonEmpty(i);
      workflowExecutorMeta.getResultRowsField()[i] = item.getText(1);
      workflowExecutorMeta.getResultRowsType()[i] =
          ValueMetaFactory.getIdForValueMeta(item.getText(2));
      workflowExecutorMeta.getResultRowsLength()[i] = Const.toInt(item.getText(3), -1);
      workflowExecutorMeta.getResultRowsPrecision()[i] = Const.toInt(item.getText(4), -1);
    }
  }
}
