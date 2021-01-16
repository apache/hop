/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow.actions.repeat;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RepeatDialog extends ActionDialog implements IActionDialog {

  private static Class<?> PKG = RepeatDialog.class; // For Translator

  private static final String COLON_SEPARATOR = " : ";

  private Shell shell;

  private Repeat action;

  private Text wName;
  private TextVar wFilename;
  private Button wbbFilename; // Browse for a file
  private TextVar wVariableName;
  private TextVar wVariableValue;
  private TextVar wDelay;
  private Button wKeepValues;
  private TableView wParameters;

  private Group wLogFileGroup;
  private Button wLogFileEnabled;
  private Label wlLogFileBase;
  private TextVar wLogFileBase;
  private Label wlLogFileExtension;
  private TextVar wLogFileExtension;
  private Label wlLogFileDateAdded;
  private Button wLogFileDateAdded;
  private Label wlLogFileTimeAdded;
  private Button wLogFileTimeAdded;
  private Label wlLogFileRepetitionAdded;
  private Button wLogFileRepetitionAdded;
  private Label wlLogFileAppended;
  private Button wLogFileAppended;
  private Label wlLogFileUpdateInterval;
  private TextVar wLogFileUpdateInterval;
  private ComboVar wRunConfiguration;

  private Button wOK, wCancel;

  public RepeatDialog(Shell parent, IAction action, WorkflowMeta workflowMeta) {
    super(parent, workflowMeta);
    this.action = (Repeat) action;

    if (this.action.getName() == null) {
      this.action.setName("Repeat");
    }
  }

  @Override
  public IAction open() {

    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    props.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText("Repeat");

    int middle = props.getMiddlePct();
    int margin = (int)(Const.MARGIN*props.getZoomFactor());

    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText("Action name");
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(0, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    Label wlFilename = new Label(shell, SWT.RIGHT);
    wlFilename.setText("File to repeat (.hpl or .hwf) ");
    props.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.right = new FormAttachment(middle, -margin);
    fdlFilename.top = new FormAttachment(lastControl, margin);
    wlFilename.setLayoutData(fdlFilename);

    // The filename browse button
    //
    wbbFilename = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbFilename );
    wbbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    wbbFilename.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForFileOrDirAndAdd" ) );
    FormData fdbFilename = new FormData();
    fdbFilename.top = new FormAttachment( wlFilename, 0, SWT.CENTER );
    fdbFilename.right = new FormAttachment( 100, 0 );
    wbbFilename.setLayoutData( fdbFilename );
    wbbFilename.addListener( SWT.Selection, e->browseForFile() );

    wFilename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilename);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.right = new FormAttachment(wbbFilename, -margin);
    fdFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    wFilename.setLayoutData(fdFilename);
    lastControl = wFilename;

    Label wlRunConfiguration = new Label(shell, SWT.RIGHT);
    wlRunConfiguration.setText("Run configuration"); // TODO i18n
    props.setLook(wlRunConfiguration);
    FormData fdlRunConfiguration = new FormData();
    fdlRunConfiguration.left = new FormAttachment(0, 0);
    fdlRunConfiguration.top = new FormAttachment(lastControl, Const.isOSX() ? 0 : 5);
    fdlRunConfiguration.right = new FormAttachment(middle, -margin);
    wlRunConfiguration.setLayoutData(fdlRunConfiguration);

    wRunConfiguration = new ComboVar(variables, shell, SWT.LEFT | SWT.BORDER);
    props.setLook(wRunConfiguration);
    FormData fdRunConfiguration = new FormData();
    fdRunConfiguration.left = new FormAttachment(middle, 0);
    fdRunConfiguration.top = new FormAttachment(wlRunConfiguration, 0, SWT.CENTER);
    fdRunConfiguration.right = new FormAttachment(100, 0);
    wRunConfiguration.setLayoutData(fdRunConfiguration);
    lastControl = wRunConfiguration;

    Label wlVariableName = new Label(shell, SWT.RIGHT);
    wlVariableName.setText("Stop repeating when this variable is set");
    props.setLook(wlVariableName);
    FormData fdlVariableName = new FormData();
    fdlVariableName.left = new FormAttachment(0, 0);
    fdlVariableName.right = new FormAttachment(middle, 0);
    fdlVariableName.top = new FormAttachment(lastControl, margin);
    wlVariableName.setLayoutData(fdlVariableName);
    wVariableName = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wVariableName);
    FormData fdVariableName = new FormData();
    fdVariableName.left = new FormAttachment(middle, 0);
    fdVariableName.right = new FormAttachment(100, 0);
    fdVariableName.top = new FormAttachment(wlVariableName, 0, SWT.CENTER);
    wVariableName.setLayoutData(fdVariableName);
    lastControl = wVariableName;

    Label wlVariableValue = new Label(shell, SWT.RIGHT);
    wlVariableValue.setText("Optional variable value ");
    props.setLook(wlVariableValue);
    FormData fdlVariableValue = new FormData();
    fdlVariableValue.left = new FormAttachment(0, 0);
    fdlVariableValue.right = new FormAttachment(middle, -margin);
    fdlVariableValue.top = new FormAttachment(lastControl, margin);
    wlVariableValue.setLayoutData(fdlVariableValue);
    wVariableValue = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wVariableValue);
    FormData fdVariableValue = new FormData();
    fdVariableValue.left = new FormAttachment(middle, 0);
    fdVariableValue.right = new FormAttachment(100, 0);
    fdVariableValue.top = new FormAttachment(wlVariableValue, 0, SWT.CENTER);
    wVariableValue.setLayoutData(fdVariableValue);
    lastControl = wVariableValue;

    Label wlDelay = new Label(shell, SWT.RIGHT);
    wlDelay.setText("Delay in seconds ");
    props.setLook(wlDelay);
    FormData fdlDelay = new FormData();
    fdlDelay.left = new FormAttachment(0, 0);
    fdlDelay.right = new FormAttachment(middle, -margin);
    fdlDelay.top = new FormAttachment(lastControl, margin);
    wlDelay.setLayoutData(fdlDelay);
    wDelay = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wDelay);
    FormData fdDelay = new FormData();
    fdDelay.left = new FormAttachment(middle, 0);
    fdDelay.right = new FormAttachment(100, 0);
    fdDelay.top = new FormAttachment(wlDelay, 0, SWT.CENTER);
    wDelay.setLayoutData(fdDelay);
    lastControl = wDelay;

    Label wlKeepValues = new Label(shell, SWT.RIGHT);
    wlKeepValues.setText("Keep variable values after executions ");
    props.setLook(wlKeepValues);
    FormData fdlKeepValues = new FormData();
    fdlKeepValues.left = new FormAttachment(0, 0);
    fdlKeepValues.right = new FormAttachment(middle, -margin);
    fdlKeepValues.top = new FormAttachment(lastControl, margin);
    wlKeepValues.setLayoutData(fdlKeepValues);
    wKeepValues = new Button(shell, SWT.CHECK | SWT.LEFT);
    props.setLook(wKeepValues);
    FormData fdKeepValues = new FormData();
    fdKeepValues.left = new FormAttachment(middle, 0);
    fdKeepValues.right = new FormAttachment(100, 0);
    fdKeepValues.top = new FormAttachment(wlKeepValues, 0, SWT.CENTER);
    wKeepValues.setLayoutData(fdKeepValues);
    lastControl = wlKeepValues;

    wLogFileGroup = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wLogFileGroup);
    wLogFileGroup.setText("Logging file");
    FormLayout logFileGroupLayout = new FormLayout();
    logFileGroupLayout.marginLeft = Const.MARGIN;
    logFileGroupLayout.marginRight = Const.MARGIN;
    logFileGroupLayout.marginTop = 2 * Const.MARGIN;
    logFileGroupLayout.marginBottom = 2 * Const.MARGIN;
    wLogFileGroup.setLayout(logFileGroupLayout);

    Label wlLogFileEnabled = new Label(wLogFileGroup, SWT.RIGHT);
    wlLogFileEnabled.setText("Log the execution to a file? ");
    props.setLook(wlLogFileEnabled);
    FormData fdlLogFileEnabled = new FormData();
    fdlLogFileEnabled.left = new FormAttachment(0, 0);
    fdlLogFileEnabled.right = new FormAttachment(middle, -margin);
    fdlLogFileEnabled.top = new FormAttachment(0, 0);
    wlLogFileEnabled.setLayoutData(fdlLogFileEnabled);
    wLogFileEnabled = new Button(wLogFileGroup, SWT.CHECK | SWT.LEFT);
    props.setLook(wLogFileEnabled);
    FormData fdLogFileEnabled = new FormData();
    fdLogFileEnabled.left = new FormAttachment(middle, 0);
    fdLogFileEnabled.right = new FormAttachment(100, 0);
    fdLogFileEnabled.top = new FormAttachment(wlLogFileEnabled, 0, SWT.CENTER);
    wLogFileEnabled.setLayoutData(fdLogFileEnabled);
    wLogFileEnabled.addListener(SWT.Selection, e -> enableControls());
    Control lastLogControl = wlLogFileEnabled;

    wlLogFileBase = new Label(wLogFileGroup, SWT.RIGHT);
    wlLogFileBase.setText("The base log file name ");
    props.setLook(wlLogFileBase);
    FormData fdlLogFileBase = new FormData();
    fdlLogFileBase.left = new FormAttachment(0, 0);
    fdlLogFileBase.right = new FormAttachment(middle, -margin);
    fdlLogFileBase.top = new FormAttachment(lastLogControl, margin);
    wlLogFileBase.setLayoutData(fdlLogFileBase);
    wLogFileBase =
        new TextVar(variables, wLogFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wLogFileBase);
    FormData fdLogFileBase = new FormData();
    fdLogFileBase.left = new FormAttachment(middle, 0);
    fdLogFileBase.right = new FormAttachment(100, 0);
    fdLogFileBase.top = new FormAttachment(wlLogFileBase, 0, SWT.CENTER);
    wLogFileBase.setLayoutData(fdLogFileBase);
    lastLogControl = wLogFileBase;

    wlLogFileExtension = new Label(wLogFileGroup, SWT.RIGHT);
    wlLogFileExtension.setText("The log file extension ");
    props.setLook(wlLogFileExtension);
    FormData fdlLogFileExtension = new FormData();
    fdlLogFileExtension.left = new FormAttachment(0, 0);
    fdlLogFileExtension.right = new FormAttachment(middle, -margin);
    fdlLogFileExtension.top = new FormAttachment(lastLogControl, margin);
    wlLogFileExtension.setLayoutData(fdlLogFileExtension);
    wLogFileExtension =
        new TextVar(variables, wLogFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wLogFileExtension);
    FormData fdLogFileExtension = new FormData();
    fdLogFileExtension.left = new FormAttachment(middle, 0);
    fdLogFileExtension.right = new FormAttachment(100, 0);
    fdLogFileExtension.top = new FormAttachment(wlLogFileExtension, 0, SWT.CENTER);
    wLogFileExtension.setLayoutData(fdLogFileExtension);
    lastLogControl = wLogFileExtension;

    wlLogFileDateAdded = new Label(wLogFileGroup, SWT.RIGHT);
    wlLogFileDateAdded.setText("Add the date to the filename? ");
    props.setLook(wlLogFileDateAdded);
    FormData fdlLogFileDateAdded = new FormData();
    fdlLogFileDateAdded.left = new FormAttachment(0, 0);
    fdlLogFileDateAdded.right = new FormAttachment(middle, -margin);
    fdlLogFileDateAdded.top = new FormAttachment(lastLogControl, margin);
    wlLogFileDateAdded.setLayoutData(fdlLogFileDateAdded);
    wLogFileDateAdded = new Button(wLogFileGroup, SWT.CHECK | SWT.LEFT);
    props.setLook(wLogFileDateAdded);
    FormData fdLogFileDateAdded = new FormData();
    fdLogFileDateAdded.left = new FormAttachment(middle, 0);
    fdLogFileDateAdded.right = new FormAttachment(100, 0);
    fdLogFileDateAdded.top = new FormAttachment(wlLogFileDateAdded, 0, SWT.CENTER);
    wLogFileDateAdded.setLayoutData(fdLogFileDateAdded);
    lastLogControl = wlLogFileDateAdded;

    wlLogFileTimeAdded = new Label(wLogFileGroup, SWT.RIGHT);
    wlLogFileTimeAdded.setText("Add the time to the filename? ");
    props.setLook(wlLogFileTimeAdded);
    FormData fdlLogFileTimeAdded = new FormData();
    fdlLogFileTimeAdded.left = new FormAttachment(0, 0);
    fdlLogFileTimeAdded.right = new FormAttachment(middle, -margin);
    fdlLogFileTimeAdded.top = new FormAttachment(lastLogControl, margin);
    wlLogFileTimeAdded.setLayoutData(fdlLogFileTimeAdded);
    wLogFileTimeAdded = new Button(wLogFileGroup, SWT.CHECK | SWT.LEFT);
    props.setLook(wLogFileTimeAdded);
    FormData fdLogFileTimeAdded = new FormData();
    fdLogFileTimeAdded.left = new FormAttachment(middle, 0);
    fdLogFileTimeAdded.right = new FormAttachment(100, 0);
    fdLogFileTimeAdded.top = new FormAttachment(wlLogFileTimeAdded, 0, SWT.CENTER);
    wLogFileTimeAdded.setLayoutData(fdLogFileTimeAdded);
    lastLogControl = wlLogFileTimeAdded;

    wlLogFileRepetitionAdded = new Label(wLogFileGroup, SWT.RIGHT);
    wlLogFileRepetitionAdded.setText("Add the repetition number to the filename? ");
    props.setLook(wlLogFileRepetitionAdded);
    FormData fdlLogFileRepetitionAdded = new FormData();
    fdlLogFileRepetitionAdded.left = new FormAttachment(0, 0);
    fdlLogFileRepetitionAdded.right = new FormAttachment(middle, -margin);
    fdlLogFileRepetitionAdded.top = new FormAttachment(lastLogControl, margin);
    wlLogFileRepetitionAdded.setLayoutData(fdlLogFileRepetitionAdded);
    wLogFileRepetitionAdded = new Button(wLogFileGroup, SWT.CHECK | SWT.LEFT);
    props.setLook(wLogFileRepetitionAdded);
    FormData fdLogFileRepetitionAdded = new FormData();
    fdLogFileRepetitionAdded.left = new FormAttachment(middle, 0);
    fdLogFileRepetitionAdded.right = new FormAttachment(100, 0);
    fdLogFileRepetitionAdded.top = new FormAttachment(wlLogFileRepetitionAdded, 0, SWT.CENTER);
    wLogFileRepetitionAdded.setLayoutData(fdLogFileRepetitionAdded);
    lastLogControl = wlLogFileRepetitionAdded;

    wlLogFileAppended = new Label(wLogFileGroup, SWT.RIGHT);
    wlLogFileAppended.setText("Append to any existing log file? ");
    props.setLook(wlLogFileAppended);
    FormData fdlLogFileAppended = new FormData();
    fdlLogFileAppended.left = new FormAttachment(0, 0);
    fdlLogFileAppended.right = new FormAttachment(middle, -margin);
    fdlLogFileAppended.top = new FormAttachment(lastLogControl, margin);
    wlLogFileAppended.setLayoutData(fdlLogFileAppended);
    wLogFileAppended = new Button(wLogFileGroup, SWT.CHECK | SWT.LEFT);
    props.setLook(wLogFileAppended);
    FormData fdLogFileAppended = new FormData();
    fdLogFileAppended.left = new FormAttachment(middle, 0);
    fdLogFileAppended.right = new FormAttachment(100, 0);
    fdLogFileAppended.top = new FormAttachment(wlLogFileAppended, 0, SWT.CENTER);
    wLogFileAppended.setLayoutData(fdLogFileAppended);
    lastLogControl = wlLogFileAppended;

    wlLogFileUpdateInterval = new Label(wLogFileGroup, SWT.RIGHT);
    wlLogFileUpdateInterval.setText("The log file update interval in ms ");
    props.setLook(wlLogFileUpdateInterval);
    FormData fdlLogFileUpdateInterval = new FormData();
    fdlLogFileUpdateInterval.left = new FormAttachment(0, 0);
    fdlLogFileUpdateInterval.right = new FormAttachment(middle, -margin);
    fdlLogFileUpdateInterval.top = new FormAttachment(lastLogControl, margin);
    wlLogFileUpdateInterval.setLayoutData(fdlLogFileUpdateInterval);
    wLogFileUpdateInterval =
        new TextVar(variables, wLogFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wLogFileUpdateInterval);
    FormData fdLogFileUpdateInterval = new FormData();
    fdLogFileUpdateInterval.left = new FormAttachment(middle, 0);
    fdLogFileUpdateInterval.right = new FormAttachment(100, 0);
    fdLogFileUpdateInterval.top = new FormAttachment(wlLogFileUpdateInterval, 0, SWT.CENTER);
    wLogFileUpdateInterval.setLayoutData(fdLogFileUpdateInterval);
    lastLogControl = wLogFileUpdateInterval;

    FormData fdLogFileGroup = new FormData();
    fdLogFileGroup.left = new FormAttachment(0, 0);
    fdLogFileGroup.right = new FormAttachment(100, 0);
    fdLogFileGroup.top = new FormAttachment(lastControl, 2*margin);
    wLogFileGroup.setLayoutData(fdLogFileGroup);
    wLogFileGroup.pack();
    lastControl = wLogFileGroup;

    // Parameters
    //
    Label wlParameters = new Label(shell, SWT.LEFT);
    wlParameters.setText("Parameters/Variables to set: ");
    props.setLook(wlParameters);
    FormData fdlParameters = new FormData();
    fdlParameters.left = new FormAttachment(0, 0);
    fdlParameters.top = new FormAttachment(lastControl, 2 * margin);
    fdlParameters.right = new FormAttachment(100, 0);
    wlParameters.setLayoutData(fdlParameters);
    lastControl = wlParameters;

    // Add buttons first, then the script field can use dynamic sizing
    //
    wOK = new Button(shell, SWT.PUSH);
    wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOK.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    // Put these buttons at the bottom
    //
    BaseTransformDialog.positionBottomButtons(
        shell,
        new Button[] {
          wOK, wCancel,
        },
        margin,
        null);

    ColumnInfo[] columnInfos =
        new ColumnInfo[] {
          new ColumnInfo("Name", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
          new ColumnInfo("Value", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
        };
    columnInfos[1].setUsingVariables(true);

    wParameters =
        new TableView(
            variables,
            shell,
            SWT.BORDER,
            columnInfos,
            action.getParameters().size(),
            null,
            props);
    props.setLook(wParameters);
    FormData fdParameters = new FormData();
    fdParameters.left = new FormAttachment(0, 0);
    fdParameters.right = new FormAttachment(100, 0);
    fdParameters.top = new FormAttachment(lastControl, margin);
    fdParameters.bottom = new FormAttachment(wOK, -margin * 2);
    wParameters.setLayoutData(fdParameters);
    lastControl = wParameters;

    // Detect X or ALT-F4 or something that kills this window...
    //
    shell.addListener(SWT.Close, e -> cancel());
    wName.addListener(SWT.DefaultSelection, e -> ok());
    wFilename.addListener(SWT.DefaultSelection, e -> ok());
    wVariableName.addListener(SWT.DefaultSelection, e -> ok());
    wVariableValue.addListener(SWT.DefaultSelection, e -> ok());
    wDelay.addListener(SWT.DefaultSelection, e -> ok());

    getData();

    BaseTransformDialog.setSize(shell);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    return action;
  }

  private void browseForFile() {
    HopPipelineFileType pipelineFileType = new HopPipelineFileType<>();
    HopWorkflowFileType workflowFileType = new HopWorkflowFileType<>();

    List<String> filterExtensions = new ArrayList<>();
    filterExtensions.add(
      pipelineFileType.getFilterExtensions()[0]+
        ";"+
        workflowFileType.getFilterExtensions()[0] );
    filterExtensions.addAll( Arrays.asList( pipelineFileType.getFilterExtensions()));
    filterExtensions.addAll( Arrays.asList( workflowFileType.getFilterExtensions()));
    filterExtensions.add("*.*");

    List<String> filterNames = new ArrayList<>();
    filterNames.add(
      pipelineFileType.getFilterNames()[0]+
        " and "+
        workflowFileType.getFilterNames()[0] );
    filterNames.addAll( Arrays.asList( pipelineFileType.getFilterNames()));
    filterNames.addAll( Arrays.asList( workflowFileType.getFilterNames()));
    filterNames.add(BaseMessages.getString( PKG, "System.FileType.AllFiles" ));

    BaseDialog.presentFileDialog( shell, wFilename, variables,
      filterExtensions.toArray(new String[0]),
      filterNames.toArray(new String[0]),
      true
    );
  }

  private void enableControls() {
    boolean logEnabled = wLogFileEnabled.getSelection();

    wlLogFileBase.setEnabled(logEnabled);
    wLogFileBase.setEnabled(logEnabled);
    wlLogFileExtension.setEnabled(logEnabled);
    wLogFileExtension.setEnabled(logEnabled);
    wlLogFileDateAdded.setEnabled(logEnabled);
    wLogFileDateAdded.setEnabled(logEnabled);
    wlLogFileTimeAdded.setEnabled(logEnabled);
    wLogFileTimeAdded.setEnabled(logEnabled);
    wlLogFileRepetitionAdded.setEnabled(logEnabled);
    wLogFileRepetitionAdded.setEnabled(logEnabled);
    wlLogFileAppended.setEnabled(logEnabled);
    wLogFileAppended.setEnabled(logEnabled);
    wlLogFileUpdateInterval.setEnabled(logEnabled);
    wLogFileUpdateInterval.setEnabled(logEnabled);
  }

  private void cancel() {
    action = null;
    dispose();
  }

  private void getData() {
    wName.setText(Const.NVL(action.getName(), ""));
    wFilename.setText(Const.NVL(action.getFilename(), ""));
    wVariableName.setText(Const.NVL(action.getVariableName(), ""));
    wVariableValue.setText(Const.NVL(action.getVariableValue(), ""));
    wDelay.setText(Const.NVL(action.getDelay(), ""));
    wKeepValues.setSelection(action.isKeepingValues());

    wLogFileEnabled.setSelection(action.isLogFileEnabled());
    wLogFileBase.setText(Const.NVL(action.getLogFileBase(), ""));
    wLogFileExtension.setText(Const.NVL(action.getLogFileExtension(), ""));
    wLogFileDateAdded.setSelection(action.isLogFileDateAdded());
    wLogFileTimeAdded.setSelection(action.isLogFileTimeAdded());
    wLogFileRepetitionAdded.setSelection(action.isLogFileRepetitionAdded());
    wLogFileAppended.setSelection(action.isLogFileAppended());
    wLogFileUpdateInterval.setText(Const.NVL(action.getLogFileUpdateInterval(), "5000"));

    int rowNr = 0;
    for (ParameterDetails parameter : action.getParameters()) {
      TableItem item = wParameters.table.getItem(rowNr++);
      item.setText(1, Const.NVL(parameter.getName(), ""));
      item.setText(2, Const.NVL(parameter.getField(), ""));
    }
    wParameters.setRowNums();
    wParameters.optWidth(true);

    // Get the run configurations for both pipelines and workflows
    //
    MetadataManager<PipelineRunConfiguration> prcManager =
        new MetadataManager<>(
            variables, getMetadataProvider(), PipelineRunConfiguration.class);
    MetadataManager<WorkflowRunConfiguration> wrcManager =
        new MetadataManager<>(
            variables, getMetadataProvider(), WorkflowRunConfiguration.class);
    List<String> entries = new ArrayList<>();
    try {
      prcManager.getNames().forEach(name -> entries.add("Pipeline" + COLON_SEPARATOR + name));
    } catch (Exception e) {
      // Ignore
    }
    try {
      wrcManager.getNames().forEach(name -> entries.add("Workflow" + COLON_SEPARATOR + name));
    } catch (Exception e) {
      // Ignore
    }
    wRunConfiguration.setItems(entries.toArray(new String[0]));
    if (StringUtils.isNotEmpty(action.getRunConfigurationName())) {
      String realFilename = variables.resolve(wFilename.getText());
      try {
        if (this.action.isPipeline(realFilename)) {
          wRunConfiguration.setText(
              "Pipeline" + COLON_SEPARATOR + action.getRunConfigurationName());
        } else if (this.action.isWorkflow(realFilename)) {
          wRunConfiguration.setText(
              "Workflow" + COLON_SEPARATOR + action.getRunConfigurationName());
        }
      } catch (Exception e) {
        // Ignore
      }
    }

    wName.selectAll();
    wName.setFocus();

    enableControls();
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText("Warning");
      mb.setMessage("The name of the action is missing!");
      mb.open();
      return;
    }
    action.setName(wName.getText());
    action.setFilename(wFilename.getText());
    action.setVariableName(wVariableName.getText());
    action.setVariableValue(wVariableValue.getText());
    action.setDelay(wDelay.getText());
    action.setKeepingValues(wKeepValues.getSelection());

    action.setLogFileEnabled(wLogFileEnabled.getSelection());
    action.setLogFileAppended(wLogFileAppended.getSelection());
    action.setLogFileBase(wLogFileBase.getText());
    action.setLogFileExtension(wLogFileExtension.getText());
    action.setLogFileDateAdded(wLogFileDateAdded.getSelection());
    action.setLogFileTimeAdded(wLogFileTimeAdded.getSelection());
    action.setLogFileRepetitionAdded(wLogFileRepetitionAdded.getSelection());
    action.setLogFileUpdateInterval(wLogFileUpdateInterval.getText());

    action.getParameters().clear();
    for (int i = 0; i < wParameters.nrNonEmpty(); i++) {
      TableItem item = wParameters.getNonEmpty(i);
      action.getParameters().add(new ParameterDetails(item.getText(1), item.getText(2)));
    }

    // Get the name of the run configuration:
    //
    String runConfigRaw = wRunConfiguration.getText();
    if (StringUtils.isEmpty(runConfigRaw)) {
      action.setRunConfigurationName(null);
    } else {
      int colonIndex = runConfigRaw.indexOf(COLON_SEPARATOR);
      if (colonIndex > 0 && colonIndex + COLON_SEPARATOR.length() < runConfigRaw.length()) {
        action.setRunConfigurationName(
            runConfigRaw.substring(colonIndex + COLON_SEPARATOR.length()));
      } else {
        action.setRunConfigurationName(runConfigRaw);
      }
    }

    action.setChanged();

    dispose();
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
