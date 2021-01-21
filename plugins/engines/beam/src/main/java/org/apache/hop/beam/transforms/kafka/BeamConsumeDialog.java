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

package org.apache.hop.beam.transforms.kafka;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class BeamConsumeDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = BeamConsume.class; // For Translator
  private final BeamConsumeMeta input;

  int middle;
  int margin;

  private TextVar wBootstrapServers;
  private TextVar wTopics;
  private TextVar wGroupId;
  private TextVar wKeyField;
  private TextVar wMessageField;
  private Button wUseProcessingTime;
  private Button wUseLogAppendTime;
  private Button wUseCreateTime;
  private Button wRestrictToCommitted;
  private Button wAllowCommitConsumed;
  private TableView wConfigOptions;

  public BeamConsumeDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (BeamConsumeMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "BeamConsumeDialog.DialogTitle"));

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    Label wlBootstrapServers = new Label(shell, SWT.RIGHT);
    wlBootstrapServers.setText(BaseMessages.getString(PKG, "BeamConsumeDialog.BootstrapServers"));
    props.setLook(wlBootstrapServers);
    FormData fdlBootstrapServers = new FormData();
    fdlBootstrapServers.left = new FormAttachment(0, 0);
    fdlBootstrapServers.top = new FormAttachment(lastControl, margin);
    fdlBootstrapServers.right = new FormAttachment(middle, -margin);
    wlBootstrapServers.setLayoutData(fdlBootstrapServers);
    wBootstrapServers = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBootstrapServers);
    FormData fdBootstrapServers = new FormData();
    fdBootstrapServers.left = new FormAttachment(middle, 0);
    fdBootstrapServers.top = new FormAttachment(wlBootstrapServers, 0, SWT.CENTER);
    fdBootstrapServers.right = new FormAttachment(100, 0);
    wBootstrapServers.setLayoutData(fdBootstrapServers);
    lastControl = wBootstrapServers;

    Label wlTopics = new Label(shell, SWT.RIGHT);
    wlTopics.setText(BaseMessages.getString(PKG, "BeamConsumeDialog.Topics"));
    props.setLook(wlTopics);
    FormData fdlTopics = new FormData();
    fdlTopics.left = new FormAttachment(0, 0);
    fdlTopics.top = new FormAttachment(lastControl, margin);
    fdlTopics.right = new FormAttachment(middle, -margin);
    wlTopics.setLayoutData(fdlTopics);
    wTopics = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTopics);
    FormData fdTopics = new FormData();
    fdTopics.left = new FormAttachment(middle, 0);
    fdTopics.top = new FormAttachment(wlTopics, 0, SWT.CENTER);
    fdTopics.right = new FormAttachment(100, 0);
    wTopics.setLayoutData(fdTopics);
    lastControl = wTopics;

    Label wlGroupId = new Label(shell, SWT.RIGHT);
    wlGroupId.setText(BaseMessages.getString(PKG, "BeamConsumeDialog.GroupId"));
    props.setLook(wlGroupId);
    FormData fdlGroupId = new FormData();
    fdlGroupId.left = new FormAttachment(0, 0);
    fdlGroupId.top = new FormAttachment(lastControl, margin);
    fdlGroupId.right = new FormAttachment(middle, -margin);
    wlGroupId.setLayoutData(fdlGroupId);
    wGroupId = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wGroupId);
    FormData fdGroupId = new FormData();
    fdGroupId.left = new FormAttachment(middle, 0);
    fdGroupId.top = new FormAttachment(wlGroupId, 0, SWT.CENTER);
    fdGroupId.right = new FormAttachment(100, 0);
    wGroupId.setLayoutData(fdGroupId);
    lastControl = wGroupId;

    Label wlKeyField = new Label(shell, SWT.RIGHT);
    wlKeyField.setText(BaseMessages.getString(PKG, "BeamConsumeDialog.KeyField"));
    props.setLook(wlKeyField);
    FormData fdlKeyField = new FormData();
    fdlKeyField.left = new FormAttachment(0, 0);
    fdlKeyField.top = new FormAttachment(lastControl, margin);
    fdlKeyField.right = new FormAttachment(middle, -margin);
    wlKeyField.setLayoutData(fdlKeyField);
    wKeyField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wKeyField);
    FormData fdKeyField = new FormData();
    fdKeyField.left = new FormAttachment(middle, 0);
    fdKeyField.top = new FormAttachment(wlKeyField, 0, SWT.CENTER);
    fdKeyField.right = new FormAttachment(100, 0);
    wKeyField.setLayoutData(fdKeyField);
    lastControl = wKeyField;

    Label wlMessageField = new Label(shell, SWT.RIGHT);
    wlMessageField.setText(BaseMessages.getString(PKG, "BeamConsumeDialog.MessageField"));
    props.setLook(wlMessageField);
    FormData fdlMessageField = new FormData();
    fdlMessageField.left = new FormAttachment(0, 0);
    fdlMessageField.top = new FormAttachment(lastControl, margin);
    fdlMessageField.right = new FormAttachment(middle, -margin);
    wlMessageField.setLayoutData(fdlMessageField);
    wMessageField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wMessageField);
    FormData fdMessageField = new FormData();
    fdMessageField.left = new FormAttachment(middle, 0);
    fdMessageField.top = new FormAttachment(wlMessageField, 0, SWT.CENTER);
    fdMessageField.right = new FormAttachment(100, 0);
    wMessageField.setLayoutData(fdMessageField);
    lastControl = wMessageField;

    Label wlUseProcessingTime = new Label(shell, SWT.RIGHT);
    wlUseProcessingTime.setText(BaseMessages.getString(PKG, "BeamProduceDialog.UseProcessingTime"));
    props.setLook(wlUseProcessingTime);
    FormData fdlUseProcessingTime = new FormData();
    fdlUseProcessingTime.left = new FormAttachment(0, 0);
    fdlUseProcessingTime.top = new FormAttachment(lastControl, margin);
    fdlUseProcessingTime.right = new FormAttachment(middle, -margin);
    wlUseProcessingTime.setLayoutData(fdlUseProcessingTime);
    wUseProcessingTime = new Button(shell, SWT.CHECK | SWT.LEFT);
    props.setLook(wUseProcessingTime);
    FormData fdUseProcessingTime = new FormData();
    fdUseProcessingTime.left = new FormAttachment(middle, 0);
    fdUseProcessingTime.top = new FormAttachment(wlUseProcessingTime, 0, SWT.CENTER);
    fdUseProcessingTime.right = new FormAttachment(100, 0);
    wUseProcessingTime.setLayoutData(fdUseProcessingTime);
    lastControl = wUseProcessingTime;

    Label wlUseLogAppendTime = new Label(shell, SWT.RIGHT);
    wlUseLogAppendTime.setText(BaseMessages.getString(PKG, "BeamProduceDialog.UseLogAppendTime"));
    props.setLook(wlUseLogAppendTime);
    FormData fdlUseLogAppendTime = new FormData();
    fdlUseLogAppendTime.left = new FormAttachment(0, 0);
    fdlUseLogAppendTime.top = new FormAttachment(lastControl, margin);
    fdlUseLogAppendTime.right = new FormAttachment(middle, -margin);
    wlUseLogAppendTime.setLayoutData(fdlUseLogAppendTime);
    wUseLogAppendTime = new Button(shell, SWT.CHECK | SWT.LEFT);
    props.setLook(wUseLogAppendTime);
    FormData fdUseLogAppendTime = new FormData();
    fdUseLogAppendTime.left = new FormAttachment(middle, 0);
    fdUseLogAppendTime.top = new FormAttachment(wlUseLogAppendTime, 0, SWT.CENTER);
    fdUseLogAppendTime.right = new FormAttachment(100, 0);
    wUseLogAppendTime.setLayoutData(fdUseLogAppendTime);
    lastControl = wUseLogAppendTime;

    // private Button wUseCreateTime;
    Label wlUseCreateTime = new Label(shell, SWT.RIGHT);
    wlUseCreateTime.setText(BaseMessages.getString(PKG, "BeamProduceDialog.UseCreateTime"));
    props.setLook(wlUseCreateTime);
    FormData fdlUseCreateTime = new FormData();
    fdlUseCreateTime.left = new FormAttachment(0, 0);
    fdlUseCreateTime.top = new FormAttachment(lastControl, margin);
    fdlUseCreateTime.right = new FormAttachment(middle, -margin);
    wlUseCreateTime.setLayoutData(fdlUseCreateTime);
    wUseCreateTime = new Button(shell, SWT.CHECK | SWT.LEFT);
    props.setLook(wUseCreateTime);
    FormData fdUseCreateTime = new FormData();
    fdUseCreateTime.left = new FormAttachment(middle, 0);
    fdUseCreateTime.top = new FormAttachment(wlUseCreateTime, 0, SWT.CENTER);
    fdUseCreateTime.right = new FormAttachment(100, 0);
    wUseCreateTime.setLayoutData(fdUseCreateTime);
    lastControl = wUseCreateTime;

    Label wlRestrictToCommitted = new Label(shell, SWT.RIGHT);
    wlRestrictToCommitted.setText(
        BaseMessages.getString(PKG, "BeamProduceDialog.RestrictToCommitted"));
    props.setLook(wlRestrictToCommitted);
    FormData fdlRestrictToCommitted = new FormData();
    fdlRestrictToCommitted.left = new FormAttachment(0, 0);
    fdlRestrictToCommitted.top = new FormAttachment(lastControl, margin);
    fdlRestrictToCommitted.right = new FormAttachment(middle, -margin);
    wlRestrictToCommitted.setLayoutData(fdlRestrictToCommitted);
    wRestrictToCommitted = new Button(shell, SWT.CHECK | SWT.LEFT);
    props.setLook(wRestrictToCommitted);
    FormData fdRestrictToCommitted = new FormData();
    fdRestrictToCommitted.left = new FormAttachment(middle, 0);
    fdRestrictToCommitted.top = new FormAttachment(wlRestrictToCommitted, 0, SWT.CENTER);
    fdRestrictToCommitted.right = new FormAttachment(100, 0);
    wRestrictToCommitted.setLayoutData(fdRestrictToCommitted);
    lastControl = wRestrictToCommitted;

    Label wlAllowCommitConsumed = new Label(shell, SWT.RIGHT);
    wlAllowCommitConsumed.setText(
        BaseMessages.getString(PKG, "BeamProduceDialog.AllowCommitConsumed"));
    props.setLook(wlAllowCommitConsumed);
    FormData fdlAllowCommitConsumed = new FormData();
    fdlAllowCommitConsumed.left = new FormAttachment(0, 0);
    fdlAllowCommitConsumed.top = new FormAttachment(lastControl, margin);
    fdlAllowCommitConsumed.right = new FormAttachment(middle, -margin);
    wlAllowCommitConsumed.setLayoutData(fdlAllowCommitConsumed);
    wAllowCommitConsumed = new Button(shell, SWT.CHECK | SWT.LEFT);
    props.setLook(wAllowCommitConsumed);
    FormData fdAllowCommitConsumed = new FormData();
    fdAllowCommitConsumed.left = new FormAttachment(middle, 0);
    fdAllowCommitConsumed.top = new FormAttachment(wlAllowCommitConsumed, 0, SWT.CENTER);
    fdAllowCommitConsumed.right = new FormAttachment(100, 0);
    wAllowCommitConsumed.setLayoutData(fdAllowCommitConsumed);
    lastControl = wAllowCommitConsumed;

    // private Button wAllowCommitConsumed;
    Label wlConfigOptions = new Label(shell, SWT.LEFT);
    wlConfigOptions.setText(BaseMessages.getString(PKG, "BeamProduceDialog.ConfigOptions"));
    props.setLook(wlConfigOptions);
    FormData fdlConfigOptions = new FormData();
    fdlConfigOptions.left = new FormAttachment(0, 0);
    fdlConfigOptions.top = new FormAttachment(lastControl, margin);
    fdlConfigOptions.right = new FormAttachment(100, 0);
    wlConfigOptions.setLayoutData(fdlConfigOptions);
    lastControl = wlConfigOptions;

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));

    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "BeamProduceDialog.ConfigOptions.Column.Parameter"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "BeamProduceDialog.ConfigOptions.Column.Value"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "BeamProduceDialog.ConfigOptions.Column.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ConfigOption.Type.getTypeNames(),
              true),
        };
    columns[0].setUsingVariables(true);
    columns[1].setUsingVariables(true);

    wConfigOptions =
        new TableView(
            variables, shell, SWT.NONE, columns, input.getConfigOptions().size(), null, props);
    props.setLook(wConfigOptions);
    FormData fdConfigOptions = new FormData();
    fdConfigOptions.left = new FormAttachment(0, 0);
    fdConfigOptions.right = new FormAttachment(100, 0);
    fdConfigOptions.top = new FormAttachment(lastControl, margin);
    fdConfigOptions.bottom = new FormAttachment(wOk, -margin * 2);
    wConfigOptions.setLayoutData(fdConfigOptions);
    // lastControl = wConfigOptions;

    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());

    lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wTransformName.addSelectionListener(lsDef);
    wBootstrapServers.addSelectionListener(lsDef);
    wKeyField.addSelectionListener(lsDef);
    wMessageField.addSelectionListener(lsDef);
    wTopics.addSelectionListener(lsDef);

    wUseProcessingTime.addListener(
        SWT.Selection,
        e -> {
          wUseLogAppendTime.setSelection(false);
          wUseCreateTime.setSelection(false);
        });
    wUseLogAppendTime.addListener(
        SWT.Selection,
        e -> {
          wUseProcessingTime.setSelection(false);
          wUseCreateTime.setSelection(false);
        });
    wUseCreateTime.addListener(
        SWT.Selection,
        e -> {
          wUseProcessingTime.setSelection(false);
          wUseLogAppendTime.setSelection(false);
        });

    // Detect X or ALT-F4 or something that kills this window...
    shell.addListener(SWT.Close, e -> cancel());

    getData();
    setSize();
    input.setChanged(changed);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return transformName;
  }

  /** Populate the widgets. */
  public void getData() {
    wTransformName.setText(transformName);
    wBootstrapServers.setText(Const.NVL(input.getBootstrapServers(), ""));
    wTopics.setText(Const.NVL(input.getTopics(), ""));
    wGroupId.setText(Const.NVL(input.getGroupId(), ""));
    wKeyField.setText(Const.NVL(input.getKeyField(), ""));
    wMessageField.setText(Const.NVL(input.getMessageField(), ""));

    wUseProcessingTime.setSelection(input.isUsingProcessingTime());
    wUseLogAppendTime.setSelection(input.isUsingLogAppendTime());
    wUseCreateTime.setSelection(input.isUsingCreateTime());

    wRestrictToCommitted.setSelection(input.isRestrictedToCommitted());
    wAllowCommitConsumed.setSelection(input.isAllowingCommitOnConsumedOffset());

    for (int i = 0; i < input.getConfigOptions().size(); i++) {
      ConfigOption option = input.getConfigOptions().get(i);
      TableItem item = wConfigOptions.table.getItem(i);
      item.setText(1, Const.NVL(option.getParameter(), ""));
      item.setText(2, Const.NVL(option.getValue(), ""));
      item.setText(3, option.getType() != null ? option.getType().name() : "");
    }
    wConfigOptions.removeEmptyRows();
    wConfigOptions.setRowNums();
    wConfigOptions.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
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

    getInfo(input);

    dispose();
  }

  private void getInfo(BeamConsumeMeta in) {
    transformName = wTransformName.getText(); // return value

    in.setBootstrapServers(wBootstrapServers.getText());
    in.setTopics(wTopics.getText());
    in.setKeyField(wKeyField.getText());
    in.setMessageField(wMessageField.getText());
    in.setGroupId(wGroupId.getText());

    in.setUsingProcessingTime(wUseProcessingTime.getSelection());
    in.setUsingLogAppendTime(wUseLogAppendTime.getSelection());
    in.setUsingCreateTime(wUseCreateTime.getSelection());
    in.setRestrictedToCommitted(wRestrictToCommitted.getSelection());
    in.setAllowingCommitOnConsumedOffset(wAllowCommitConsumed.getSelection());
    int nr = wConfigOptions.nrNonEmpty();
    in.getConfigOptions().clear();
    for (int i = 0; i < nr; i++) {
      TableItem item = wConfigOptions.getNonEmpty(i);
      String parameter = item.getText(1);
      String value = item.getText(2);
      ConfigOption.Type type = ConfigOption.Type.getTypeFromName(item.getText(3));
      in.getConfigOptions().add(new ConfigOption(parameter, value, type));
    }

    input.setChanged();
  }
}
