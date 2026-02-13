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

package org.apache.hop.beam.transforms.kafka;

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class BeamConsumeDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BeamConsume.class;
  private final BeamConsumeMeta input;

  private TextVar wBootstrapServers;
  private TextVar wTopics;
  private TextVar wGroupId;
  private TextVar wKeyField;
  private TextVar wMessageField;
  private ComboVar wMessageType;
  private TextVar wSchemaRegistryUrl;
  private TextVar wSchemaRegistrySubject;
  private Button wUseProcessingTime;
  private Button wUseLogAppendTime;
  private Button wUseCreateTime;
  private Button wRestrictToCommitted;
  private Button wAllowCommitConsumed;
  private TableView wConfigOptions;

  public BeamConsumeDialog(
      Shell parent,
      IVariables variables,
      BeamConsumeMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "BeamConsumeDialog.DialogTitle"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ScrolledComposite scrolledComposite = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(scrolledComposite);
    FormData fdScrolledComposite = new FormData();
    fdScrolledComposite.left = new FormAttachment(0, 0);
    fdScrolledComposite.top = new FormAttachment(wSpacer, 0);
    fdScrolledComposite.right = new FormAttachment(100, 0);
    fdScrolledComposite.bottom = new FormAttachment(wOk, -margin);
    scrolledComposite.setLayoutData(fdScrolledComposite);
    scrolledComposite.setLayout(new FillLayout());

    Composite wContent = new Composite(scrolledComposite, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    changed = input.hasChanged();

    Control lastControl = null;

    Label wlBootstrapServers = new Label(wContent, SWT.RIGHT);
    wlBootstrapServers.setText(BaseMessages.getString(PKG, "BeamConsumeDialog.BootstrapServers"));
    PropsUi.setLook(wlBootstrapServers);
    FormData fdlBootstrapServers = new FormData();
    fdlBootstrapServers.left = new FormAttachment(0, 0);
    fdlBootstrapServers.top = new FormAttachment(0, margin);
    fdlBootstrapServers.right = new FormAttachment(middle, -margin);
    wlBootstrapServers.setLayoutData(fdlBootstrapServers);
    wBootstrapServers = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBootstrapServers);
    FormData fdBootstrapServers = new FormData();
    fdBootstrapServers.left = new FormAttachment(middle, 0);
    fdBootstrapServers.top = new FormAttachment(wlBootstrapServers, 0, SWT.CENTER);
    fdBootstrapServers.right = new FormAttachment(100, 0);
    wBootstrapServers.setLayoutData(fdBootstrapServers);
    lastControl = wBootstrapServers;

    Label wlTopics = new Label(wContent, SWT.RIGHT);
    wlTopics.setText(BaseMessages.getString(PKG, "BeamConsumeDialog.Topics"));
    PropsUi.setLook(wlTopics);
    FormData fdlTopics = new FormData();
    fdlTopics.left = new FormAttachment(0, 0);
    fdlTopics.top = new FormAttachment(lastControl, margin);
    fdlTopics.right = new FormAttachment(middle, -margin);
    wlTopics.setLayoutData(fdlTopics);
    wTopics = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTopics);
    FormData fdTopics = new FormData();
    fdTopics.left = new FormAttachment(middle, 0);
    fdTopics.top = new FormAttachment(wlTopics, 0, SWT.CENTER);
    fdTopics.right = new FormAttachment(100, 0);
    wTopics.setLayoutData(fdTopics);
    lastControl = wTopics;

    Label wlGroupId = new Label(wContent, SWT.RIGHT);
    wlGroupId.setText(BaseMessages.getString(PKG, "BeamConsumeDialog.GroupId"));
    PropsUi.setLook(wlGroupId);
    FormData fdlGroupId = new FormData();
    fdlGroupId.left = new FormAttachment(0, 0);
    fdlGroupId.top = new FormAttachment(lastControl, margin);
    fdlGroupId.right = new FormAttachment(middle, -margin);
    wlGroupId.setLayoutData(fdlGroupId);
    wGroupId = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wGroupId);
    FormData fdGroupId = new FormData();
    fdGroupId.left = new FormAttachment(middle, 0);
    fdGroupId.top = new FormAttachment(wlGroupId, 0, SWT.CENTER);
    fdGroupId.right = new FormAttachment(100, 0);
    wGroupId.setLayoutData(fdGroupId);
    lastControl = wGroupId;

    Label wlKeyField = new Label(wContent, SWT.RIGHT);
    wlKeyField.setText(BaseMessages.getString(PKG, "BeamConsumeDialog.KeyField"));
    PropsUi.setLook(wlKeyField);
    FormData fdlKeyField = new FormData();
    fdlKeyField.left = new FormAttachment(0, 0);
    fdlKeyField.top = new FormAttachment(lastControl, margin);
    fdlKeyField.right = new FormAttachment(middle, -margin);
    wlKeyField.setLayoutData(fdlKeyField);
    wKeyField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wKeyField);
    FormData fdKeyField = new FormData();
    fdKeyField.left = new FormAttachment(middle, 0);
    fdKeyField.top = new FormAttachment(wlKeyField, 0, SWT.CENTER);
    fdKeyField.right = new FormAttachment(100, 0);
    wKeyField.setLayoutData(fdKeyField);
    lastControl = wKeyField;

    Label wlMessageField = new Label(wContent, SWT.RIGHT);
    wlMessageField.setText(BaseMessages.getString(PKG, "BeamConsumeDialog.MessageField"));
    PropsUi.setLook(wlMessageField);
    FormData fdlMessageField = new FormData();
    fdlMessageField.left = new FormAttachment(0, 0);
    fdlMessageField.top = new FormAttachment(lastControl, margin);
    fdlMessageField.right = new FormAttachment(middle, -margin);
    wlMessageField.setLayoutData(fdlMessageField);
    wMessageField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMessageField);
    FormData fdMessageField = new FormData();
    fdMessageField.left = new FormAttachment(middle, 0);
    fdMessageField.top = new FormAttachment(wlMessageField, 0, SWT.CENTER);
    fdMessageField.right = new FormAttachment(100, 0);
    wMessageField.setLayoutData(fdMessageField);
    lastControl = wMessageField;

    Label wlMessageType = new Label(wContent, SWT.RIGHT);
    wlMessageType.setText(BaseMessages.getString(PKG, "BeamConsumeDialog.MessageType"));
    PropsUi.setLook(wlMessageType);
    FormData fdlMessageType = new FormData();
    fdlMessageType.left = new FormAttachment(0, 0);
    fdlMessageType.top = new FormAttachment(lastControl, margin);
    fdlMessageType.right = new FormAttachment(middle, -margin);
    wlMessageType.setLayoutData(fdlMessageType);
    wMessageType = new ComboVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wMessageType.setItems(new String[] {"String", "Avro Record"});
    PropsUi.setLook(wMessageType);
    FormData fdMessageType = new FormData();
    fdMessageType.left = new FormAttachment(middle, 0);
    fdMessageType.top = new FormAttachment(wlMessageType, 0, SWT.CENTER);
    fdMessageType.right = new FormAttachment(100, 0);
    wMessageType.setLayoutData(fdMessageType);
    lastControl = wMessageType;

    Label wlSchemaRegistryUrl = new Label(wContent, SWT.RIGHT);
    wlSchemaRegistryUrl.setText(BaseMessages.getString(PKG, "BeamConsumeDialog.SchemaRegistryUrl"));
    PropsUi.setLook(wlSchemaRegistryUrl);
    FormData fdlSchemaRegistryUrl = new FormData();
    fdlSchemaRegistryUrl.left = new FormAttachment(0, 0);
    fdlSchemaRegistryUrl.top = new FormAttachment(lastControl, margin);
    fdlSchemaRegistryUrl.right = new FormAttachment(middle, -margin);
    wlSchemaRegistryUrl.setLayoutData(fdlSchemaRegistryUrl);
    wSchemaRegistryUrl = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchemaRegistryUrl);
    FormData fdSchemaRegistryUrl = new FormData();
    fdSchemaRegistryUrl.left = new FormAttachment(middle, 0);
    fdSchemaRegistryUrl.top = new FormAttachment(wlSchemaRegistryUrl, 0, SWT.CENTER);
    fdSchemaRegistryUrl.right = new FormAttachment(100, 0);
    wSchemaRegistryUrl.setLayoutData(fdSchemaRegistryUrl);
    lastControl = wSchemaRegistryUrl;

    Label wlSchemaRegistrySubject = new Label(wContent, SWT.RIGHT);
    wlSchemaRegistrySubject.setText(
        BaseMessages.getString(PKG, "BeamConsumeDialog.SchemaRegistrySubject"));
    PropsUi.setLook(wlSchemaRegistrySubject);
    FormData fdlSchemaRegistrySubject = new FormData();
    fdlSchemaRegistrySubject.left = new FormAttachment(0, 0);
    fdlSchemaRegistrySubject.top = new FormAttachment(lastControl, margin);
    fdlSchemaRegistrySubject.right = new FormAttachment(middle, -margin);
    wlSchemaRegistrySubject.setLayoutData(fdlSchemaRegistrySubject);
    wSchemaRegistrySubject = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchemaRegistrySubject);
    FormData fdSchemaRegistrySubject = new FormData();
    fdSchemaRegistrySubject.left = new FormAttachment(middle, 0);
    fdSchemaRegistrySubject.top = new FormAttachment(wlSchemaRegistrySubject, 0, SWT.CENTER);
    fdSchemaRegistrySubject.right = new FormAttachment(100, 0);
    wSchemaRegistrySubject.setLayoutData(fdSchemaRegistrySubject);
    lastControl = wSchemaRegistrySubject;

    Label wlUseProcessingTime = new Label(wContent, SWT.RIGHT);
    wlUseProcessingTime.setText(BaseMessages.getString(PKG, "BeamProduceDialog.UseProcessingTime"));
    PropsUi.setLook(wlUseProcessingTime);
    FormData fdlUseProcessingTime = new FormData();
    fdlUseProcessingTime.left = new FormAttachment(0, 0);
    fdlUseProcessingTime.top = new FormAttachment(lastControl, margin);
    fdlUseProcessingTime.right = new FormAttachment(middle, -margin);
    wlUseProcessingTime.setLayoutData(fdlUseProcessingTime);
    wUseProcessingTime = new Button(wContent, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wUseProcessingTime);
    FormData fdUseProcessingTime = new FormData();
    fdUseProcessingTime.left = new FormAttachment(middle, 0);
    fdUseProcessingTime.top = new FormAttachment(wlUseProcessingTime, 0, SWT.CENTER);
    fdUseProcessingTime.right = new FormAttachment(100, 0);
    wUseProcessingTime.setLayoutData(fdUseProcessingTime);
    lastControl = wUseProcessingTime;

    Label wlUseLogAppendTime = new Label(wContent, SWT.RIGHT);
    wlUseLogAppendTime.setText(BaseMessages.getString(PKG, "BeamProduceDialog.UseLogAppendTime"));
    PropsUi.setLook(wlUseLogAppendTime);
    FormData fdlUseLogAppendTime = new FormData();
    fdlUseLogAppendTime.left = new FormAttachment(0, 0);
    fdlUseLogAppendTime.top = new FormAttachment(lastControl, margin);
    fdlUseLogAppendTime.right = new FormAttachment(middle, -margin);
    wlUseLogAppendTime.setLayoutData(fdlUseLogAppendTime);
    wUseLogAppendTime = new Button(wContent, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wUseLogAppendTime);
    FormData fdUseLogAppendTime = new FormData();
    fdUseLogAppendTime.left = new FormAttachment(middle, 0);
    fdUseLogAppendTime.top = new FormAttachment(wlUseLogAppendTime, 0, SWT.CENTER);
    fdUseLogAppendTime.right = new FormAttachment(100, 0);
    wUseLogAppendTime.setLayoutData(fdUseLogAppendTime);
    lastControl = wUseLogAppendTime;

    Label wlUseCreateTime = new Label(wContent, SWT.RIGHT);
    wlUseCreateTime.setText(BaseMessages.getString(PKG, "BeamProduceDialog.UseCreateTime"));
    PropsUi.setLook(wlUseCreateTime);
    FormData fdlUseCreateTime = new FormData();
    fdlUseCreateTime.left = new FormAttachment(0, 0);
    fdlUseCreateTime.top = new FormAttachment(lastControl, margin);
    fdlUseCreateTime.right = new FormAttachment(middle, -margin);
    wlUseCreateTime.setLayoutData(fdlUseCreateTime);
    wUseCreateTime = new Button(wContent, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wUseCreateTime);
    FormData fdUseCreateTime = new FormData();
    fdUseCreateTime.left = new FormAttachment(middle, 0);
    fdUseCreateTime.top = new FormAttachment(wlUseCreateTime, 0, SWT.CENTER);
    fdUseCreateTime.right = new FormAttachment(100, 0);
    wUseCreateTime.setLayoutData(fdUseCreateTime);
    lastControl = wUseCreateTime;

    Label wlRestrictToCommitted = new Label(wContent, SWT.RIGHT);
    wlRestrictToCommitted.setText(
        BaseMessages.getString(PKG, "BeamProduceDialog.RestrictToCommitted"));
    PropsUi.setLook(wlRestrictToCommitted);
    FormData fdlRestrictToCommitted = new FormData();
    fdlRestrictToCommitted.left = new FormAttachment(0, 0);
    fdlRestrictToCommitted.top = new FormAttachment(lastControl, margin);
    fdlRestrictToCommitted.right = new FormAttachment(middle, -margin);
    wlRestrictToCommitted.setLayoutData(fdlRestrictToCommitted);
    wRestrictToCommitted = new Button(wContent, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wRestrictToCommitted);
    FormData fdRestrictToCommitted = new FormData();
    fdRestrictToCommitted.left = new FormAttachment(middle, 0);
    fdRestrictToCommitted.top = new FormAttachment(wlRestrictToCommitted, 0, SWT.CENTER);
    fdRestrictToCommitted.right = new FormAttachment(100, 0);
    wRestrictToCommitted.setLayoutData(fdRestrictToCommitted);
    lastControl = wlRestrictToCommitted;

    Label wlAllowCommitConsumed = new Label(wContent, SWT.RIGHT);
    wlAllowCommitConsumed.setText(
        BaseMessages.getString(PKG, "BeamProduceDialog.AllowCommitConsumed"));
    PropsUi.setLook(wlAllowCommitConsumed);
    FormData fdlAllowCommitConsumed = new FormData();
    fdlAllowCommitConsumed.left = new FormAttachment(0, 0);
    fdlAllowCommitConsumed.top = new FormAttachment(lastControl, margin);
    fdlAllowCommitConsumed.right = new FormAttachment(middle, -margin);
    wlAllowCommitConsumed.setLayoutData(fdlAllowCommitConsumed);
    wAllowCommitConsumed = new Button(wContent, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wAllowCommitConsumed);
    FormData fdAllowCommitConsumed = new FormData();
    fdAllowCommitConsumed.left = new FormAttachment(middle, 0);
    fdAllowCommitConsumed.top = new FormAttachment(wlAllowCommitConsumed, 0, SWT.CENTER);
    fdAllowCommitConsumed.right = new FormAttachment(100, 0);
    wAllowCommitConsumed.setLayoutData(fdAllowCommitConsumed);
    lastControl = wlAllowCommitConsumed;

    Label wlConfigOptions = new Label(wContent, SWT.LEFT);
    wlConfigOptions.setText(BaseMessages.getString(PKG, "BeamProduceDialog.ConfigOptions"));
    PropsUi.setLook(wlConfigOptions);
    FormData fdlConfigOptions = new FormData();
    fdlConfigOptions.left = new FormAttachment(0, 0);
    fdlConfigOptions.top = new FormAttachment(lastControl, margin);
    fdlConfigOptions.right = new FormAttachment(100, 0);
    wlConfigOptions.setLayoutData(fdlConfigOptions);
    lastControl = wlConfigOptions;

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
              false),
        };
    columns[0].setUsingVariables(true);
    columns[1].setUsingVariables(true);

    wConfigOptions =
        new TableView(
            variables, wContent, SWT.NONE, columns, input.getConfigOptions().size(), null, props);
    PropsUi.setLook(wConfigOptions);
    FormData fdConfigOptions = new FormData();
    fdConfigOptions.left = new FormAttachment(0, 0);
    fdConfigOptions.right = new FormAttachment(100, 0);
    fdConfigOptions.top = new FormAttachment(lastControl, margin);
    fdConfigOptions.bottom = new FormAttachment(100, -margin);
    wConfigOptions.setLayoutData(fdConfigOptions);

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    scrolledComposite.setContent(wContent);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setMinWidth(bounds.width);
    scrolledComposite.setMinHeight(bounds.height);

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

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Populate the widgets. */
  public void getData() {
    wBootstrapServers.setText(Const.NVL(input.getBootstrapServers(), ""));
    wTopics.setText(Const.NVL(input.getTopics(), ""));
    wGroupId.setText(Const.NVL(input.getGroupId(), ""));
    wKeyField.setText(Const.NVL(input.getKeyField(), ""));
    wMessageField.setText(Const.NVL(input.getMessageField(), ""));
    wMessageType.setText(Const.NVL(input.getMessageType(), ""));
    wSchemaRegistryUrl.setText(Const.NVL(input.getSchemaRegistryUrl(), ""));
    wSchemaRegistrySubject.setText(Const.NVL(input.getSchemaRegistrySubject(), ""));

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
    wConfigOptions.optimizeTableView();
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
    in.setMessageType(wMessageType.getText());
    in.setGroupId(wGroupId.getText());
    in.setSchemaRegistryUrl(wSchemaRegistryUrl.getText());
    in.setSchemaRegistrySubject(wSchemaRegistrySubject.getText());

    in.setUsingProcessingTime(wUseProcessingTime.getSelection());
    in.setUsingLogAppendTime(wUseLogAppendTime.getSelection());
    in.setUsingCreateTime(wUseCreateTime.getSelection());
    in.setRestrictedToCommitted(wRestrictToCommitted.getSelection());
    in.setAllowingCommitOnConsumedOffset(wAllowCommitConsumed.getSelection());

    in.getConfigOptions().clear();
    List<TableItem> nonEmptyItems = wConfigOptions.getNonEmptyItems();
    for (TableItem item : nonEmptyItems) {
      String parameter = item.getText(1);
      String value = item.getText(2);
      ConfigOption.Type type = ConfigOption.Type.getTypeFromName(item.getText(3));
      in.getConfigOptions().add(new ConfigOption(parameter, value, type));
    }

    input.setChanged();
  }
}
