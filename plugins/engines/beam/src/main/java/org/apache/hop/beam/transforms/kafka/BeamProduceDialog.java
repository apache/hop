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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class BeamProduceDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BeamProduce.class;
  private final BeamProduceMeta input;

  private TextVar wBootstrapServers;
  private TextVar wTopic;
  private TextVar wKeyField;
  private TextVar wMessageField;
  private TableView wConfigOptions;

  public BeamProduceDialog(
      Shell parent,
      IVariables variables,
      BeamProduceMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "BeamProduceDialog.DialogTitle"));

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
    wlBootstrapServers.setText(BaseMessages.getString(PKG, "BeamProduceDialog.BootstrapServers"));
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

    Label wlTopic = new Label(wContent, SWT.RIGHT);
    wlTopic.setText(BaseMessages.getString(PKG, "BeamProduceDialog.Topic"));
    PropsUi.setLook(wlTopic);
    FormData fdlTopic = new FormData();
    fdlTopic.left = new FormAttachment(0, 0);
    fdlTopic.top = new FormAttachment(lastControl, margin);
    fdlTopic.right = new FormAttachment(middle, -margin);
    wlTopic.setLayoutData(fdlTopic);
    wTopic = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTopic);
    FormData fdTopic = new FormData();
    fdTopic.left = new FormAttachment(middle, 0);
    fdTopic.top = new FormAttachment(wlTopic, 0, SWT.CENTER);
    fdTopic.right = new FormAttachment(100, 0);
    wTopic.setLayoutData(fdTopic);
    lastControl = wTopic;

    Label wlKeyField = new Label(wContent, SWT.RIGHT);
    wlKeyField.setText(BaseMessages.getString(PKG, "BeamProduceDialog.KeyField"));
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
    wlMessageField.setText(BaseMessages.getString(PKG, "BeamProduceDialog.MessageField"));
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

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Populate the widgets. */
  public void getData() {
    wBootstrapServers.setText(Const.NVL(input.getBootstrapServers(), ""));
    wTopic.setText(Const.NVL(input.getTopic(), ""));
    wKeyField.setText(Const.NVL(input.getKeyField(), ""));
    wMessageField.setText(Const.NVL(input.getMessageField(), ""));

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

  private void getInfo(BeamProduceMeta in) {
    transformName = wTransformName.getText(); // return value

    in.setBootstrapServers(wBootstrapServers.getText());
    in.setTopic(wTopic.getText());
    in.setKeyField(wKeyField.getText());
    in.setMessageField(wMessageField.getText());

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
