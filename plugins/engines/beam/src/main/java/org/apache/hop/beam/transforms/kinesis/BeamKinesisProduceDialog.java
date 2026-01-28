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

package org.apache.hop.beam.transforms.kinesis;

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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class BeamKinesisProduceDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BeamKinesisProduce.class;
  private final BeamKinesisProduceMeta input;

  private TextVar wSecretKey;
  private TextVar wAccessKey;
  private TextVar wStreamName;
  private TextVar wDataField;
  private ComboVar wDataType;
  private TextVar wPartitionKey;

  private TableView wConfigOptions;

  public BeamKinesisProduceDialog(
      Shell parent,
      IVariables variables,
      BeamKinesisProduceMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "BeamKinesisProduceDialog.DialogTitle"));
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

    Label wlAccessKey = new Label(wContent, SWT.RIGHT);
    wlAccessKey.setText(BaseMessages.getString(PKG, "BeamKinesisProduceDialog.AccessKey"));
    PropsUi.setLook(wlAccessKey);
    FormData fdlAccessKey = new FormData();
    fdlAccessKey.left = new FormAttachment(0, 0);
    fdlAccessKey.top = new FormAttachment(0, margin);
    fdlAccessKey.right = new FormAttachment(middle, -margin);
    wlAccessKey.setLayoutData(fdlAccessKey);
    wAccessKey =
        new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    PropsUi.setLook(wAccessKey);
    FormData fdAccessKey = new FormData();
    fdAccessKey.left = new FormAttachment(middle, 0);
    fdAccessKey.top = new FormAttachment(wlAccessKey, 0, SWT.CENTER);
    fdAccessKey.right = new FormAttachment(100, 0);
    wAccessKey.setLayoutData(fdAccessKey);
    lastControl = wAccessKey;

    Label wlSecretKey = new Label(wContent, SWT.RIGHT);
    wlSecretKey.setText(BaseMessages.getString(PKG, "BeamKinesisProduceDialog.SecretKey"));
    PropsUi.setLook(wlSecretKey);
    FormData fdlSecretKey = new FormData();
    fdlSecretKey.left = new FormAttachment(0, 0);
    fdlSecretKey.top = new FormAttachment(lastControl, margin);
    fdlSecretKey.right = new FormAttachment(middle, -margin);
    wlSecretKey.setLayoutData(fdlSecretKey);
    wSecretKey =
        new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    PropsUi.setLook(wSecretKey);
    FormData fdSecretKey = new FormData();
    fdSecretKey.left = new FormAttachment(middle, 0);
    fdSecretKey.top = new FormAttachment(wlSecretKey, 0, SWT.CENTER);
    fdSecretKey.right = new FormAttachment(100, 0);
    wSecretKey.setLayoutData(fdSecretKey);
    lastControl = wSecretKey;

    Label wlStreamName = new Label(wContent, SWT.RIGHT);
    wlStreamName.setText(BaseMessages.getString(PKG, "BeamKinesisProduceDialog.StreamName"));
    PropsUi.setLook(wlStreamName);
    FormData fdlStreamName = new FormData();
    fdlStreamName.left = new FormAttachment(0, 0);
    fdlStreamName.top = new FormAttachment(lastControl, margin);
    fdlStreamName.right = new FormAttachment(middle, -margin);
    wlStreamName.setLayoutData(fdlStreamName);
    wStreamName = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wStreamName);
    FormData fdStreamName = new FormData();
    fdStreamName.left = new FormAttachment(middle, 0);
    fdStreamName.top = new FormAttachment(wlStreamName, 0, SWT.CENTER);
    fdStreamName.right = new FormAttachment(100, 0);
    wStreamName.setLayoutData(fdStreamName);
    lastControl = wStreamName;

    Label wlDataField = new Label(wContent, SWT.RIGHT);
    wlDataField.setText(BaseMessages.getString(PKG, "BeamKinesisProduceDialog.DataField"));
    PropsUi.setLook(wlDataField);
    FormData fdlDataField = new FormData();
    fdlDataField.left = new FormAttachment(0, 0);
    fdlDataField.top = new FormAttachment(lastControl, margin);
    fdlDataField.right = new FormAttachment(middle, -margin);
    wlDataField.setLayoutData(fdlDataField);
    wDataField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDataField);
    FormData fdDataField = new FormData();
    fdDataField.left = new FormAttachment(middle, 0);
    fdDataField.top = new FormAttachment(wlDataField, 0, SWT.CENTER);
    fdDataField.right = new FormAttachment(100, 0);
    wDataField.setLayoutData(fdDataField);
    lastControl = wDataField;

    Label wlDataType = new Label(wContent, SWT.RIGHT);
    wlDataType.setText(BaseMessages.getString(PKG, "BeamKinesisProduceDialog.DataType"));
    PropsUi.setLook(wlDataType);
    FormData fdlDataType = new FormData();
    fdlDataType.left = new FormAttachment(0, 0);
    fdlDataType.top = new FormAttachment(lastControl, margin);
    fdlDataType.right = new FormAttachment(middle, -margin);
    wlDataType.setLayoutData(fdlDataType);
    wDataType = new ComboVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wDataType.setItems(
        new String[] {
          "String",
        }); // TODO add "Avro Record"
    PropsUi.setLook(wDataType);
    FormData fdDataType = new FormData();
    fdDataType.left = new FormAttachment(middle, 0);
    fdDataType.top = new FormAttachment(wlDataType, 0, SWT.CENTER);
    fdDataType.right = new FormAttachment(100, 0);
    wDataType.setLayoutData(fdDataType);
    lastControl = wDataType;

    Label wlPartitionKey = new Label(wContent, SWT.RIGHT);
    wlPartitionKey.setText(BaseMessages.getString(PKG, "BeamKinesisProduceDialog.PartitionKey"));
    PropsUi.setLook(wlPartitionKey);
    FormData fdlPartitionKey = new FormData();
    fdlPartitionKey.left = new FormAttachment(0, 0);
    fdlPartitionKey.top = new FormAttachment(lastControl, margin);
    fdlPartitionKey.right = new FormAttachment(middle, -margin);
    wlPartitionKey.setLayoutData(fdlPartitionKey);
    wPartitionKey = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPartitionKey);
    FormData fdPartitionKey = new FormData();
    fdPartitionKey.left = new FormAttachment(middle, 0);
    fdPartitionKey.top = new FormAttachment(wlPartitionKey, 0, SWT.CENTER);
    fdPartitionKey.right = new FormAttachment(100, 0);
    wPartitionKey.setLayoutData(fdPartitionKey);
    lastControl = wPartitionKey;

    Label wlConfigOptions = new Label(wContent, SWT.RIGHT);
    wlConfigOptions.setText(BaseMessages.getString(PKG, "BeamKinesisProduceDialog.ConfigOptions"));
    PropsUi.setLook(wlConfigOptions);
    FormData fdlConfigOptions = new FormData();
    fdlConfigOptions.left = new FormAttachment(0, 0);
    fdlConfigOptions.top = new FormAttachment(lastControl, margin);
    fdlConfigOptions.right = new FormAttachment(middle, -margin);
    wlConfigOptions.setLayoutData(fdlConfigOptions);
    lastControl = wlConfigOptions;

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "BeamKinesisProduceDialog.ConfigOptions.Column.Property"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "BeamKinesisProduceDialog.ConfigOptions.Column.Value"),
              ColumnInfo.COLUMN_TYPE_TEXT,
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
    wSecretKey.setText(Const.NVL(input.getSecretKey(), ""));
    wAccessKey.setText(Const.NVL(input.getAccessKey(), ""));
    wStreamName.setText(Const.NVL(input.getStreamName(), ""));
    wDataField.setText(Const.NVL(input.getDataField(), ""));
    wDataType.setText(Const.NVL(input.getDataType(), ""));
    wPartitionKey.setText(Const.NVL(input.getPartitionKey(), ""));

    for (int i = 0; i < input.getConfigOptions().size(); i++) {
      KinesisConfigOption option = input.getConfigOptions().get(i);
      TableItem item = wConfigOptions.table.getItem(i);
      item.setText(1, Const.NVL(option.getParameter(), ""));
      item.setText(2, Const.NVL(option.getValue(), ""));
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

  private void getInfo(BeamKinesisProduceMeta in) {
    transformName = wTransformName.getText(); // return value

    input.setAccessKey(wAccessKey.getText());
    input.setSecretKey(wSecretKey.getText());
    input.setStreamName(wStreamName.getText());
    input.setDataField(wDataField.getText());
    input.setDataType(wDataType.getText());
    input.setPartitionKey(wPartitionKey.getText());

    in.getConfigOptions().clear();
    List<TableItem> nonEmptyItems = wConfigOptions.getNonEmptyItems();
    for (TableItem item : nonEmptyItems) {
      String parameter = item.getText(1);
      String value = item.getText(2);
      in.getConfigOptions().add(new KinesisConfigOption(parameter, value));
    }

    input.setChanged();
  }
}
