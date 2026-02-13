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

package org.apache.hop.pipeline.transforms.kafka.producer;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.kafka.shared.KafkaDialogHelper;
import org.apache.hop.pipeline.transforms.kafka.shared.KafkaFactory;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class KafkaProducerOutputDialog extends BaseTransformDialog {

  private static final Class<?> PKG = KafkaProducerOutputMeta.class;

  private static final ImmutableMap<String, String> DEFAULT_OPTION_VALUES =
      ImmutableMap.of(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
  public static final String CONST_KAFKA_PRODUCER_OUTPUT_DIALOG_FIELD_NOT_EXISTS_TITLE =
      "KafkaProducerOutputDialog.FieldNotExists.Title";

  private final KafkaFactory kafkaFactory = KafkaFactory.defaultFactory();

  private static final int SHELL_MIN_WIDTH = 527;
  private static final int SHELL_MIN_HEIGHT = 569;

  private final KafkaProducerOutputMeta meta;
  private ModifyListener lsMod;

  private TextVar wClientId;
  private ComboVar wTopic;
  private ComboVar wKeyField;
  private ComboVar wMessageField;
  private TableView optionsTable;
  private CTabFolder wTabFolder;

  private TextVar wBootstrapServers;

  public KafkaProducerOutputDialog(
      Shell parent,
      IVariables variables,
      KafkaProducerOutputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    meta = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "KafkaProducerOutputDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = meta.hasChanged();
    lsMod = e -> meta.setChanged();

    Control lastControl = wSpacer;

    // Start of tabbed display
    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);
    wTabFolder.setUnselectedCloseVisible(true);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wSpacer, margin);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    wTabFolder.setLayoutData(fdTabFolder);

    buildSetupTab();
    buildOptionsTab();

    getData();

    meta.setChanged(changed);

    wTabFolder.setSelection(0);
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void buildSetupTab() {
    CTabItem wSetupTab = new CTabItem(wTabFolder, SWT.NONE);
    wSetupTab.setFont(GuiResource.getInstance().getFontDefault());
    wSetupTab.setText(BaseMessages.getString(PKG, "KafkaProducerOutputDialog.SetupTab"));

    Composite wSetupComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wSetupComp);
    FormLayout setupLayout = new FormLayout();
    setupLayout.marginHeight = 15;
    setupLayout.marginWidth = 15;
    wSetupComp.setLayout(setupLayout);

    Label wlBootstrapServers = new Label(wSetupComp, SWT.RIGHT);
    PropsUi.setLook(wlBootstrapServers);
    wlBootstrapServers.setText(
        BaseMessages.getString(PKG, "KafkaProducerOutputDialog.BootstrapServers"));
    FormData fdlBootstrapServers = new FormData();
    fdlBootstrapServers.left = new FormAttachment(0, 0);
    fdlBootstrapServers.top = new FormAttachment(0, 0);
    fdlBootstrapServers.right = new FormAttachment(middle, -margin);
    wlBootstrapServers.setLayoutData(fdlBootstrapServers);

    wBootstrapServers = new TextVar(variables, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBootstrapServers);
    wBootstrapServers.addModifyListener(lsMod);
    FormData fdBootstrapServers = new FormData();
    fdBootstrapServers.left = new FormAttachment(wlBootstrapServers, margin);
    fdBootstrapServers.top = new FormAttachment(wlBootstrapServers, 0, SWT.CENTER);
    fdBootstrapServers.right = new FormAttachment(100, 0);
    wBootstrapServers.setLayoutData(fdBootstrapServers);

    Label wlClientId = new Label(wSetupComp, SWT.RIGHT);
    PropsUi.setLook(wlClientId);
    wlClientId.setText(BaseMessages.getString(PKG, "KafkaProducerOutputDialog.ClientId"));
    FormData fdlClientId = new FormData();
    fdlClientId.left = new FormAttachment(0, 0);
    fdlClientId.top = new FormAttachment(wBootstrapServers, margin);
    fdlClientId.right = new FormAttachment(middle, -margin);
    wlClientId.setLayoutData(fdlClientId);

    wClientId = new TextVar(variables, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wClientId);
    wClientId.addModifyListener(lsMod);
    FormData fdClientId = new FormData();
    fdClientId.left = new FormAttachment(wlClientId, margin);
    fdClientId.top = new FormAttachment(wlClientId, 0, SWT.CENTER);
    fdClientId.right = new FormAttachment(100, 0);
    wClientId.setLayoutData(fdClientId);

    Label wlTopic = new Label(wSetupComp, SWT.RIGHT);
    PropsUi.setLook(wlTopic);
    wlTopic.setText(BaseMessages.getString(PKG, "KafkaProducerOutputDialog.Topic"));
    FormData fdlTopic = new FormData();
    fdlTopic.left = new FormAttachment(0, 0);
    fdlTopic.top = new FormAttachment(wClientId, margin);
    fdlTopic.right = new FormAttachment(middle, -margin);
    wlTopic.setLayoutData(fdlTopic);

    wTopic = new ComboVar(variables, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTopic);
    wTopic.addModifyListener(lsMod);
    FormData fdTopic = new FormData();
    fdTopic.left = new FormAttachment(wlTopic, margin);
    fdTopic.top = new FormAttachment(wlTopic, 0, SWT.CENTER);
    fdTopic.right = new FormAttachment(100, 0); // 60% of dialog width
    wTopic.setLayoutData(fdTopic);
    wTopic
        .getCComboWidget()
        .addListener(
            SWT.FocusIn,
            event -> {
              KafkaDialogHelper kafkaDialogHelper =
                  new KafkaDialogHelper(
                      variables,
                      wTopic,
                      wBootstrapServers,
                      kafkaFactory,
                      optionsTable,
                      meta.getParentTransformMeta());
              kafkaDialogHelper.clusterNameChanged(event);
            });

    Label wlKeyField = new Label(wSetupComp, SWT.RIGHT);
    PropsUi.setLook(wlKeyField);
    wlKeyField.setText(BaseMessages.getString(PKG, "KafkaProducerOutputDialog.KeyField"));
    FormData fdlKeyField = new FormData();
    fdlKeyField.left = new FormAttachment(0, 0);
    fdlKeyField.top = new FormAttachment(wTopic, margin);
    fdlKeyField.right = new FormAttachment(middle, -margin);
    wlKeyField.setLayoutData(fdlKeyField);

    wKeyField = new ComboVar(variables, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wKeyField);
    wKeyField.addModifyListener(lsMod);
    FormData fdKeyField = new FormData();
    fdKeyField.left = new FormAttachment(wlKeyField, margin);
    fdKeyField.top = new FormAttachment(wlKeyField, 0, SWT.CENTER);
    fdKeyField.right = new FormAttachment(100, 0);
    wKeyField.setLayoutData(fdKeyField);
    Listener lsKeyFocus =
        e ->
            KafkaDialogHelper.populateFieldsList(variables, pipelineMeta, wKeyField, transformName);
    wKeyField.getCComboWidget().addListener(SWT.FocusIn, lsKeyFocus);

    Label wlMessageField = new Label(wSetupComp, SWT.RIGHT);
    PropsUi.setLook(wlMessageField);
    wlMessageField.setText(BaseMessages.getString(PKG, "KafkaProducerOutputDialog.MessageField"));
    FormData fdlMessageField = new FormData();
    fdlMessageField.left = new FormAttachment(0, 0);
    fdlMessageField.top = new FormAttachment(wKeyField, margin);
    fdlMessageField.right = new FormAttachment(middle, -margin);
    wlMessageField.setLayoutData(fdlMessageField);

    wMessageField = new ComboVar(variables, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMessageField);
    wMessageField.addModifyListener(lsMod);
    FormData fdMessageField = new FormData();
    fdMessageField.left = new FormAttachment(wlMessageField, margin);
    fdMessageField.top = new FormAttachment(wlMessageField, 0, SWT.CENTER);
    fdMessageField.right = new FormAttachment(100, 0);
    wMessageField.setLayoutData(fdMessageField);
    Listener lsMessageFocus =
        e ->
            KafkaDialogHelper.populateFieldsList(
                variables, pipelineMeta, wMessageField, transformName);
    wMessageField.getCComboWidget().addListener(SWT.FocusIn, lsMessageFocus);

    FormData fdSetupComp = new FormData();
    fdSetupComp.left = new FormAttachment(0, 0);
    fdSetupComp.top = new FormAttachment(0, 0);
    fdSetupComp.right = new FormAttachment(100, 0);
    fdSetupComp.bottom = new FormAttachment(100, 0);
    wSetupComp.setLayoutData(fdSetupComp);
    wSetupComp.layout();
    wSetupTab.setControl(wSetupComp);
  }

  private void buildOptionsTab() {
    CTabItem wOptionsTab = new CTabItem(wTabFolder, SWT.NONE);
    wOptionsTab.setFont(GuiResource.getInstance().getFontDefault());
    wOptionsTab.setText(BaseMessages.getString(PKG, "KafkaProducerOutputDialog.Options.Tab"));
    Composite wOptionsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wOptionsComp);
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginHeight = 15;
    fieldsLayout.marginWidth = 15;
    wOptionsComp.setLayout(fieldsLayout);

    FormData optionsFormData = new FormData();
    optionsFormData.left = new FormAttachment(0, 0);
    optionsFormData.top = new FormAttachment(wOptionsComp, 0);
    optionsFormData.right = new FormAttachment(100, 0);
    optionsFormData.bottom = new FormAttachment(100, 0);
    wOptionsComp.setLayoutData(optionsFormData);

    buildOptionsTable(wOptionsComp);

    wOptionsComp.layout();
    wOptionsTab.setControl(wOptionsComp);
  }

  private void buildOptionsTable(Composite parentWidget) {
    ColumnInfo[] columns = getOptionsColumns();

    if (meta.getConfig().isEmpty()) {
      // initial call
      List<String> list = KafkaDialogHelper.getProducerAdvancedConfigOptionNames();
      Map<String, String> advancedConfig = new LinkedHashMap<>();
      for (String item : list) {
        advancedConfig.put(item, DEFAULT_OPTION_VALUES.getOrDefault(item, ""));
      }
      meta.setConfig(advancedConfig);
    }
    int fieldCount = meta.getConfig().size();

    optionsTable =
        new TableView(
            variables,
            parentWidget,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            fieldCount,
            false,
            lsMod,
            props,
            false);

    optionsTable.setSortable(false);

    FormData fdData = new FormData();
    fdData.left = new FormAttachment(0, 0);
    fdData.top = new FormAttachment(0, 0);
    fdData.right = new FormAttachment(100, 0);
    fdData.bottom = new FormAttachment(100, 0);

    // resize the columns to fit the data in them
    Arrays.stream(optionsTable.getTable().getColumns())
        .forEach(
            column -> {
              if (column.getWidth() > 0) {
                // don't pack anything with a 0 width, it will resize it to make it visible (like
                // the index column)
                column.setWidth(120);
              }
            });

    optionsTable.setLayoutData(fdData);
  }

  private ColumnInfo[] getOptionsColumns() {

    ColumnInfo optionName =
        new ColumnInfo(
            BaseMessages.getString(PKG, "KafkaProducerOutputDialog.Options.Column.Name"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false,
            false);

    ColumnInfo value =
        new ColumnInfo(
            BaseMessages.getString(PKG, "KafkaProducerOutputDialog.Options.Column.Value"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false,
            false);
    value.setUsingVariables(true);

    return new ColumnInfo[] {optionName, value};
  }

  private void populateOptionsData() {
    int rowIndex = 0;
    for (Map.Entry<String, String> entry : meta.getConfig().entrySet()) {
      TableItem key = optionsTable.getTable().getItem(rowIndex++);
      key.setText(1, entry.getKey());
      key.setText(2, entry.getValue());
    }
    optionsTable.optimizeTableView();
  }

  private void getData() {
    wBootstrapServers.setText(Const.NVL(meta.getDirectBootstrapServers(), ""));
    wClientId.setText(Const.NVL(meta.getClientId(), ""));
    wTopic.setText(Const.NVL(meta.getTopic(), ""));
    wKeyField.setText(Const.NVL(meta.getKeyField(), ""));
    wMessageField.setText(Const.NVL(meta.getMessageField(), ""));

    populateOptionsData();
  }

  private boolean checkIfFieldExists(String fieldName) {
    boolean fieldFound = false;
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      String[] fieldNames = r.getFieldNames();
      for (String name : fieldNames) {
        if (fieldName.equals(name)) {
          fieldFound = true;
          break;
        }
      }
    } catch (HopTransformException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "KafkaProducerOutputDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "KafkaProducerOutputDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
    return fieldFound;
  }

  private void cancel() {
    meta.setChanged(false);
    dispose();
  }

  private void ok() {

    if (Utils.isEmpty(wBootstrapServers.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(
              PKG, "KafkaProducerOutputDialog.BootstrapServerMandatory.Message"));
      mb.setText(
          BaseMessages.getString(PKG, CONST_KAFKA_PRODUCER_OUTPUT_DIALOG_FIELD_NOT_EXISTS_TITLE));
      mb.open();
      return;
    }

    meta.setDirectBootstrapServers(wBootstrapServers.getText());
    meta.setClientId(wClientId.getText());
    meta.setTopic(wTopic.getText());
    if (!Utils.isEmpty(wKeyField.getText()) && !checkIfFieldExists(wKeyField.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(
              PKG, "KafkaProducerOutputDialog.KeyFieldNotExists.Message", wKeyField.getText()));
      mb.setText(
          BaseMessages.getString(PKG, CONST_KAFKA_PRODUCER_OUTPUT_DIALOG_FIELD_NOT_EXISTS_TITLE));
      mb.open();
      return;
    }

    meta.setKeyField(wKeyField.getText());
    if (Utils.isEmpty(wMessageField.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "KafkaProducerOutputDialog.MessageFieldMandatory.Message"));
      mb.setText(
          BaseMessages.getString(PKG, CONST_KAFKA_PRODUCER_OUTPUT_DIALOG_FIELD_NOT_EXISTS_TITLE));
      mb.open();
      return;
    }

    if (!checkIfFieldExists(wMessageField.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(
              PKG,
              "KafkaProducerOutputDialog.MessageFieldNotExists.Message",
              wMessageField.getText()));
      mb.setText(
          BaseMessages.getString(PKG, CONST_KAFKA_PRODUCER_OUTPUT_DIALOG_FIELD_NOT_EXISTS_TITLE));
      mb.open();
      return;
    }

    meta.setMessageField(wMessageField.getText());
    meta.setConfig(KafkaDialogHelper.getConfig(optionsTable));

    transformName = wTransformName.getText();

    dispose();
  }
}
