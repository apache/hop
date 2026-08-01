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

package org.apache.hop.redis.transforms.redisinput;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.redis.codec.RedisCodecType;
import org.apache.hop.redis.metadata.RedisConnection;
import org.apache.hop.redis.transforms.RedisDataStructure;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/**
 * Redis Input dialog: connection plus a per-row mapping table (STRING / HGET / SMEMBERS / LRANGE).
 */
public class RedisInputDialog extends BaseTransformDialog {

  private static final Class<?> PKG = RedisInputMeta.class;

  private final RedisInputMeta input;

  private MetaSelectionLine<RedisConnection> wConnection;
  private Label wlFields;
  private TableView wFields;

  public RedisInputDialog(
      Shell parent, IVariables variables, RedisInputMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    this.input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "RedisInput.Name"));
    buildButtonBar().ok(e -> ok()).get(e -> getFields()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    ScrolledComposite wScrolledComposite =
        new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    FormLayout scFormLayout = new FormLayout();
    wScrolledComposite.setLayout(scFormLayout);
    FormData fdSComposite = new FormData();
    fdSComposite.left = new FormAttachment(0, 0);
    fdSComposite.right = new FormAttachment(100, 0);
    fdSComposite.top = new FormAttachment(wSpacer, 0);
    fdSComposite.bottom = new FormAttachment(wOk, -margin);
    wScrolledComposite.setLayoutData(fdSComposite);

    Composite wComposite = new Composite(wScrolledComposite, SWT.NONE);
    PropsUi.setLook(wComposite);
    FormData fdComposite = new FormData();
    fdComposite.left = new FormAttachment(0, 0);
    fdComposite.right = new FormAttachment(100, 0);
    fdComposite.top = new FormAttachment(0, 0);
    fdComposite.bottom = new FormAttachment(100, 0);
    wComposite.setLayoutData(fdComposite);
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    wComposite.setLayout(formLayout);

    String[] fieldNames = new String[0];
    try {
      fieldNames = pipelineMeta.getPrevTransformFields(variables, transformMeta).getFieldNames();
    } catch (HopException e) {
      log.logError("Error getting input fields", e);
    }

    wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            RedisConnection.class,
            wComposite,
            SWT.NONE,
            BaseMessages.getString(PKG, "RedisInputDialog.Connection.Label"),
            BaseMessages.getString(PKG, "RedisInputDialog.Connection.Tooltip"));
    PropsUi.setLook(wConnection);
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, 0);
    fdConnection.right = new FormAttachment(100, 0);
    fdConnection.top = new FormAttachment(0, margin);
    wConnection.setLayoutData(fdConnection);
    try {
      wConnection.fillItems();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error loading Redis connections", e);
    }
    wConnection.addModifyListener(lsMod);
    Control last = wConnection;

    wlFields = new Label(wComposite, SWT.LEFT);
    wlFields.setText(BaseMessages.getString(PKG, "RedisInputDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(last, margin);
    wlFields.setLayoutData(fdlFields);

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "RedisInputDialog.Column.RedisKey"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              fieldNames,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RedisInputDialog.Column.RedisKeyCodec"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              RedisCodecType.getNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RedisInputDialog.Column.DataStructure"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              RedisDataStructure.getNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RedisInputDialog.Column.HashField"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              fieldNames,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RedisInputDialog.Column.HashFieldCodec"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              RedisCodecType.getNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RedisInputDialog.Column.ValueField"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RedisInputDialog.Column.ValueCodec"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              RedisCodecType.getNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RedisInputDialog.Column.ListStart"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RedisInputDialog.Column.ListStop"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false)
        };
    columns[5].setUsingVariables(true);
    columns[7].setUsingVariables(true);
    columns[8].setUsingVariables(true);

    int fieldRows =
        input.getFields() == null || input.getFields().isEmpty() ? 5 : input.getFields().size();
    wFields =
        new TableView(
            variables,
            wComposite,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            fieldRows,
            lsMod,
            props);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.bottom = new FormAttachment(100, 0);
    wFields.setLayoutData(fdFields);

    wComposite.pack();
    Rectangle bounds = wComposite.getBounds();
    wScrolledComposite.setContent(wComposite);
    wScrolledComposite.setExpandHorizontal(true);
    wScrolledComposite.setExpandVertical(true);
    wScrolledComposite.setMinWidth(bounds.width);
    wScrolledComposite.setMinHeight(bounds.height);

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void getFields() {
    try {
      IRowMeta rowMeta = pipelineMeta.getPrevTransformFields(variables, transformName);
      BaseTransformDialog.getFieldsFromPrevious(
          rowMeta,
          wFields,
          1,
          new int[] {1},
          new int[] {},
          -1,
          -1,
          (item, valueMeta) -> {
            String name = Const.NVL(valueMeta.getName(), "");
            item.setText(2, RedisCodecType.STRING.name());
            item.setText(3, RedisDataStructure.STRING.name());
            item.setText(4, "");
            item.setText(5, "");
            // Avoid clashing with the upstream field used as Redis key
            item.setText(6, StringUtils.isEmpty(name) ? "" : name + "Value");
            item.setText(7, RedisCodecType.STRING.name());
            item.setText(8, "0");
            item.setText(9, "-1");
            return true;
          });
      input.setChanged();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting fields", e);
    }
  }

  private void getData() {
    wConnection.setText(Const.NVL(input.getConnectionName(), ""));
    wFields.clearAll(false);
    if (input.getFields() != null) {
      for (RedisInputField field : input.getFields()) {
        TableItem item = new TableItem(wFields.table, SWT.NONE);
        item.setText(1, Const.NVL(field.getRedisKey(), ""));
        item.setText(2, codecName(field.getRedisKeyCodec()));
        item.setText(3, field.resolveDataStructure().name());
        item.setText(4, Const.NVL(field.getHashField(), ""));
        item.setText(5, optionalCodecName(field.getHashFieldCodec()));
        item.setText(6, Const.NVL(field.getValueField(), ""));
        item.setText(7, codecName(field.getValueCodec()));
        item.setText(8, Const.NVL(field.getListStart(), "0"));
        item.setText(9, Const.NVL(field.getListStop(), "-1"));
      }
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);
  }

  private static String codecName(RedisCodecType type) {
    return type == null ? RedisCodecType.STRING.name() : type.name();
  }

  private static String optionalCodecName(RedisCodecType type) {
    return type == null ? "" : type.name();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText();
    input.setConnectionName(wConnection.getText());

    List<RedisInputField> fields = new ArrayList<>();
    for (TableItem item : wFields.getNonEmptyItems()) {
      RedisInputField field = new RedisInputField();
      field.setRedisKey(item.getText(1));
      field.setRedisKeyCodec(RedisCodecType.fromCode(item.getText(2)));
      field.setDataStructure(RedisDataStructure.fromCode(item.getText(3)));
      field.setHashField(item.getText(4));
      String hashCodec = item.getText(5);
      field.setHashFieldCodec(
          StringUtils.isEmpty(hashCodec) ? null : RedisCodecType.fromCode(hashCodec));
      field.setValueField(item.getText(6));
      field.setValueCodec(RedisCodecType.fromCode(item.getText(7)));
      String listStart = item.getText(8);
      field.setListStart(StringUtils.isEmpty(listStart) ? "0" : listStart);
      String listStop = item.getText(9);
      field.setListStop(StringUtils.isEmpty(listStop) ? "-1" : listStop);
      fields.add(field);
    }
    input.setFields(fields);
    input.setChanged();
    dispose();
  }
}
