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

package org.apache.hop.redis.transforms.redisoutput;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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
import org.apache.hop.redis.transforms.RedisListPushDirection;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.custom.StackLayout;
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
 * Redis Output dialog.
 *
 * <p>Write mode switches a {@link StackLayout}. KEY_VALUE shows a structure-driven form.
 * STREAM_FIELDS shows a per-row mapping table (including per-row TTL).
 */
public class RedisOutputDialog extends BaseTransformDialog {

  private static final Class<?> PKG = RedisOutputMeta.class;

  private final RedisOutputMeta input;

  private MetaSelectionLine<RedisConnection> wConnection;
  private CCombo wWriteMode;

  // KEY_VALUE page
  private Composite wKeyValuePage;
  private CCombo wDataStructure;
  private CCombo wKeyField;
  private CCombo wKeyCodec;
  private CCombo wHashKeyField;
  private CCombo wHashKeyCodec;
  private CCombo wListPushDirection;
  private CCombo wValueField;
  private CCombo wValueCodec;
  private TextVar wTtlSeconds;

  private Label wlListPushDirection;

  private Composite wKeyRow;
  private Composite wHashKeyRow;
  private Composite wValueRow;

  // STREAM_FIELDS page
  private Composite wStreamPage;

  private TableView wFields;

  private Composite wModeStack;
  private StackLayout modeStackLayout;

  public RedisOutputDialog(
      Shell parent,
      IVariables variables,
      RedisOutputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    this.input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "RedisOutput.Name"));
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
            BaseMessages.getString(PKG, "RedisOutputDialog.Connection.Label"),
            BaseMessages.getString(PKG, "RedisOutputDialog.Connection.Tooltip"));
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

    wWriteMode = new CCombo(wComposite, SWT.BORDER | SWT.READ_ONLY);
    last =
        addLabeledCombo(
            wComposite,
            last,
            BaseMessages.getString(PKG, "RedisOutputDialog.WriteMode.Label"),
            wWriteMode,
            RedisOutputWriteMode.getNames(),
            lsMod,
            null);
    wWriteMode.addListener(SWT.Selection, e -> updateVisibility());

    modeStackLayout = new StackLayout();
    wModeStack = new Composite(wComposite, SWT.NONE);
    PropsUi.setLook(wModeStack);
    wModeStack.setLayout(modeStackLayout);
    FormData fdModeStack = new FormData();
    fdModeStack.left = new FormAttachment(0, 0);
    fdModeStack.right = new FormAttachment(100, 0);
    fdModeStack.top = new FormAttachment(last, margin);
    fdModeStack.bottom = new FormAttachment(100, -margin);
    wModeStack.setLayoutData(fdModeStack);

    buildKeyValuePage(lsMod, fieldNames);
    buildStreamPage(lsMod, fieldNames);
    modeStackLayout.topControl = wKeyValuePage;

    wComposite.pack();
    Rectangle bounds = wComposite.getBounds();
    wScrolledComposite.setContent(wComposite);
    wScrolledComposite.setExpandHorizontal(true);
    wScrolledComposite.setExpandVertical(true);
    wScrolledComposite.setMinWidth(bounds.width);
    wScrolledComposite.setMinHeight(bounds.height);

    getData();
    updateVisibility();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void buildKeyValuePage(ModifyListener lsMod, String[] fieldNames) {
    Label wlValueCodec;
    Composite wTtlRow;
    Label wlTtl;
    Label wlKeyCodec;
    Label wlHashKeyCodec;
    Label wlHashKeyField;
    Label wlKeyField;
    wKeyValuePage = new Composite(wModeStack, SWT.NONE);
    PropsUi.setLook(wKeyValuePage);
    wKeyValuePage.setLayout(new FormLayout());

    // Data structure — full width
    wDataStructure = new CCombo(wKeyValuePage, SWT.BORDER | SWT.READ_ONLY);
    addLabeledCombo(
        wKeyValuePage,
        null,
        BaseMessages.getString(PKG, "RedisOutputDialog.DataStructure.Label"),
        wDataStructure,
        RedisDataStructure.getNames(),
        lsMod,
        new Label(wKeyValuePage, SWT.RIGHT));
    wDataStructure.addListener(SWT.Selection, e -> updateKeyValueStructureVisibility());

    // key field | codec
    wKeyRow = createRowComposite(wKeyValuePage, wDataStructure);
    wKeyField = new CCombo(wKeyRow, SWT.BORDER);
    wlKeyField = new Label(wKeyRow, SWT.RIGHT);
    wKeyCodec = new CCombo(wKeyRow, SWT.BORDER | SWT.READ_ONLY);
    wlKeyCodec = new Label(wKeyRow, SWT.RIGHT);
    addPairToRow(
        new LabeledCombo(
            BaseMessages.getString(PKG, "RedisOutputDialog.KeyField.Label"),
            wlKeyField,
            wKeyField,
            fieldNames),
        new LabeledCombo(
            BaseMessages.getString(PKG, "RedisOutputDialog.KeyCodec.Label"),
            wlKeyCodec,
            wKeyCodec,
            RedisCodecType.getNames()),
        lsMod);

    // hash key field | hash key codec (HASH only)
    wHashKeyRow = createRowComposite(wKeyValuePage, wKeyRow);
    wHashKeyField = new CCombo(wHashKeyRow, SWT.BORDER);
    wlHashKeyField = new Label(wHashKeyRow, SWT.RIGHT);
    wHashKeyCodec = new CCombo(wHashKeyRow, SWT.BORDER | SWT.READ_ONLY);
    wlHashKeyCodec = new Label(wHashKeyRow, SWT.RIGHT);
    addPairToRow(
        new LabeledCombo(
            BaseMessages.getString(PKG, "RedisOutputDialog.HashKeyField.Label"),
            wlHashKeyField,
            wHashKeyField,
            fieldNames),
        new LabeledCombo(
            BaseMessages.getString(PKG, "RedisOutputDialog.HashKeyCodec.Label"),
            wlHashKeyCodec,
            wHashKeyCodec,
            RedisCodecType.getNames()),
        lsMod);

    // value field | value codec
    wValueRow = createRowComposite(wKeyValuePage, wHashKeyRow);
    wValueField = new CCombo(wValueRow, SWT.BORDER);
    Label wlValueField = new Label(wValueRow, SWT.RIGHT);
    wValueCodec = new CCombo(wValueRow, SWT.BORDER | SWT.READ_ONLY);
    wlValueCodec = new Label(wValueRow, SWT.RIGHT);
    addPairToRow(
        new LabeledCombo(
            BaseMessages.getString(PKG, "RedisOutputDialog.ValueField.Label"),
            wlValueField,
            wValueField,
            fieldNames),
        new LabeledCombo(
            BaseMessages.getString(PKG, "RedisOutputDialog.ValueCodec.Label"),
            wlValueCodec,
            wValueCodec,
            RedisCodecType.getNames()),
        lsMod);

    // TTL | list push direction
    wTtlRow = createRowComposite(wKeyValuePage, wValueRow);
    wlTtl = new Label(wTtlRow, SWT.RIGHT);
    wlTtl.setText(BaseMessages.getString(PKG, "RedisOutputDialog.TtlSeconds.Label"));
    PropsUi.setLook(wlTtl);
    FormData fdlTtl = new FormData();
    fdlTtl.left = new FormAttachment(0, 0);
    fdlTtl.top = new FormAttachment(0, 0);
    fdlTtl.width = 260;
    wlTtl.setLayoutData(fdlTtl);

    wTtlSeconds = new TextVar(variables, wTtlRow, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTtlSeconds);
    FormData fdTtl = new FormData();
    fdTtl.left = new FormAttachment(wlTtl, margin);
    fdTtl.right = new FormAttachment(50, -margin);
    fdTtl.top = new FormAttachment(wlTtl, 0, SWT.CENTER);
    wTtlSeconds.setLayoutData(fdTtl);
    wTtlSeconds.addModifyListener(lsMod);

    wlListPushDirection = new Label(wTtlRow, SWT.RIGHT);
    wlListPushDirection.setText(
        BaseMessages.getString(PKG, "RedisOutputDialog.ListPushDirection.Label"));
    PropsUi.setLook(wlListPushDirection);
    FormData fdlList = new FormData();
    fdlList.left = new FormAttachment(50, margin);
    fdlList.top = new FormAttachment(0, 0);
    fdlList.width = 260;
    wlListPushDirection.setLayoutData(fdlList);

    wListPushDirection = new CCombo(wTtlRow, SWT.BORDER | SWT.READ_ONLY);
    wListPushDirection.setItems(
        new String[] {RedisListPushDirection.RPUSH.name(), RedisListPushDirection.LPUSH.name()});
    PropsUi.setLook(wListPushDirection);
    FormData fdList = new FormData();
    fdList.left = new FormAttachment(wlListPushDirection, margin);
    fdList.right = new FormAttachment(100, 0);
    fdList.top = new FormAttachment(wlListPushDirection, 0, SWT.CENTER);
    wListPushDirection.setLayoutData(fdList);
    wListPushDirection.addModifyListener(lsMod);
  }

  private Composite createRowComposite(Composite parent, Control previous) {
    Composite row = new Composite(parent, SWT.NONE);
    PropsUi.setLook(row);
    row.setLayout(new FormLayout());
    FormData fdRow = new FormData();
    fdRow.left = new FormAttachment(0, 0);
    fdRow.right = new FormAttachment(100, 0);
    if (previous == null) {
      fdRow.top = new FormAttachment(0, 0);
    } else {
      fdRow.top = new FormAttachment(previous, margin);
    }
    row.setLayoutData(fdRow);
    return row;
  }

  private void addPairToRow(LabeledCombo left, LabeledCombo right, ModifyListener lsMod) {
    layoutLabeledCombo(left, 0, 50, lsMod);
    layoutLabeledCombo(right, 50, 100, lsMod);
  }

  private void layoutLabeledCombo(
      LabeledCombo labeledCombo, int leftPercent, int rightPercent, ModifyListener lsMod) {
    labeledCombo.label().setText(labeledCombo.labelText());
    PropsUi.setLook(labeledCombo.label());
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(leftPercent, leftPercent == 0 ? 0 : margin);
    fdl.top = new FormAttachment(0, 0);
    fdl.width = 260;
    labeledCombo.label().setLayoutData(fdl);

    labeledCombo.combo().setItems(labeledCombo.items());
    PropsUi.setLook(labeledCombo.combo());
    FormData fd = new FormData();
    fd.left = new FormAttachment(labeledCombo.label(), margin);
    fd.right = new FormAttachment(rightPercent, rightPercent == 100 ? 0 : -margin);
    fd.top = new FormAttachment(labeledCombo.label(), 0, SWT.CENTER);
    labeledCombo.combo().setLayoutData(fd);
    labeledCombo.combo().addModifyListener(lsMod);
  }

  private record LabeledCombo(String labelText, Label label, CCombo combo, String[] items) {
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      return o
              instanceof
              LabeledCombo(
                  String otherLabelText,
                  Label otherLabel,
                  CCombo otherCombo,
                  String[] otherItems)
          && Objects.equals(labelText, otherLabelText)
          && Objects.equals(label, otherLabel)
          && Objects.equals(combo, otherCombo)
          && Arrays.equals(items, otherItems);
    }

    @Override
    public int hashCode() {
      return Objects.hash(labelText, label, combo, Arrays.hashCode(items));
    }

    @Override
    public String toString() {
      return "LabeledCombo[labelText="
          + labelText
          + ", label="
          + label
          + ", combo="
          + combo
          + ", items="
          + Arrays.toString(items)
          + "]";
    }
  }

  private void buildStreamPage(ModifyListener lsMod, String[] fieldNames) {
    Label wlStreamFields;
    wStreamPage = new Composite(wModeStack, SWT.NONE);
    PropsUi.setLook(wStreamPage);
    wStreamPage.setLayout(new FormLayout());

    wlStreamFields = new Label(wStreamPage, SWT.LEFT);
    wlStreamFields.setText(BaseMessages.getString(PKG, "RedisOutputDialog.Fields.Label"));
    PropsUi.setLook(wlStreamFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(0, 0);
    wlStreamFields.setLayoutData(fdlFields);

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "RedisOutputDialog.Column.StreamField"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              fieldNames,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RedisOutputDialog.Column.DataStructure"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              RedisDataStructure.getNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RedisOutputDialog.Column.Key"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              fieldNames,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RedisOutputDialog.Column.KeyCodec"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              RedisCodecType.getNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RedisOutputDialog.Column.HashKey"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              fieldNames,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RedisOutputDialog.Column.HashKeyCodec"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              RedisCodecType.getNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RedisOutputDialog.Column.ValueCodec"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              RedisCodecType.getNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RedisOutputDialog.Column.TtlSeconds"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false)
        };
    columns[7].setUsingVariables(true);

    int fieldRows =
        input.getFields() == null || input.getFields().isEmpty() ? 5 : input.getFields().size();
    wFields =
        new TableView(
            variables,
            wStreamPage,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            fieldRows,
            lsMod,
            props);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.top = new FormAttachment(wlStreamFields, margin);
    fdFields.bottom = new FormAttachment(100, 0);
    wFields.setLayoutData(fdFields);
  }

  private Control addLabeledCombo(
      Composite parent,
      Control last,
      String label,
      CCombo combo,
      String[] items,
      ModifyListener lsMod,
      Label existingLabel) {
    Label wl = existingLabel != null ? existingLabel : new Label(parent, SWT.RIGHT);
    wl.setText(label);
    PropsUi.setLook(wl);
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, -margin);
    if (last == null) {
      fdl.top = new FormAttachment(0, 0);
    } else {
      fdl.top = new FormAttachment(last, margin);
    }
    wl.setLayoutData(fdl);
    combo.setItems(items);
    PropsUi.setLook(combo);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(wl, 0, SWT.CENTER);
    combo.setLayoutData(fd);
    combo.addModifyListener(lsMod);
    return combo;
  }

  private void updateVisibility() {
    RedisOutputWriteMode writeMode = RedisOutputWriteMode.fromCode(wWriteMode.getText());
    boolean streamFields = writeMode == RedisOutputWriteMode.STREAM_FIELDS;

    if (streamFields) {
      modeStackLayout.topControl = wStreamPage;
    } else {
      modeStackLayout.topControl = wKeyValuePage;
      updateKeyValueStructureVisibility();
    }
    wModeStack.layout(true, true);

    if (wGet != null) {
      wGet.setEnabled(streamFields);
    }
  }

  /**
   * KEY_VALUE 2-column layout by data structure:
   *
   * <pre>
   * Data structure
   * Key field          | Key codec
   * Hash key field     | Hash key codec   (HASH only)
   * Value field        | Value codec
   * TTL seconds        | List push        (list push LIST only)
   * </pre>
   */
  private void updateKeyValueStructureVisibility() {
    RedisDataStructure structure = RedisDataStructure.fromCode(wDataStructure.getText());
    boolean hash = structure == RedisDataStructure.HASH;
    boolean list = structure == RedisDataStructure.LIST;

    wHashKeyRow.setVisible(hash);
    FormData fdHash = (FormData) wHashKeyRow.getLayoutData();
    if (hash) {
      fdHash.top = new FormAttachment(wKeyRow, margin);
      fdHash.height = SWT.DEFAULT;
    } else {
      fdHash.top = new FormAttachment(wKeyRow, 0);
      fdHash.height = 0;
    }
    wHashKeyRow.setLayoutData(fdHash);

    FormData fdValue = (FormData) wValueRow.getLayoutData();
    fdValue.top = new FormAttachment(hash ? wHashKeyRow : wKeyRow, margin);
    wValueRow.setLayoutData(fdValue);

    wlListPushDirection.setVisible(list);
    wListPushDirection.setVisible(list);
    // TTL always half-width, same as field columns

    wKeyValuePage.layout(true, true);
  }

  private void getFields() {
    try {
      IRowMeta rowMeta = pipelineMeta.getPrevTransformFields(variables, transformName);
      BaseTransformDialog.getFieldsFromPrevious(
          rowMeta,
          wFields,
          1,
          new int[] {1, 3},
          new int[] {},
          -1,
          -1,
          (item, valueMeta) -> {
            item.setText(2, RedisDataStructure.STRING.name());
            item.setText(4, RedisCodecType.STRING.name());
            item.setText(5, "");
            item.setText(6, "");
            item.setText(7, RedisCodecType.STRING.name());
            item.setText(8, "0");
            return true;
          });
      input.setChanged();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting fields", e);
    }
  }

  private void getData() {
    wConnection.setText(Const.NVL(input.getConnectionName(), ""));
    wWriteMode.setText(
        input.getWriteMode() == null
            ? RedisOutputWriteMode.KEY_VALUE.name()
            : input.getWriteMode().name());

    RedisDataStructure structure =
        input.getDataStructure() == null ? RedisDataStructure.STRING : input.getDataStructure();
    wDataStructure.setText(structure.name());

    wKeyCodec.setText(codecName(input.getKeyCodec()));
    wHashKeyCodec.setText(codecName(input.getHashKeyCodec()));

    wKeyField.setText(Const.NVL(input.getKeyField(), ""));
    wHashKeyField.setText(Const.NVL(input.getHashKeyField(), ""));

    // HASH uses hash-value meta in the shared Value field/codec widgets
    if (structure == RedisDataStructure.HASH) {
      wValueField.setText(Const.NVL(input.getHashValueField(), ""));
      wValueCodec.setText(codecName(input.getHashValueCodec()));
    } else {
      wValueField.setText(Const.NVL(input.getValueField(), ""));
      wValueCodec.setText(codecName(input.getValueCodec()));
    }

    wTtlSeconds.setText(Const.NVL(input.getTtlSeconds(), ""));
    wListPushDirection.setText(
        input.getListPushDirection() == null
            ? RedisListPushDirection.RPUSH.name()
            : input.getListPushDirection().name());

    wFields.clearAll(false);
    if (input.getFields() != null) {
      for (RedisOutputField field : input.getFields()) {
        TableItem item = new TableItem(wFields.table, SWT.NONE);
        item.setText(1, Const.NVL(field.getStreamField(), ""));
        item.setText(2, field.resolveDataStructure().name());
        item.setText(3, Const.NVL(field.resolveKey(), ""));
        item.setText(4, codecName(field.getKeyCodec()));
        item.setText(5, Const.NVL(field.getHashKey(), ""));
        item.setText(6, optionalCodecName(field.getHashKeyCodec()));
        item.setText(7, codecName(field.getValueCodec()));
        item.setText(8, Const.NVL(field.getTtlSeconds(), "0"));
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
    input.setWriteMode(RedisOutputWriteMode.fromCode(wWriteMode.getText()));

    RedisOutputWriteMode writeMode = RedisOutputWriteMode.fromCode(wWriteMode.getText());
    // Always keep KEY_VALUE field slots; STREAM_FIELDS uses per-row TTL (not component TTL)
    saveKeyValueMappings();
    if (writeMode != RedisOutputWriteMode.STREAM_FIELDS) {
      input.setTtlSeconds(wTtlSeconds.getText());
    }

    List<RedisOutputField> fields = new ArrayList<>();
    for (TableItem item : wFields.getNonEmptyItems()) {
      RedisOutputField field = new RedisOutputField();
      field.setStreamField(item.getText(1));
      field.setDataStructure(RedisDataStructure.fromCode(item.getText(2)));
      field.setKey(item.getText(3));
      field.setKeyCodec(RedisCodecType.fromCode(item.getText(4)));
      field.setHashKey(item.getText(5));
      String hashKeyCodec = item.getText(6);
      field.setHashKeyCodec(
          StringUtils.isEmpty(hashKeyCodec) ? null : RedisCodecType.fromCode(hashKeyCodec));
      field.setValueCodec(RedisCodecType.fromCode(item.getText(7)));
      String ttl = item.getText(8);
      field.setTtlSeconds(StringUtils.isEmpty(ttl) ? "0" : ttl);
      fields.add(field);
    }
    input.setFields(fields);
    input.setChanged();
    dispose();
  }

  private void saveKeyValueMappings() {
    RedisDataStructure structure = RedisDataStructure.fromCode(wDataStructure.getText());
    input.setDataStructure(structure);
    input.setKeyCodec(RedisCodecType.fromCode(wKeyCodec.getText()));
    input.setKeyField(wKeyField.getText());
    input.setListPushDirection(RedisListPushDirection.fromCode(wListPushDirection.getText()));
    input.setHashKeyField(wHashKeyField.getText());
    input.setHashKeyCodec(RedisCodecType.fromCode(wHashKeyCodec.getText()));

    if (structure == RedisDataStructure.HASH) {
      input.setHashValueField(wValueField.getText());
      input.setHashValueCodec(RedisCodecType.fromCode(wValueCodec.getText()));
    } else {
      input.setValueField(wValueField.getText());
      input.setValueCodec(RedisCodecType.fromCode(wValueCodec.getText()));
    }
  }
}
