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

package org.apache.hop.spark.transforms.table;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.spark.table.SparkLakeFormats;
import org.apache.hop.spark.transforms.io.SparkField;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class SparkLakeTableInputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = SparkLakeTableInputMeta.class;

  private final SparkLakeTableInputMeta input;

  private CCombo wFormat;
  private CCombo wIdentifierMode;
  private TextVar wTablePath;
  private TextVar wTableIdentifier;
  private TextVar wCatalogMetadataName;
  private CCombo wTimeTravelType;
  private TextVar wTimeTravelVersion;
  private TextVar wTimeTravelTimestamp;
  private Text wExtraOptions;
  private TableView wFields;

  public SparkLakeTableInputDialog(
      Shell parent,
      IVariables variables,
      SparkLakeTableInputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    this.input = transformMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "SparkLakeTableInputDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control last = wTransformName;

    last = labeledCombo(lsMod, middle, margin, last, "SparkLakeTableInputDialog.Format", true);
    wFormat = (CCombo) last;
    wFormat.setItems(new String[] {SparkLakeFormats.FORMAT_DELTA, SparkLakeFormats.FORMAT_ICEBERG});

    last =
        labeledCombo(lsMod, middle, margin, last, "SparkLakeTableInputDialog.IdentifierMode", true);
    wIdentifierMode = (CCombo) last;
    wIdentifierMode.setItems(
        new String[] {SparkLakeTableInputMeta.MODE_PATH, SparkLakeTableInputMeta.MODE_TABLE});

    last = labeledTextVar(lsMod, middle, margin, last, "SparkLakeTableInputDialog.TablePath");
    wTablePath = (TextVar) last;

    last = labeledTextVar(lsMod, middle, margin, last, "SparkLakeTableInputDialog.TableIdentifier");
    wTableIdentifier = (TextVar) last;

    last =
        labeledTextVar(
            lsMod, middle, margin, last, "SparkLakeTableInputDialog.CatalogMetadataName");
    wCatalogMetadataName = (TextVar) last;

    last =
        labeledCombo(lsMod, middle, margin, last, "SparkLakeTableInputDialog.TimeTravelType", true);
    wTimeTravelType = (CCombo) last;
    wTimeTravelType.setItems(
        new String[] {
          SparkLakeTableInputMeta.TIME_TRAVEL_NONE,
          SparkLakeTableInputMeta.TIME_TRAVEL_VERSION,
          SparkLakeTableInputMeta.TIME_TRAVEL_TIMESTAMP
        });

    last =
        labeledTextVar(lsMod, middle, margin, last, "SparkLakeTableInputDialog.TimeTravelVersion");
    wTimeTravelVersion = (TextVar) last;

    last =
        labeledTextVar(
            lsMod, middle, margin, last, "SparkLakeTableInputDialog.TimeTravelTimestamp");
    wTimeTravelTimestamp = (TextVar) last;

    Label wlExtra = new Label(shell, SWT.RIGHT);
    wlExtra.setText(BaseMessages.getString(PKG, "SparkLakeTableInputDialog.ExtraOptions"));
    PropsUi.setLook(wlExtra);
    FormData fdlExtra = new FormData();
    fdlExtra.left = new FormAttachment(0, 0);
    fdlExtra.top = new FormAttachment(last, margin);
    fdlExtra.right = new FormAttachment(middle, -margin);
    wlExtra.setLayoutData(fdlExtra);
    wExtraOptions = new Text(shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL);
    PropsUi.setLook(wExtraOptions);
    wExtraOptions.addModifyListener(lsMod);
    FormData fdExtra = new FormData();
    fdExtra.left = new FormAttachment(middle, 0);
    fdExtra.top = new FormAttachment(wlExtra, 0, SWT.TOP);
    fdExtra.right = new FormAttachment(100, 0);
    fdExtra.height = 50;
    wExtraOptions.setLayoutData(fdExtra);
    last = wExtraOptions;

    Label wlFields = new Label(shell, SWT.RIGHT);
    wlFields.setText(BaseMessages.getString(PKG, "SparkLakeTableInputDialog.Fields"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(last, margin);
    fdlFields.right = new FormAttachment(middle, -margin);
    wlFields.setLayoutData(fdlFields);

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "SparkLakeTableInputDialog.Column.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SparkLakeTableInputDialog.Column.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SparkLakeTableInputDialog.Column.Length"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SparkLakeTableInputDialog.Column.Precision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false)
        };
    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            input.getFields() == null ? 1 : Math.max(1, input.getFields().size()),
            lsMod,
            props);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(middle, 0);
    fdFields.top = new FormAttachment(last, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(100, -50);
    wFields.setLayoutData(fdFields);

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());

    getData();
    input.setChanged(changed);
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private Control labeledCombo(
      ModifyListener lsMod,
      int middle,
      int margin,
      Control last,
      String labelKey,
      boolean readOnly) {
    Label wl = new Label(shell, SWT.RIGHT);
    wl.setText(BaseMessages.getString(PKG, labelKey));
    PropsUi.setLook(wl);
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.top = new FormAttachment(last, margin);
    fdl.right = new FormAttachment(middle, -margin);
    wl.setLayoutData(fdl);
    CCombo combo = new CCombo(shell, SWT.BORDER | (readOnly ? SWT.READ_ONLY : SWT.NONE));
    PropsUi.setLook(combo);
    combo.addModifyListener(lsMod);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(wl, 0, SWT.CENTER);
    fd.right = new FormAttachment(100, 0);
    combo.setLayoutData(fd);
    return combo;
  }

  private TextVar labeledTextVar(
      ModifyListener lsMod, int middle, int margin, Control last, String labelKey) {
    Label wl = new Label(shell, SWT.RIGHT);
    wl.setText(BaseMessages.getString(PKG, labelKey));
    PropsUi.setLook(wl);
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.top = new FormAttachment(last, margin);
    fdl.right = new FormAttachment(middle, -margin);
    wl.setLayoutData(fdl);
    TextVar text = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(text);
    text.addModifyListener(lsMod);
    String tooltip = BaseMessages.getString(PKG, labelKey + ".Tooltip");
    if (!Utils.isEmpty(tooltip) && !tooltip.startsWith("!")) {
      text.setToolTipText(tooltip);
      wl.setToolTipText(tooltip);
    }
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(wl, 0, SWT.CENTER);
    fd.right = new FormAttachment(100, 0);
    text.setLayoutData(fd);
    return text;
  }

  private void getData() {
    wTransformName.setText(Const.NVL(transformName, ""));
    wFormat.setText(Const.NVL(input.getFormat(), SparkLakeFormats.FORMAT_DELTA));
    wIdentifierMode.setText(
        Const.NVL(input.getIdentifierMode(), SparkLakeTableInputMeta.MODE_PATH));
    wTablePath.setText(Const.NVL(input.getTablePath(), ""));
    wTableIdentifier.setText(Const.NVL(input.getTableIdentifier(), ""));
    wCatalogMetadataName.setText(Const.NVL(input.getCatalogMetadataName(), ""));
    wTimeTravelType.setText(
        Const.NVL(input.getTimeTravelType(), SparkLakeTableInputMeta.TIME_TRAVEL_NONE));
    wTimeTravelVersion.setText(Const.NVL(input.getTimeTravelVersion(), ""));
    wTimeTravelTimestamp.setText(Const.NVL(input.getTimeTravelTimestamp(), ""));
    wExtraOptions.setText(Const.NVL(input.getExtraOptions(), ""));
    if (input.getFields() != null) {
      int i = 0;
      for (SparkField f : input.getFields()) {
        TableItem item = wFields.table.getItem(i);
        if (item == null) {
          item = new TableItem(wFields.table, SWT.NONE);
        }
        item.setText(1, Const.NVL(f.getName(), ""));
        item.setText(2, Const.NVL(f.getHopType(), "String"));
        item.setText(3, f.getLength() >= 0 ? Integer.toString(f.getLength()) : "");
        item.setText(4, f.getPrecision() >= 0 ? Integer.toString(f.getPrecision()) : "");
        i++;
      }
      wFields.setRowNums();
      wFields.optWidth(true);
    }
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
    transformName = wTransformName.getText();
    input.setFormat(wFormat.getText());
    input.setIdentifierMode(wIdentifierMode.getText());
    input.setTablePath(wTablePath.getText());
    input.setTableIdentifier(wTableIdentifier.getText());
    input.setCatalogMetadataName(wCatalogMetadataName.getText());
    input.setTimeTravelType(wTimeTravelType.getText());
    input.setTimeTravelVersion(wTimeTravelVersion.getText());
    input.setTimeTravelTimestamp(wTimeTravelTimestamp.getText());
    input.setExtraOptions(wExtraOptions.getText());
    List<SparkField> fields = new ArrayList<>();
    for (int i = 0; i < wFields.nrNonEmpty(); i++) {
      TableItem item = wFields.getNonEmpty(i);
      SparkField f = new SparkField();
      f.setName(item.getText(1));
      f.setHopType(item.getText(2));
      f.setLength(Const.toInt(item.getText(3), -1));
      f.setPrecision(Const.toInt(item.getText(4), -1));
      fields.add(f);
    }
    input.setFields(fields);
    dispose();
  }
}
