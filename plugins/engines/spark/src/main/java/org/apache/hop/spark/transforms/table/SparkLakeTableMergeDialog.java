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

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.spark.table.SparkLakeFormats;
import org.apache.hop.spark.table.SparkMergeSqlBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
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
import org.eclipse.swt.widgets.Text;

public class SparkLakeTableMergeDialog extends BaseTransformDialog {
  private static final Class<?> PKG = SparkLakeTableMergeMeta.class;

  private final SparkLakeTableMergeMeta input;

  private CCombo wFormat;
  private CCombo wIdentifierMode;
  private TextVar wTablePath;
  private TextVar wTableIdentifier;
  private TextVar wCatalogMetadataName;
  private TextVar wMergeCondition;
  private CCombo wMatchedAction;
  private CCombo wNotMatchedAction;
  private CCombo wNotMatchedBySourceAction;
  private Text wRawMergeSql;

  public SparkLakeTableMergeDialog(
      Shell parent,
      IVariables variables,
      SparkLakeTableMergeMeta transformMeta,
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
    shell.setText(BaseMessages.getString(PKG, "SparkLakeTableMergeDialog.Shell.Title"));

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

    last = labeledCombo(lsMod, middle, margin, last, "SparkLakeTableMergeDialog.Format");
    wFormat = (CCombo) last;
    wFormat.setItems(new String[] {SparkLakeFormats.FORMAT_DELTA, SparkLakeFormats.FORMAT_ICEBERG});

    last = labeledCombo(lsMod, middle, margin, last, "SparkLakeTableMergeDialog.IdentifierMode");
    wIdentifierMode = (CCombo) last;
    wIdentifierMode.setItems(
        new String[] {SparkLakeTableInputMeta.MODE_PATH, SparkLakeTableInputMeta.MODE_TABLE});

    last = labeledTextVar(lsMod, middle, margin, last, "SparkLakeTableMergeDialog.TablePath");
    wTablePath = (TextVar) last;

    last = labeledTextVar(lsMod, middle, margin, last, "SparkLakeTableMergeDialog.TableIdentifier");
    wTableIdentifier = (TextVar) last;

    last =
        labeledTextVar(
            lsMod, middle, margin, last, "SparkLakeTableMergeDialog.CatalogMetadataName");
    wCatalogMetadataName = (TextVar) last;

    last = labeledTextVar(lsMod, middle, margin, last, "SparkLakeTableMergeDialog.MergeCondition");
    wMergeCondition = (TextVar) last;

    last = labeledCombo(lsMod, middle, margin, last, "SparkLakeTableMergeDialog.MatchedAction");
    wMatchedAction = (CCombo) last;
    wMatchedAction.setItems(
        new String[] {
          SparkMergeSqlBuilder.MATCHED_UPDATE_ALL,
          SparkMergeSqlBuilder.MATCHED_DELETE,
          SparkMergeSqlBuilder.MATCHED_NONE
        });

    last = labeledCombo(lsMod, middle, margin, last, "SparkLakeTableMergeDialog.NotMatchedAction");
    wNotMatchedAction = (CCombo) last;
    wNotMatchedAction.setItems(
        new String[] {
          SparkMergeSqlBuilder.NOT_MATCHED_INSERT_ALL, SparkMergeSqlBuilder.NOT_MATCHED_NONE
        });

    last =
        labeledCombo(
            lsMod, middle, margin, last, "SparkLakeTableMergeDialog.NotMatchedBySourceAction");
    wNotMatchedBySourceAction = (CCombo) last;
    wNotMatchedBySourceAction.setItems(
        new String[] {
          SparkMergeSqlBuilder.NOT_MATCHED_BY_SOURCE_NONE,
          SparkMergeSqlBuilder.NOT_MATCHED_BY_SOURCE_DELETE
        });

    Label wlRaw = new Label(shell, SWT.RIGHT);
    wlRaw.setText(BaseMessages.getString(PKG, "SparkLakeTableMergeDialog.RawMergeSql"));
    PropsUi.setLook(wlRaw);
    FormData fdlRaw = new FormData();
    fdlRaw.left = new FormAttachment(0, 0);
    fdlRaw.top = new FormAttachment(last, margin);
    fdlRaw.right = new FormAttachment(middle, -margin);
    wlRaw.setLayoutData(fdlRaw);
    wRawMergeSql = new Text(shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL);
    PropsUi.setLook(wRawMergeSql);
    wRawMergeSql.addModifyListener(lsMod);
    FormData fdRaw = new FormData();
    fdRaw.left = new FormAttachment(middle, 0);
    fdRaw.top = new FormAttachment(wlRaw, 0, SWT.TOP);
    fdRaw.right = new FormAttachment(100, 0);
    fdRaw.height = 80;
    wRawMergeSql.setLayoutData(fdRaw);

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    setButtonPositions(new Button[] {wOk, wCancel}, margin, wRawMergeSql);

    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());

    getData();
    input.setChanged(changed);
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private Control labeledCombo(
      ModifyListener lsMod, int middle, int margin, Control last, String labelKey) {
    Label wl = new Label(shell, SWT.RIGHT);
    wl.setText(BaseMessages.getString(PKG, labelKey));
    PropsUi.setLook(wl);
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.top = new FormAttachment(last, margin);
    fdl.right = new FormAttachment(middle, -margin);
    wl.setLayoutData(fdl);
    CCombo combo = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
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
    wMergeCondition.setText(Const.NVL(input.getMergeCondition(), "t.id = s.id"));
    wMatchedAction.setText(
        Const.NVL(input.getMatchedAction(), SparkMergeSqlBuilder.MATCHED_UPDATE_ALL));
    wNotMatchedAction.setText(
        Const.NVL(input.getNotMatchedAction(), SparkMergeSqlBuilder.NOT_MATCHED_INSERT_ALL));
    wNotMatchedBySourceAction.setText(
        Const.NVL(
            input.getNotMatchedBySourceAction(), SparkMergeSqlBuilder.NOT_MATCHED_BY_SOURCE_NONE));
    wRawMergeSql.setText(Const.NVL(input.getRawMergeSql(), ""));
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
    input.setMergeCondition(wMergeCondition.getText());
    input.setMatchedAction(wMatchedAction.getText());
    input.setNotMatchedAction(wNotMatchedAction.getText());
    input.setNotMatchedBySourceAction(wNotMatchedBySourceAction.getText());
    input.setRawMergeSql(wRawMergeSql.getText());
    dispose();
  }
}
