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

package org.apache.hop.spark.transforms.sql;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.spark.transforms.io.SparkField;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class SparkSqlDialog extends BaseTransformDialog {
  private static final Class<?> PKG = SparkSqlMeta.class;

  private final SparkSqlMeta input;

  private Text wSql;
  private TableView wViews;
  private TableView wFields;

  public SparkSqlDialog(
      Shell parent, IVariables variables, SparkSqlMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    this.input = transformMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "SparkSqlDialog.Shell.Title"));

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
    wTransformName.addModifyListener(e -> input.setChanged());
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control last = wTransformName;

    Label wlSql = new Label(shell, SWT.LEFT);
    wlSql.setText(BaseMessages.getString(PKG, "SparkSqlDialog.Sql"));
    PropsUi.setLook(wlSql);
    FormData fdlSql = new FormData();
    fdlSql.left = new FormAttachment(0, 0);
    fdlSql.top = new FormAttachment(last, margin);
    fdlSql.right = new FormAttachment(100, 0);
    wlSql.setLayoutData(fdlSql);

    wSql = new Text(shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(wSql);
    wSql.addModifyListener(e -> input.setChanged());
    wSql.setToolTipText(BaseMessages.getString(PKG, "SparkSqlDialog.Sql.Tooltip"));
    FormData fdSql = new FormData();
    fdSql.left = new FormAttachment(0, 0);
    fdSql.top = new FormAttachment(wlSql, margin);
    fdSql.right = new FormAttachment(100, 0);
    fdSql.height = (int) (150 * props.getZoomFactor());
    wSql.setLayoutData(fdSql);
    last = wSql;

    Label wlViews = new Label(shell, SWT.LEFT);
    wlViews.setText(BaseMessages.getString(PKG, "SparkSqlDialog.Views"));
    PropsUi.setLook(wlViews);
    FormData fdlViews = new FormData();
    fdlViews.left = new FormAttachment(0, 0);
    fdlViews.top = new FormAttachment(last, margin);
    fdlViews.right = new FormAttachment(100, 0);
    wlViews.setLayoutData(fdlViews);

    String[] previousTransforms = pipelineMeta.getPrevTransformNames(transformName);
    ColumnInfo[] viewColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "SparkSqlDialog.Column.Transform"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              previousTransforms == null ? new String[0] : previousTransforms,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SparkSqlDialog.Column.View"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false)
        };
    wViews =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            viewColumns,
            input.getViews() == null ? 1 : Math.max(1, input.getViews().size()),
            e -> input.setChanged(),
            props);
    FormData fdViews = new FormData();
    fdViews.left = new FormAttachment(0, 0);
    fdViews.top = new FormAttachment(wlViews, margin);
    fdViews.right = new FormAttachment(100, 0);
    fdViews.height = (int) (100 * props.getZoomFactor());
    wViews.setLayoutData(fdViews);
    last = wViews;

    Label wlFields = new Label(shell, SWT.LEFT);
    wlFields.setText(BaseMessages.getString(PKG, "SparkSqlDialog.Fields"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(last, margin);
    fdlFields.right = new FormAttachment(100, 0);
    wlFields.setLayoutData(fdlFields);

    ColumnInfo[] fieldColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "SparkSqlDialog.Column.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SparkSqlDialog.Column.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SparkSqlDialog.Column.Length"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SparkSqlDialog.Column.Precision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false)
        };
    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            fieldColumns,
            input.getFields() == null ? 1 : Math.max(1, input.getFields().size()),
            e -> input.setChanged(),
            props);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
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

  private void getData() {
    wTransformName.setText(Const.NVL(transformName, ""));
    wSql.setText(Const.NVL(input.getSql(), ""));

    if (input.getViews() != null) {
      int i = 0;
      for (SparkSqlView view : input.getViews()) {
        TableItem item = wViews.table.getItem(i);
        if (item == null) {
          item = new TableItem(wViews.table, SWT.NONE);
        }
        item.setText(1, Const.NVL(view.getTransformName(), ""));
        item.setText(2, Const.NVL(view.getViewName(), ""));
        i++;
      }
      wViews.setRowNums();
      wViews.optWidth(true);
    }

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
    input.setSql(wSql.getText());

    List<SparkSqlView> views = new ArrayList<>();
    for (int i = 0; i < wViews.nrNonEmpty(); i++) {
      TableItem item = wViews.getNonEmpty(i);
      views.add(new SparkSqlView(item.getText(1), item.getText(2)));
    }
    input.setViews(views);

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
