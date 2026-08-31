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

package org.apache.hop.pipeline.transforms.transformsmetrics;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;

public class TransformsMetricsDialog extends BaseTransformDialog {
  private static final Class<?> PKG = TransformsMetricsMeta.class;

  private final TransformsMetricsMeta input;
  private GuiCompositeWidgets widgets;
  private TableView wFields;
  private String[] previousTransforms;

  public TransformsMetricsDialog(
      Shell parent,
      IVariables variables,
      TransformsMetricsMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "TransformsMetricsDialog.Shell.Title"));

    changed = input.hasChanged();

    buildButtonBar().ok(e -> ok()).get(e -> get()).cancel(e -> cancel()).build();

    setTransformNames();

    Composite area = new Composite(shell, SWT.NONE);
    PropsUi.setLook(area);
    area.setLayout(new FormLayout());
    FormData fdArea = new FormData();
    fdArea.left = new FormAttachment(0, 0);
    fdArea.top = new FormAttachment(wSpacer, margin);
    fdArea.right = new FormAttachment(100, 0);
    fdArea.bottom = new FormAttachment(wOk, -2 * margin);
    area.setLayoutData(fdArea);

    widgets = new GuiCompositeWidgets(variables);
    widgets.registerExtraGroup(
        BaseMessages.getString(PKG, "TransformsMetricsMeta.Group.Transforms"),
        "10",
        null,
        this::addTransformsTable);
    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            input.setChanged();
          }
        });
    widgets.createCompositeWidgets(
        input, null, area, TransformsMetricsMeta.GUI_PLUGIN_ELEMENT_PARENT_ID, null);

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void addTransformsTable(Composite parent) {
    Label wlFields = new Label(parent, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "TransformsMetricsDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(0, 0);
    wlFields.setLayoutData(fdlFields);

    String[] yesNo =
        new String[] {
          BaseMessages.getString(PKG, "System.Combo.No"),
          BaseMessages.getString(PKG, "System.Combo.Yes")
        };

    ColumnInfo[] colinf = new ColumnInfo[3];
    colinf[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "TransformsMetricsDialog.Fieldname.Transform"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            previousTransforms,
            false);
    colinf[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "TransformsMetricsDialog.Fieldname.CopyNr"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[1].setUsingVariables(true);
    colinf[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "TransformsMetricsDialog.Fieldname.Required"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            yesNo);

    wFields =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            input.getMetricTransforms() == null ? 0 : input.getMetricTransforms().size(),
            e -> input.setChanged(),
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(100, 0);
    wFields.setLayoutData(fdFields);
  }

  private void setTransformNames() {
    previousTransforms = pipelineMeta.getTransformNames();
    List<String> nextTransforms = getNextTransforms(new ArrayList<>(), transformMeta);

    List<String> entries = new ArrayList<>();
    for (String previousTransform : previousTransforms) {
      if (!previousTransform.equals(transformName) && !nextTransforms.contains(previousTransform)) {
        entries.add(previousTransform);
      }
    }
    previousTransforms = entries.toArray(new String[0]);
  }

  private List<String> getNextTransforms(List<String> transformNames, TransformMeta transformMeta) {
    List<TransformMeta> nextTransformMeta = pipelineMeta.findNextTransforms(transformMeta);
    for (TransformMeta nextTransform : nextTransformMeta) {
      transformNames.add(nextTransform.getName());
      getNextTransforms(transformNames, nextTransform);
    }
    return transformNames.stream().distinct().toList();
  }

  private void get() {
    wFields.removeAll();
    Table table = wFields.table;
    for (int i = 0; i < previousTransforms.length; i++) {
      TableItem ti = new TableItem(table, SWT.NONE);
      ti.setText(0, "" + (i + 1));
      ti.setText(1, previousTransforms[i]);
      ti.setText(2, "0");
      ti.setText(3, BaseMessages.getString(PKG, "System.Combo.No"));
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);
    input.setChanged();
  }

  private void getData() {
    widgets.setWidgetsContents(input, shell, TransformsMetricsMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);

    Table table = wFields.table;
    if (input.getMetricTransforms() != null && !input.getMetricTransforms().isEmpty()) {
      table.removeAll();
    }
    if (input.getMetricTransforms() != null) {
      for (MetricTransform metricTransform : input.getMetricTransforms()) {
        TableItem ti = new TableItem(table, SWT.NONE);
        ti.setText(1, Const.NVL(metricTransform.getName(), ""));
        ti.setText(2, Const.NVL(metricTransform.getCopyNr(), ""));
        ti.setText(
            3,
            metricTransform.isRequired()
                ? BaseMessages.getString(PKG, "System.Combo.Yes")
                : BaseMessages.getString(PKG, "System.Combo.No"));
      }
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);
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

    widgets.getWidgetsContents(input, TransformsMetricsMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    transformName = wTransformName.getText();

    if (input.getMetricTransforms() == null) {
      input.setMetricTransforms(new ArrayList<>());
    } else {
      input.getMetricTransforms().clear();
    }
    String yes = BaseMessages.getString(PKG, "System.Combo.Yes");
    for (TableItem item : wFields.getNonEmptyItems()) {
      String name = item.getText(1);
      if (Utils.isEmpty(name)) {
        continue;
      }
      input
          .getMetricTransforms()
          .add(new MetricTransform(name, item.getText(2), yes.equalsIgnoreCase(item.getText(3))));
    }
    input.setChanged();
    dispose();
  }
}
