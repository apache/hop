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

package org.apache.hop.beam.transforms.snowflake;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class BeamSnowflakeInputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BeamSnowflakeInputMeta.class;
  private final BeamSnowflakeInputMeta input;
  private GuiCompositeWidgets widgets;
  private TableView wFields;

  public BeamSnowflakeInputDialog(
      Shell parent, IVariables variables, BeamSnowflakeInputMeta meta, PipelineMeta pipelineMeta) {
    super(parent, variables, meta, pipelineMeta);
    input = meta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "BeamSnowflakeInputDialog.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();
    changed = input.hasChanged();
    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wTransformName,
            wOk,
            BeamSnowflakeInputMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            input,
            w -> {
              widgets = w;
              w.registerExtraGroup(
                  BaseMessages.getString(PKG, "BeamSnowflakeInput.Fields.Group"),
                  "0400",
                  null,
                  this::addFieldsTable);
            });
    widgets.setCompositeWidgetsListener(
        new IGuiPluginCompositeWidgetsListener() {
          @Override
          public void widgetsCreated(GuiCompositeWidgets w) {}

          @Override
          public void widgetsPopulated(GuiCompositeWidgets w) {}

          @Override
          public void widgetModified(GuiCompositeWidgets w, Control control, String id) {
            input.setChanged();
          }

          @Override
          public void persistContents(GuiCompositeWidgets w) {
            input.setFields(readFields());
          }
        });
    populateFields();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void addFieldsTable(Composite parent) {
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "BeamSnowflakeInput.Fields.Column.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "BeamSnowflakeInput.Fields.Column.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                "String", "Integer", "Number", "BigNumber", "Boolean", "Date", "Timestamp", "Binary"
              },
              true)
        };
    int rows = input.getFields() == null ? 0 : input.getFields().size();
    wFields =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            rows,
            e -> input.setChanged(),
            props);
    FormData formData = new FormData();
    formData.left = new FormAttachment(0, 0);
    formData.right = new FormAttachment(100, 0);
    formData.top = new FormAttachment(0, 0);
    formData.bottom = new FormAttachment(100, 0);
    wFields.setLayoutData(formData);
  }

  private void populateFields() {
    if (wFields == null || input.getFields() == null) return;
    for (SnowflakeField field : input.getFields()) {
      TableItem item = new TableItem(wFields.table, SWT.NONE);
      item.setText(1, Const.NVL(field.getName(), ""));
      item.setText(2, Const.NVL(field.getType(), ""));
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
  }

  private List<SnowflakeField> readFields() {
    List<SnowflakeField> fields = new ArrayList<>();
    if (wFields == null || wFields.isDisposed()) return fields;
    for (TableItem item : wFields.getNonEmptyItems()) {
      if (!Utils.isEmpty(item.getText(1)))
        fields.add(new SnowflakeField(item.getText(1), item.getText(2)));
    }
    return fields;
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) return;
    widgets.getWidgetsContents(input, BeamSnowflakeInputMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    input.setFields(readFields());
    transformName = wTransformName.getText();
    input.setChanged();
    dispose();
  }
}
