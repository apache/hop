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

package org.apache.hop.pipeline.transforms.ddl;

import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.widgets.Shell;

public class DdlDialog extends BaseTransformDialog {
  private static final Class<?> PKG = DdlDialog.class;

  private final DdlMeta input;
  private GuiCompositeWidgets widgets;

  public DdlDialog(
      Shell parent, IVariables variables, DdlMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "Ddl.Name"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    // Add the widgets from metadata...
    //
    widgets = new GuiCompositeWidgets(variables);
    widgets.createCompositeWidgets(
        input, null, shell, DdlMeta.GUI_PLUGIN_ELEMENT_PARENT_ID, wTransformName);
    widgets.setWidgetsContents(input, shell, DdlMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);

    setFieldNamesOnComboWidgets();

    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Cancel the dialog. */
  private void cancel() {
    transformName = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    widgets.getWidgetsContents(input, DdlMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);

    // return value
    transformName = wTransformName.getText();
    dispose();
  }

  private void setFieldNamesOnComboWidgets() {
    // The input field names:
    String[] fieldNames;
    try {
      fieldNames = pipelineMeta.getPrevTransformFields(variables, transformName).getFieldNames();
    } catch (Exception e) {
      fieldNames = new String[] {};
    }

    // Set the field names on the combo widgets
    widgets.setComboValues(DdlMeta.WIDGET_COLUMN_NAME_FIELD, fieldNames);
    widgets.setComboValues(DdlMeta.WIDGET_COLUMN_TYPE_FIELD, fieldNames);
    widgets.setComboValues(DdlMeta.WIDGET_COLUMN_LENGTH_FIELD, fieldNames);
    widgets.setComboValues(DdlMeta.WIDGET_COLUMN_PRECISION_FIELD, fieldNames);
  }
}
