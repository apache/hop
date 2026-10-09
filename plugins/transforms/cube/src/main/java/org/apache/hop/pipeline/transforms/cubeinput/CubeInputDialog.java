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

package org.apache.hop.pipeline.transforms.cubeinput;

import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

public class CubeInputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = CubeInputMeta.class;

  private final CubeInputMeta input;
  private GuiCompositeWidgets widgets;

  public CubeInputDialog(
      Shell parent, IVariables variables, CubeInputMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "CubeInputDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = input.hasChanged();

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wTransformName,
            wOk,
            CubeInputMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            input);
    setFilenameFieldChoices();
    widgets.setCompositeWidgetsListener(
        new IGuiPluginCompositeWidgetsListener() {
          @Override
          public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
            // Fields come from the metadata annotations.
          }

          @Override
          public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
            updateFieldMode();
          }

          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            input.setChanged();
            if (CubeInputMeta.WIDGET_FILENAME_IN_FIELD.equals(widgetId)) {
              updateFieldMode();
            }
          }

          @Override
          public void persistContents(GuiCompositeWidgets compositeWidgets) {
            // OK reads the widgets itself.
          }
        });
    updateFieldMode();

    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void setFilenameFieldChoices() {
    String[] fieldNames;
    try {
      fieldNames = pipelineMeta.getPrevTransformFields(variables, transformName).getFieldNames();
    } catch (Exception e) {
      fieldNames = new String[] {};
    }
    widgets.setComboValues(CubeInputMeta.WIDGET_FILENAME_FIELD, fieldNames);
  }

  /**
   * Filenames from a field and the transform-number suffix are alternatives. The static filename
   * stays available: it is the file the layout is read from.
   */
  private void updateFieldMode() {
    Control filenameInField = widgets.getWidgetsMap().get(CubeInputMeta.WIDGET_FILENAME_IN_FIELD);
    Control includeTransformNr =
        widgets.getWidgetsMap().get(CubeInputMeta.WIDGET_INCLUDE_TRANSFORM_NR);
    Control filenameField = widgets.getWidgetsMap().get(CubeInputMeta.WIDGET_FILENAME_FIELD);
    boolean fromField = filenameInField instanceof Button button && button.getSelection();
    if (fromField && includeTransformNr instanceof Button includeButton) {
      includeButton.setSelection(false);
    }
    if (includeTransformNr != null) {
      includeTransformNr.setEnabled(!fromField);
    }
    if (filenameField != null) {
      filenameField.setEnabled(fromField);
    }
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

    widgets.getWidgetsContents(input, CubeInputMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    transformName = wTransformName.getText();
    dispose();
  }
}
