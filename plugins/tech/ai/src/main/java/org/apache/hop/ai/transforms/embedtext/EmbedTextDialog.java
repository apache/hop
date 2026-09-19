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
package org.apache.hop.ai.transforms.embedtext;

import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

public class EmbedTextDialog extends BaseTransformDialog {

  private static final Class<?> PKG = EmbedTextMeta.class;

  private final EmbedTextMeta input;
  private GuiCompositeWidgets widgets;
  private boolean loading;

  public EmbedTextDialog(
      Shell parent, IVariables variables, EmbedTextMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "EmbedTextDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = input.hasChanged();
    loading = true;

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wTransformName,
            wOk,
            EmbedTextMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            input);
    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            if (!loading) {
              input.setChanged();
            }
          }
        });

    setFieldComboValues();
    loading = false;
    input.setChanged(changed);

    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  /**
   * Stream field names cannot come from {@code comboValuesMethod}, which is handed only a log
   * channel and a metadata provider, so the input field combo is filled once the widgets exist.
   */
  private void setFieldComboValues() {
    try {
      IRowMeta fields = pipelineMeta.getPrevTransformFields(variables, transformName);
      String[] names = fields == null ? new String[0] : fields.getFieldNames();
      setComboItems(EmbedTextMeta.WIDGET_INPUT_FIELD, names);
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting source fields", e);
    }
  }

  /** Fills a combo without losing the selection the transform was saved with. */
  private void setComboItems(String widgetId, String[] names) {
    // Setting items clears the widget's text, so put the saved selection back afterwards.
    String selected = comboText(widgetId);
    widgets.setComboValues(widgetId, names);
    if (!Utils.isEmpty(selected)) {
      setComboText(widgetId, selected);
    }
  }

  /**
   * {@code GuiCompositeWidgets} builds a plain SWT {@link Combo} when the element has no variable
   * support and a {@link ComboVar} when it does, so both have to be handled.
   */
  private String comboText(String widgetId) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control instanceof ComboVar comboVar && !comboVar.isDisposed()) {
      return comboVar.getText();
    }
    if (control instanceof Combo combo && !combo.isDisposed()) {
      return combo.getText();
    }
    return "";
  }

  private void setComboText(String widgetId, String text) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control == null || control.isDisposed()) {
      return;
    }
    if (control instanceof ComboVar comboVar) {
      comboVar.setText(text);
    } else if (control instanceof Combo combo) {
      combo.setText(text);
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
    widgets.getWidgetsContents(input, EmbedTextMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    transformName = wTransformName.getText();
    input.setChanged();
    dispose();
  }
}
