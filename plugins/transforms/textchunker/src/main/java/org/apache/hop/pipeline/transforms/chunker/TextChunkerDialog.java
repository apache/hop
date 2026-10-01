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
package org.apache.hop.pipeline.transforms.chunker;

import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.chunker.chunking.ChunkingStrategyType;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

public class TextChunkerDialog extends BaseTransformDialog {

  private static final Class<?> PKG = TextChunkerMeta.class;

  private final TextChunkerMeta input;
  private GuiCompositeWidgets widgets;
  private boolean loading;

  public TextChunkerDialog(
      Shell parent,
      IVariables variables,
      TextChunkerMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "TextChunkerDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = input.hasChanged();
    loading = true;

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wTransformName,
            wOk,
            TextChunkerMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            input);
    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            if (!loading) {
              input.setChanged();
            }
            if (TextChunkerMeta.WIDGET_CHUNKING_STRATEGY.equals(widgetId)
                || TextChunkerMeta.WIDGET_INCLUDE_METADATA.equals(widgetId)) {
              enableFields();
            }
          }
        });

    setFieldComboValues();
    enableFields();
    loading = false;
    input.setChanged(changed);

    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  /**
   * Stream field names cannot come from {@code comboValuesMethod}, which is handed only a log
   * channel and a metadata provider, so the three field combos are filled once the widgets exist.
   */
  private void setFieldComboValues() {
    try {
      IRowMeta fields = pipelineMeta.getPrevTransformFields(variables, transformName);
      String[] names = fields == null ? new String[0] : fields.getFieldNames();
      setComboItems(TextChunkerMeta.WIDGET_INPUT_FIELD, names);
      setComboItems(TextChunkerMeta.WIDGET_SOURCE_DOCUMENT_ID_FIELD, names);
      setComboItems(TextChunkerMeta.WIDGET_CONTENT_TYPE_FIELD, names);
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting source fields", e);
    }
  }

  /** Fills a combo without losing the selection the transform was saved with. */
  private void setComboItems(String widgetId, String[] names) {
    // Setting items clears the widget's text, so put the saved selection back afterwards. This is
    // the same dance GitInputDialog does around setComboValues.
    String selected = comboText(widgetId, "");
    widgets.setComboValues(widgetId, names);
    if (!Utils.isEmpty(selected)) {
      setComboText(widgetId, selected);
    }
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

  private void enableFields() {
    // fromString accepts the constant name and the old display text, so this follows the widget
    // whatever it holds rather than assuming one spelling.
    boolean structure =
        ChunkingStrategyType.fromString(
                comboText(
                    TextChunkerMeta.WIDGET_CHUNKING_STRATEGY, input.getChunkingStrategy().name()))
            == ChunkingStrategyType.STRUCTURE;
    setEnabled(TextChunkerMeta.WIDGET_CONTENT_TYPE, structure);
    setEnabled(TextChunkerMeta.WIDGET_CONTENT_TYPE_FIELD, structure);

    boolean metadata =
        isChecked(TextChunkerMeta.WIDGET_INCLUDE_METADATA, input.isIncludeMetadata());
    setEnabled(TextChunkerMeta.WIDGET_CHUNK_INDEX_FIELD, metadata);
    setEnabled(TextChunkerMeta.WIDGET_CHUNK_START_POS_FIELD, metadata);
    setEnabled(TextChunkerMeta.WIDGET_DOCUMENT_ID_FIELD, metadata);
    setEnabled(TextChunkerMeta.WIDGET_CHUNK_COUNT_FIELD, metadata);
  }

  /**
   * Reads a generated combo. {@code GuiCompositeWidgets} builds a plain SWT {@link Combo} when the
   * element has no variable support and a {@link ComboVar} when it does, so both have to be handled
   * or this silently returns the fallback.
   */
  private String comboText(String widgetId, String fallback) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control instanceof ComboVar comboVar && !comboVar.isDisposed()) {
      return comboVar.getText();
    }
    if (control instanceof Combo combo && !combo.isDisposed()) {
      return combo.getText();
    }
    return fallback;
  }

  private boolean isChecked(String widgetId, boolean fallback) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control instanceof Button button && !button.isDisposed()) {
      return button.getSelection();
    }
    return fallback;
  }

  private void setEnabled(String widgetId, boolean enabled) {
    Control label = widgets.getLabelsMap().get(widgetId);
    if (label != null && !label.isDisposed()) {
      label.setEnabled(enabled);
    }
    Control widget = widgets.getWidgetsMap().get(widgetId);
    if (widget != null && !widget.isDisposed()) {
      widget.setEnabled(enabled);
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
    widgets.getWidgetsContents(input, TextChunkerMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    transformName = wTransformName.getText();
    input.setChanged();
    dispose();
  }
}
