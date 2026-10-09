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

package org.apache.hop.neo4j.transforms.vectorsearch;

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

public class GraphVectorSearchDialog extends BaseTransformDialog {

  private static final Class<?> PKG = GraphVectorSearchMeta.class;

  private final GraphVectorSearchMeta input;
  private GuiCompositeWidgets widgets;
  private boolean loading;

  public GraphVectorSearchDialog(
      Shell parent,
      IVariables variables,
      GraphVectorSearchMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "GraphVectorSearchDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = input.hasChanged();
    loading = true;
    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wTransformName,
            wOk,
            GraphVectorSearchMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
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
    setEmbeddingFieldItems();
    loading = false;
    input.setChanged(changed);

    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  /**
   * The input fields are not known to the annotations, which get only a log channel and a metadata
   * provider: fill them once the widgets exist.
   */
  private void setEmbeddingFieldItems() {
    try {
      IRowMeta fields = pipelineMeta.getPrevTransformFields(variables, transformName);
      String[] names = fields == null ? new String[0] : fields.getFieldNames();
      Control control = widgets.getWidgetsMap().get(GraphVectorSearchMeta.WIDGET_EMBEDDING_FIELD);
      if (control instanceof ComboVar comboVar && !comboVar.isDisposed()) {
        String selected = comboVar.getText();
        comboVar.setItems(names);
        comboVar.setText(Utils.isEmpty(selected) ? "" : selected);
      } else if (control instanceof Combo combo && !combo.isDisposed()) {
        String selected = combo.getText();
        combo.setItems(names);
        combo.setText(Utils.isEmpty(selected) ? "" : selected);
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting the input fields", e);
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
    widgets.getWidgetsContents(input, GraphVectorSearchMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    transformName = wTransformName.getText();
    input.setChanged();
    dispose();
  }
}
