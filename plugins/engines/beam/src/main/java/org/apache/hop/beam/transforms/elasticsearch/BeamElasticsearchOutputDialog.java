/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.beam.transforms.elasticsearch;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

public class BeamElasticsearchOutputDialog extends BaseTransformDialog {
  private final BeamElasticsearchOutputMeta input;
  private GuiCompositeWidgets widgets;

  public BeamElasticsearchOutputDialog(
      Shell parent,
      IVariables variables,
      BeamElasticsearchOutputMeta meta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, meta, pipelineMeta);
    input = meta;
  }

  @Override
  public String open() {
    createShell(
        BaseMessages.getString(BeamElasticsearchOutputMeta.class, "BeamElasticsearchOutput.Name"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();
    changed = input.hasChanged();
    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wTransformName,
            wOk,
            BeamElasticsearchOutputMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            input);
    widgets.setCompositeWidgetsListener(
        new IGuiPluginCompositeWidgetsListener() {
          @Override
          public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {}

          @Override
          public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {}

          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control control, String widgetId) {
            input.setChanged();
          }

          @Override
          public void persistContents(GuiCompositeWidgets compositeWidgets) {}
        });
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (wTransformName.getText().isBlank()) {
      return;
    }
    widgets.getWidgetsContents(input, BeamElasticsearchOutputMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    transformName = wTransformName.getText();
    input.setChanged();
    dispose();
  }
}
