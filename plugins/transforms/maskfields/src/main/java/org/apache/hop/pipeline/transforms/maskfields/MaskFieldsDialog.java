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

package org.apache.hop.pipeline.transforms.maskfields;

import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeButtonsListener;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

public class MaskFieldsDialog extends BaseTransformDialog {

  private static final Class<?> PKG = MaskFieldsMeta.class;

  private final MaskFieldsMeta input;
  private GuiCompositeWidgets widgets;

  public MaskFieldsDialog(
      Shell parent, IVariables variables, MaskFieldsMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "MaskFieldsDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();
    changed = input.hasChanged();

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wTransformName,
            wOk,
            MaskFieldsMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            input);
    widgets.setCompositeButtonsListener(
        new IGuiPluginCompositeButtonsListener() {
          @Override
          public void buttonPressed(Object sourceObject) {
            addIncomingFields();
          }
        });
    widgets.setCompositeWidgetsListener(
        new IGuiPluginCompositeWidgetsListener() {
          @Override
          public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
            // Every field is annotated.
          }

          @Override
          public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
            // Values are loaded by addScrolledComposite.
          }

          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            input.setChanged();
          }

          @Override
          public void persistContents(GuiCompositeWidgets compositeWidgets) {
            // OK reads the widgets.
          }
        });

    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void addIncomingFields() {
    try {
      widgets.getWidgetsContents(input, MaskFieldsMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
      if (input.getFields() == null) {
        input.setFields(new java.util.ArrayList<>());
      }
      IRowMeta previous = pipelineMeta.getPrevTransformFields(variables, transformMeta);
      Set<String> present = new HashSet<>();
      for (MaskField field : input.getFields()) {
        if (field != null && StringUtils.isNotEmpty(field.getFieldName())) {
          present.add(field.getFieldName());
        }
      }
      if (previous != null) {
        boolean added = false;
        for (IValueMeta valueMeta : previous.getValueMetaList()) {
          if (present.add(valueMeta.getName())) {
            input.getFields().add(new MaskField(valueMeta.getName(), ""));
            added = true;
          }
        }
        if (added) {
          input.setChanged();
        }
      }
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MaskFields.GetFields.Error.Title"),
          BaseMessages.getString(PKG, "MaskFields.GetFields.Error.Message"),
          e);
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
    widgets.getWidgetsContents(input, MaskFieldsMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    if (input.getFields() != null) {
      input
          .getFields()
          .removeIf(field -> field == null || StringUtils.isEmpty(field.getFieldName()));
    }
    transformName = wTransformName.getText();
    input.setChanged();
    dispose();
  }
}
