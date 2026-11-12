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

package org.apache.hop.pipeline.transforms.normaliser;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.BackgroundThreadFacade;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

public class NormaliserDialog extends BaseTransformDialog {
  private static final Class<?> PKG = NormaliserMeta.class;

  private final NormaliserMeta input;

  private GuiCompositeWidgets widgets;

  /** The names of the input fields, offered in the field name columns. */
  private final List<String> inputFields = new ArrayList<>();

  public NormaliserDialog(
      Shell parent, IVariables variables, NormaliserMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "NormaliserDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).get(e -> get()).cancel(e -> cancel()).build();

    changed = input.hasChanged();

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell, variables, wSpacer, wOk, NormaliserMeta.GUI_PLUGIN_ELEMENT_PARENT_ID, input);
    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            input.setChanged();
            if (NormaliserMeta.WIDGET_FIELDS.equals(widgetId)) {
              enableKeyFields();
            }
          }
        });

    for (TableView table :
        List.of(table(NormaliserMeta.WIDGET_KEY_FIELDS), table(NormaliserMeta.WIDGET_FIELDS))) {
      table.getColumns()[0].setComboValueSupplier(this::inputFieldNames);
    }
    enableKeyFields();
    searchInputFields();

    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private String[] inputFieldNames() {
    synchronized (inputFields) {
      return ConstUi.sortFieldNames(new ArrayList<>(inputFields));
    }
  }

  private TableView table(String widgetId) {
    return (TableView) widgets.getWidgetsMap().get(widgetId);
  }

  /** The key fields only count when no fields are listed to normalise. */
  private void enableKeyFields() {
    boolean noFieldsListed = table(NormaliserMeta.WIDGET_FIELDS).nrNonEmpty() == 0;
    table(NormaliserMeta.WIDGET_KEY_FIELDS).setEnabled(noFieldsListed);
  }

  private void searchInputFields() {
    BackgroundThreadFacade.start(
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);
              List<String> names = new ArrayList<>();
              for (int i = 0; i < row.size(); i++) {
                names.add(row.getValueMeta(i).getName());
              }
              synchronized (inputFields) {
                inputFields.clear();
                inputFields.addAll(names);
              }
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        });
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
    applyWidgets();
    dispose();
  }

  private void applyWidgets() {
    widgets.getWidgetsContents(input, NormaliserMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    input.getNormaliserFields().removeIf(field -> field == null || Utils.isEmpty(field.getName()));
    input.getKeyFields().removeIf(field -> field == null || Utils.isEmpty(field.getName()));
  }

  /** Adds the input fields that are not listed yet, each under its own name as type. */
  private void get() {
    try {
      applyWidgets();
      IRowMeta previous = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (previous == null || previous.isEmpty()) {
        return;
      }
      Set<String> present = new HashSet<>();
      for (NormaliserField field : input.getNormaliserFields()) {
        present.add(field.getName());
      }
      for (IValueMeta valueMeta : previous.getValueMetaList()) {
        if (present.add(valueMeta.getName())) {
          NormaliserField field = new NormaliserField();
          field.setName(valueMeta.getName());
          field.setValue(valueMeta.getName());
          input.getNormaliserFields().add(field);
        }
      }
      input.setChanged();
      widgets.setWidgetsContents(input, shell, NormaliserMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
      enableKeyFields();
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "NormaliserDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "NormaliserDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }
}
