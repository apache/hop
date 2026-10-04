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
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeButtonsListener;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataEditorDialog;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
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
    buildButtonBar().ok(e -> ok()).get(e -> addIncomingFields()).cancel(e -> cancel()).build();
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
            editMaskingRule();
          }
        });
    widgets.setCompositeWidgetsListener(
        new IGuiPluginCompositeWidgetsListener() {
          @Override
          public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
            installPatternComboSupplier();
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

    installPatternComboSupplier();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  /**
   * Edit the rule on the selected row, create one when that row has none, or open the Masking
   * pattern type when no row is selected.
   */
  private void editMaskingRule() {
    TableView table = fieldsTable();
    if (table == null) {
      return;
    }
    int row = table.getSelectionIndex();
    if (row < 0) {
      shell.getDisplay().asyncExec(this::openMaskingPatternType);
      return;
    }
    String ruleName = table.getItem(row, MaskFieldsMeta.RULE_COLUMN);
    if (StringUtils.isEmpty(ruleName)) {
      MaskingPattern created = createMaskingPattern();
      if (created != null && StringUtils.isNotEmpty(created.getName())) {
        table.setText(created.getName(), MaskFieldsMeta.RULE_COLUMN, row);
        widgets.getWidgetsContents(input, MaskFieldsMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
        input.setChanged();
      }
      return;
    }
    String savedName = editMaskingPattern(ruleName);
    if (savedName != null && !savedName.equals(ruleName)) {
      table.setText(savedName, MaskFieldsMeta.RULE_COLUMN, row);
      widgets.getWidgetsContents(input, MaskFieldsMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
      input.setChanged();
    }
  }

  private void openMaskingPatternType() {
    if (shell.isDisposed()) {
      return;
    }
    MetadataPerspective perspective = HopGui.getMetadataPerspective();
    if (perspective == null) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      box.setText(BaseMessages.getString(PKG, "MaskFields.EditRule.Error.Title"));
      box.setMessage(BaseMessages.getString(PKG, "MaskFields.EditRule.NoPerspective.Message"));
      box.open();
      return;
    }
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }
    applyWidgets();
    transformName = wTransformName.getText();
    input.setChanged();
    String metadataKey = MaskingPattern.class.getAnnotation(HopMetadata.class).key();
    dispose();
    perspective.activate();
    perspective.selectType(metadataKey);
  }

  private MaskingPattern createMaskingPattern() {
    try {
      HopGui hopGui = HopGui.getInstance();
      MaskingPattern element = new MaskingPattern();
      ExtensionPointHandler.callExtensionPoint(
          hopGui.getLog(),
          variables,
          HopExtensionPoint.HopGuiMetadataObjectCreateBeforeDialog.id,
          element);
      MetadataEditor<MaskingPattern> editor = patternManager().createEditor(element);
      editor.markAsNew();
      String name = new MetadataEditorDialog(shell, editor).open();
      return name == null ? null : element;
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MaskFields.EditRule.Error.Title"),
          BaseMessages.getString(PKG, "MaskFields.EditRule.CreateError.Message"),
          e);
      return null;
    }
  }

  private String editMaskingPattern(String ruleName) {
    try {
      MetadataManager<MaskingPattern> manager = patternManager();
      MaskingPattern element =
          hopGuiMetadataProvider()
              .getSerializer(MaskingPattern.class)
              .load(variables.resolve(ruleName));
      if (element == null) {
        MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        box.setText(BaseMessages.getString(PKG, "MaskFields.EditRule.Error.Title"));
        box.setMessage(BaseMessages.getString(PKG, "MaskFields.Check.MissingPattern", ruleName));
        box.open();
        return null;
      }
      MetadataEditor<MaskingPattern> editor = manager.createEditor(element);
      return new MetadataEditorDialog(shell, editor).open();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MaskFields.EditRule.Error.Title"),
          BaseMessages.getString(PKG, "MaskFields.EditRule.EditError.Message"),
          e);
      return null;
    }
  }

  private MetadataManager<MaskingPattern> patternManager() {
    return new MetadataManager<>(variables, hopGuiMetadataProvider(), MaskingPattern.class, shell);
  }

  private org.apache.hop.metadata.api.IHopMetadataProvider hopGuiMetadataProvider() {
    return HopGui.getInstance().getMetadataProvider();
  }

  private void installPatternComboSupplier() {
    TableView table = fieldsTable();
    if (table == null || table.getColumns().length <= 1) {
      return;
    }
    table.getColumns()[1].setComboValueSupplier(
        () -> input.patternNames(LogChannel.UI, hopGuiMetadataProvider()).toArray(new String[0]));
  }

  private TableView fieldsTable() {
    Control control = widgets.getWidgetsMap().get(MaskFieldsMeta.WIDGET_FIELDS);
    return control instanceof TableView tableView ? tableView : null;
  }

  private void applyWidgets() {
    widgets.getWidgetsContents(input, MaskFieldsMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    if (input.getFields() != null) {
      input
          .getFields()
          .removeIf(field -> field == null || StringUtils.isEmpty(field.getFieldName()));
    }
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
          widgets.setWidgetsContents(input, shell, MaskFieldsMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
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
    applyWidgets();
    transformName = wTransformName.getText();
    input.setChanged();
    dispose();
  }
}
