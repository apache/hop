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
 *
 */

package org.apache.hop.ui.hopgui.perspective.configuration.tabs;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.ConfigurationPerspective;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.TableItem;

@GuiPlugin
public class ConfigVariablesTab {
  private static final Class<?> PKG = BaseDialog.class;

  private TableView wFields;

  public ConfigVariablesTab() {
    // This instance is created in the GuiPlugin system by calling this constructor, after which it
    // calls the addConfigVariablesTab() method.
  }

  @GuiTab(
      id = "10200-config-perspective-variables-tab",
      parentId = ConfigurationPerspective.CONFIG_PERSPECTIVE_TABS,
      description = "System variables")
  public void addConfigVariablesTab(CTabFolder wTabFolder) {
    int margin = PropsUi.getMargin();

    CTabItem wVarsTab = new CTabItem(wTabFolder, SWT.NONE);
    wVarsTab.setFont(GuiResource.getInstance().getFontDefault());
    wVarsTab.setText(BaseMessages.getString(PKG, "HopSystemVariablesDialog.Title"));
    wVarsTab.setImage(GuiResource.getInstance().getImageVariable());

    Composite wVarsTabComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wVarsTabComp);
    FormLayout varsCompLayout = new FormLayout();
    varsCompLayout.marginWidth = PropsUi.getFormMargin();
    varsCompLayout.marginHeight = PropsUi.getFormMargin();
    wVarsTabComp.setLayout(varsCompLayout);

    // Let's add an "Apply" button at the bottom
    //
    Button wbSave = new Button(wVarsTabComp, SWT.PUSH);
    wbSave.setText(BaseMessages.getString(PKG, "System.Button.Save"));
    PropsUi.setLook(wbSave);
    wbSave.addListener(SWT.Selection, this::save);
    BaseTransformDialog.positionBottomButtons(wVarsTabComp, new Button[] {wbSave}, margin, null);

    ColumnInfo[] columns = {
      new ColumnInfo(
          BaseMessages.getString(PKG, "HopPropertiesFileDialog.Name.Label"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "HopPropertiesFileDialog.Value.Label"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "HopPropertiesFileDialog.Description.Label"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
    };
    columns[2].setDisabledListener(rowNr -> false);

    // Fields between the label and the buttons
    //
    wFields =
        new TableView(
            Variables.getADefaultVariableSpace(),
            wVarsTabComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL,
            columns,
            0,
            null,
            PropsUi.getInstance());
    wFields.setReadonly(false);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wbSave, -2 * margin);
    wFields.setLayoutData(fdFields);

    // Get the described variables from hop-config.json
    //
    List<DescribedVariable> describedVariables = HopConfig.getInstance().getDescribedVariables();
    for (DescribedVariable describedVariable : describedVariables) {
      TableItem item = new TableItem(wFields.table, SWT.NONE);
      int col = 1;
      item.setText(col++, Const.NVL(describedVariable.getName(), ""));
      item.setText(col++, Const.NVL(describedVariable.getValue(), ""));
      item.setText(col, Const.NVL(describedVariable.getDescription(), ""));
    }
    wFields.optimizeTableView();

    wVarsTab.setControl(wVarsTabComp);
  }

  private void save(Event event) {
    try {
      List<DescribedVariable> variables = new ArrayList<>();
      for (int i = 0; i < wFields.nrNonEmpty(); i++) {
        TableItem item = wFields.getNonEmpty(i);
        String name = item.getText(1);
        String value = item.getText(2);
        String description = item.getText(3);
        variables.add(new DescribedVariable(name, value, description));
      }
      HopConfig.getInstance().setDescribedVariables(variables);
      HopConfig.getInstance().saveToFile();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          "Error",
          "Error saving described variables to the Hop configuration file: "
              + HopConfig.getInstance().getConfigFilename(),
          e);
    }
  }

  private void saveValues() {
    try {
      HopConfig.getInstance().saveToFile();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(), "Error", "Error saving configuration to file", e);
    }
  }
}
