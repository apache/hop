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

package org.apache.hop.pipeline.transforms.formula.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigPluginOptionsTab;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import picocli.CommandLine;

/**
 * The Formula transform options in the configuration perspective and in {@code hop conf}. The
 * values themselves live in {@link FormulaConfig}.
 */
@Getter
@Setter
@ConfigPlugin(id = "FormulaConfigPlugin", description = "Formula transform options")
@GuiPlugin(description = "i18n::FormulaConfigPlugin.Name")
public class FormulaConfigPlugin implements IConfigOptions, IGuiPluginCompositeWidgetsListener {

  private static final Class<?> PKG = FormulaConfigPlugin.class;

  private static final String WIDGET_ID_FAST_PATH_ENABLED = "10000-formula-fast-path-enabled";

  @GuiWidgetElement(
      id = WIDGET_ID_FAST_PATH_ENABLED,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      variables = false,
      defaultValue = "true",
      label = "i18n::FormulaConfigPlugin.FastPathEnabled.Label",
      toolTip = "i18n::FormulaConfigPlugin.FastPathEnabled.Tooltip")
  @CommandLine.Option(
      names = {"--formula-fast-path"},
      negatable = true,
      description =
          "Let the Formula transform evaluate the formulas it can without Apache POI."
              + " --no-formula-fast-path always uses Apache POI")
  private Boolean fastPathEnabled;

  public static FormulaConfigPlugin getInstance() {
    FormulaConfigPlugin instance = new FormulaConfigPlugin();
    instance.fastPathEnabled = FormulaConfig.isFastPathEnabled();
    return instance;
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    if (fastPathEnabled == null) {
      return false;
    }
    try {
      FormulaConfig.setFastPathEnabled(fastPathEnabled);
    } catch (Exception e) {
      throw new HopException("Error saving the Formula transform options", e);
    }
    if (fastPathEnabled) {
      log.logBasic("The Formula transform evaluates the formulas it can without Apache POI");
    } else {
      log.logBasic("The Formula transform evaluates every formula with Apache POI");
    }
    return true;
  }

  @Override
  public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
    // Nothing to do
  }

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
    // Nothing to do
  }

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
    persistContents(compositeWidgets);
  }

  @Override
  public void persistContents(GuiCompositeWidgets compositeWidgets) {
    if (!(compositeWidgets.getWidgetsMap().get(WIDGET_ID_FAST_PATH_ENABLED)
        instanceof Button button)) {
      return;
    }
    fastPathEnabled = button.getSelection();
    try {
      FormulaConfig.setFastPathEnabled(fastPathEnabled);
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "FormulaConfigPlugin.SavingOption.ErrorDialog.Header"),
          BaseMessages.getString(PKG, "FormulaConfigPlugin.SavingOption.ErrorDialog.Message"),
          e);
    }
  }
}
