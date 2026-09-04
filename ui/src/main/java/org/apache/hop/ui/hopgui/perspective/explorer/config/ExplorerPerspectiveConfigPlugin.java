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

package org.apache.hop.ui.hopgui.perspective.explorer.config;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigPluginOptionsTab;
import org.apache.hop.ui.util.HelpOpenMode;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import picocli.CommandLine;

@ConfigPlugin(
    id = "ExplorerPerspectiveConfigPlugin",
    description = "Configuration options for the explorer perspective",
    category = ConfigPlugin.CATEGORY_CONFIG)
@GuiPlugin(
    description = "Explorer Perspective" // Tab label in options dialog
    )
public class ExplorerPerspectiveConfigPlugin
    implements IConfigOptions, IGuiPluginCompositeWidgetsListener {

  private static final String WIDGET_ID_LAZY_LOADING_DEPTH = "10000-lazy-loading-depth";
  private static final String WIDGET_ID_FILE_LOADING_MAX_SIZE = "10100-file-loading-max-size";
  private static final String WIDGET_ID_FILE_EXPLORER_VISIBLE_BY_DEFAULT =
      "10200-file-explorer-visible-by-default";
  private static final String WIDGET_ID_OPEN_HELP_FILES = "10300-open-help-files";
  private static final String WIDGET_ID_ACTIVE_FILE_SELECTION = "10400-active-file-selection";
  private static final String WIDGET_ID_MAX_UNDO = "10500-max-undo";

  @GuiWidgetElement(
      id = WIDGET_ID_LAZY_LOADING_DEPTH,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "i18n::ExplorerPerspectiveConfig.LazyLoading.Label",
      toolTip = "i18n::ExplorerPerspectiveConfig.LazyLoading.Tooltip")
  @CommandLine.Option(
      names = {"-exid", "--explorer-lazy-loading-initial-depth"},
      description = "For the explorer perspective: the initial depth to load not lazily")
  private String lazyLoadingDepth;

  @GuiWidgetElement(
      id = WIDGET_ID_FILE_LOADING_MAX_SIZE,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "i18n::ExplorerPerspectiveConfig.FileSize.Label",
      toolTip = "i18n::ExplorerPerspectiveConfig.FileSize.Tooltip")
  @CommandLine.Option(
      names = {"-exms", "--explorer-file-loading-max-size"},
      description = "For the explorer: the maximum file size to load")
  private String fileLoadingMaxSize;

  @GuiWidgetElement(
      id = WIDGET_ID_FILE_EXPLORER_VISIBLE_BY_DEFAULT,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::ExplorerPerspectiveConfig.FileExplorerVisible.Label",
      toolTip = "i18n::ExplorerPerspectiveConfig.FileExplorerVisible.Tooltip")
  @CommandLine.Option(
      names = {"-exv", "--explorer-file-explorer-visible-by-default"},
      description = "Show the file explorer panel by default in the explorer perspective")
  private Boolean fileExplorerVisibleByDefault = true;

  @GuiWidgetElement(
      id = WIDGET_ID_OPEN_HELP_FILES,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.COMBO,
      variables = false,
      comboValuesMethod = "getHelpOpenModeLabels",
      label = "i18n::ExplorerPerspectiveConfig.HelpOpenMode.Label",
      toolTip = "i18n::ExplorerPerspectiveConfig.HelpOpenMode.Tooltip")
  @CommandLine.Option(
      names = {"--open-help-mode"},
      description = "Where to open help pages: BROWSER, TAB or DIALOG")
  private String helpOpenMode;

  @CommandLine.Option(
      names = {"-oh", "--open-help-in-tabs"},
      description =
          "Deprecated: open help files in Hop GUI tabs instead of the external browser. Prefer --open-help-mode=TAB")
  private Boolean openingHelpFiles;

  @GuiWidgetElement(
      id = WIDGET_ID_ACTIVE_FILE_SELECTION,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::ExplorerPerspectiveConfig.ActiveFileSelection.Label",
      toolTip = "i18n::ExplorerPerspectiveConfig.ActiveFileSelection.Tooltip")
  @CommandLine.Option(
      names = {"-exafs", "--explorer-active-file-selection"},
      description = "Automatically select the active tab file in the file explorer tree")
  private Boolean activeFileSelection = true;

  @GuiWidgetElement(
      id = WIDGET_ID_MAX_UNDO,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::ExplorerPerspectiveConfig.MaxUndo.Label",
      toolTip = "i18n::ExplorerPerspectiveConfig.MaxUndo.Tooltip")
  @CommandLine.Option(
      names = {"-mu", "--max-undo"},
      description =
          "The maximum number of undo operations kept for pipelines, workflows and tables")
  private String maxUndo;

  /**
   * Gets instance
   *
   * @return value of instance
   */
  public static ExplorerPerspectiveConfigPlugin getInstance() {
    ExplorerPerspectiveConfigPlugin instance = new ExplorerPerspectiveConfigPlugin();

    ExplorerPerspectiveConfig config = ExplorerPerspectiveConfigSingleton.getConfig();
    instance.lazyLoadingDepth = config.getLazyLoadingDepth();
    instance.fileLoadingMaxSize = config.getFileLoadingMaxSize();
    Boolean visibleByDefault = config.getFileExplorerVisibleByDefault();
    instance.fileExplorerVisibleByDefault = visibleByDefault != null ? visibleByDefault : true;
    instance.helpOpenMode = config.getHelpOpenMode().getLabel();
    instance.activeFileSelection = config.getActiveFileSelection();
    instance.maxUndo = Integer.toString(org.apache.hop.ui.core.PropsUi.getInstance().getMaxUndo());

    return instance;
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    ExplorerPerspectiveConfig config = ExplorerPerspectiveConfigSingleton.getConfig();
    try {
      boolean changed = false;

      if (lazyLoadingDepth != null) {
        config.setLazyLoadingDepth(lazyLoadingDepth);
        log.logBasic(
            "Explorer perspective: the lazy loading depth is set to '" + lazyLoadingDepth + "'");
        changed = true;
      }

      if (fileLoadingMaxSize != null) {
        config.setFileLoadingMaxSize(fileLoadingMaxSize);
        log.logBasic(
            "Explorer perspective: the file loading maximum size (in MB) is set to '"
                + fileLoadingMaxSize
                + "'");
        changed = true;
      }

      if (fileExplorerVisibleByDefault != null) {
        config.setFileExplorerVisibleByDefault(fileExplorerVisibleByDefault);
        log.logDetailed(
            "Explorer perspective: file explorer visible by default is set to '"
                + fileExplorerVisibleByDefault
                + "'");
        changed = true;
      }

      if (Boolean.TRUE.equals(openingHelpFiles)) {
        config.setHelpOpenMode(HelpOpenMode.TAB);
        log.logBasic("Explorer perspective: open help mode is set to '" + HelpOpenMode.TAB + "'");
        changed = true;
      }

      if (helpOpenMode != null) {
        HelpOpenMode mode = HelpOpenMode.fromConfigValue(helpOpenMode);
        config.setHelpOpenMode(mode);
        log.logBasic("Explorer perspective: open help mode is set to '" + mode + "'");
        changed = true;
      }

      if (activeFileSelection != null) {
        config.setActiveFileSelection(activeFileSelection);
        log.logDetailed(
            "Explorer perspective: active file selection is set to '" + activeFileSelection + "'");
        changed = true;
      }

      if (maxUndo != null) {
        persistMaxUndo(maxUndo);
        log.logBasic("Maximum undo operations is set to '" + maxUndo + "'");
        changed = true;
      }

      // Save to file if anything changed
      //
      if (changed) {
        ExplorerPerspectiveConfigSingleton.saveConfig();
      }
      return changed;
    } catch (Exception e) {
      throw new HopException("Error handling explorer perspective configuration options", e);
    }
  }

  @Override
  public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
    // Do nothing
  }

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
    // Do nothing
  }

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
    persistContents(compositeWidgets);
  }

  @Override
  public void persistContents(GuiCompositeWidgets compositeWidgets) {
    for (String widgetId : compositeWidgets.getWidgetsMap().keySet()) {
      Control control = compositeWidgets.getWidgetsMap().get(widgetId);
      switch (widgetId) {
        case WIDGET_ID_LAZY_LOADING_DEPTH:
          lazyLoadingDepth = ((TextVar) control).getText();
          ExplorerPerspectiveConfigSingleton.getConfig().setLazyLoadingDepth(lazyLoadingDepth);
          break;
        case WIDGET_ID_FILE_LOADING_MAX_SIZE:
          fileLoadingMaxSize = ((TextVar) control).getText();
          ExplorerPerspectiveConfigSingleton.getConfig().setFileLoadingMaxSize(fileLoadingMaxSize);
          break;
        case WIDGET_ID_FILE_EXPLORER_VISIBLE_BY_DEFAULT:
          fileExplorerVisibleByDefault = ((Button) control).getSelection();
          ExplorerPerspectiveConfigSingleton.getConfig()
              .setFileExplorerVisibleByDefault(fileExplorerVisibleByDefault);
          break;
        case WIDGET_ID_OPEN_HELP_FILES:
          helpOpenMode = readComboText(control);
          ExplorerPerspectiveConfigSingleton.getConfig()
              .setHelpOpenMode(HelpOpenMode.fromLabel(helpOpenMode));
          break;
        case WIDGET_ID_ACTIVE_FILE_SELECTION:
          activeFileSelection = ((Button) control).getSelection();
          ExplorerPerspectiveConfigSingleton.getConfig()
              .setActiveFileSelection(activeFileSelection);
          break;
        case WIDGET_ID_MAX_UNDO:
          if (control instanceof TextVar textVar) {
            maxUndo = textVar.getText();
          } else {
            maxUndo = ((org.eclipse.swt.widgets.Text) control).getText();
          }
          persistMaxUndo(maxUndo);
          break;
        default:
          break;
      }
    }
    // Save the project...
    //
    try {
      ExplorerPerspectiveConfigSingleton.saveConfig();
    } catch (Exception e) {
      new ErrorDialog(HopGui.getInstance().getShell(), "Error", "Error saving option", e);
    }
  }

  public String getLazyLoadingDepth() {
    return lazyLoadingDepth;
  }

  public void setLazyLoadingDepth(String lazyLoadingDepth) {
    this.lazyLoadingDepth = lazyLoadingDepth;
  }

  public String getFileLoadingMaxSize() {
    return fileLoadingMaxSize;
  }

  public void setFileLoadingMaxSize(String fileLoadingMaxSize) {
    this.fileLoadingMaxSize = fileLoadingMaxSize;
  }

  public Boolean getFileExplorerVisibleByDefault() {
    return fileExplorerVisibleByDefault != null ? fileExplorerVisibleByDefault : true;
  }

  public void setFileExplorerVisibleByDefault(Boolean fileExplorerVisibleByDefault) {
    this.fileExplorerVisibleByDefault = fileExplorerVisibleByDefault;
  }

  public String getHelpOpenMode() {
    return helpOpenMode;
  }

  public void setHelpOpenMode(String helpOpenMode) {
    this.helpOpenMode = helpOpenMode;
  }

  public Boolean isOpeningHelpFiles() {
    return openingHelpFiles != null ? openingHelpFiles : false;
  }

  public void setOpeningHelpFiles(Boolean openingHelpFiles) {
    this.openingHelpFiles = openingHelpFiles;
  }

  /**
   * Combo values for {@link HelpOpenMode} shown in the Configuration perspective.
   *
   * @param log unused (required by GuiCompositeWidgets)
   * @param metadataProvider unused (required by GuiCompositeWidgets)
   * @return translated labels in enum order
   */
  public List<String> getHelpOpenModeLabels(
      ILogChannel log, IHopMetadataProvider metadataProvider) {
    List<String> labels = new ArrayList<>();
    for (HelpOpenMode mode : HelpOpenMode.values()) {
      labels.add(mode.getLabel());
    }
    return labels;
  }

  private static String readComboText(Control control) {
    if (control instanceof Combo combo) {
      return combo.getText();
    }
    if (control instanceof ComboVar comboVar) {
      return comboVar.getText();
    }
    return "";
  }

  public Boolean getActiveFileSelection() {
    return activeFileSelection != null ? activeFileSelection : true;
  }

  public void setActiveFileSelection(Boolean activeFileSelection) {
    this.activeFileSelection = activeFileSelection;
  }

  public String getMaxUndo() {
    return maxUndo;
  }

  public void setMaxUndo(String maxUndo) {
    this.maxUndo = maxUndo;
  }

  private static void persistMaxUndo(String maxUndoText) {
    int value = Const.toInt(maxUndoText, Const.MAX_UNDO);
    if (value < 1) {
      value = 1;
    }
    org.apache.hop.ui.core.PropsUi.getInstance().setMaxUndo(value);
  }
}
