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

import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.ui.core.dialog.EnterOptionsDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
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

  @GuiWidgetElement(
      id = WIDGET_ID_LAZY_LOADING_DEPTH,
      parentId = EnterOptionsDialog.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "Lazy loading maximum initial depth")
  @CommandLine.Option(
      names = {"-exid", "--explorer-lazy-loading-initial-depth"},
      description = "For the explorer perspective: the initial depth to load not lazily")
  private String lazyLoadingDepth;

  @GuiWidgetElement(
      id = WIDGET_ID_FILE_LOADING_MAX_SIZE,
      parentId = EnterOptionsDialog.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "The maxim file size to load (in MB)")
  @CommandLine.Option(
      names = {"-exms", "--explorer-file-loading-max-size"},
      description = "For the explorer: the maximum file size to load")
  private String fileLoadingMaxSize;

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
  public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {}

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {}

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {}

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
}
