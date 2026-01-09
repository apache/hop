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
 *
 */

package org.apache.hop.git.config;

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

@ConfigPlugin(
    id = "GitConfigOptionPlugin",
    description = "Configuration options for the git GUI plugin")
@GuiPlugin(
    description = "i18n::GitConfig.Tab.Name" // label in options dialog
    )
@Getter
@Setter
public class GitConfigOptionPlugin implements IConfigOptions, IGuiPluginCompositeWidgetsListener {
  protected static final Class<?> PKG = GitConfigOptionPlugin.class;

  private static final String WIDGET_ID_GIT_ENABLE = "10000-git-enable-plugin";
  private static final String WIDGET_ID_GIT_SEARCH_PARENT_FOLDERS =
      "10010-git-search-parent-folders";

  @GuiWidgetElement(
      id = WIDGET_ID_GIT_ENABLE,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::GitConfig.EnableGitPlugin.Message")
  @CommandLine.Option(
      names = {"--git-gui-enabled"},
      description = "Enable or disable the git GUI plugin")
  private Boolean gitEnabled;

  @GuiWidgetElement(
      id = WIDGET_ID_GIT_SEARCH_PARENT_FOLDERS,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::GitConfig.SearchParentFolders.Message")
  @CommandLine.Option(
      names = {"--git-gui-search-parent-folders"},
      description = "The git GUI searches the parent folders for a repository")
  private Boolean searchingParentFolders;

  public static GitConfigOptionPlugin getInstance() {
    GitConfigOptionPlugin instance = new GitConfigOptionPlugin();
    GitConfig config = GitConfigSingleton.getConfig();
    instance.gitEnabled = config.isEnabled();
    instance.searchingParentFolders = config.isSearchingParentFolders();
    return instance;
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    GitConfig config = GitConfigSingleton.getConfig();
    try {
      boolean changed = false;
      if (gitEnabled != null) {
        config.setEnabled(gitEnabled);
        if (gitEnabled) {
          log.logBasic("Enabled the git GUI plugin");
        } else {
          log.logBasic("Disabled the git GUI plugin");
        }
        changed = true;
      }
      if (searchingParentFolders != null) {
        config.setSearchingParentFolders(searchingParentFolders);
        if (searchingParentFolders) {
          log.logBasic("The git GUI plugin is searching parent folders for a repository.");
        } else {
          log.logBasic(
              "The git GUI plugin only looks at the project home folder for a repository.");
        }
        changed = true;
      }

      // Save to file if anything changed
      //
      if (changed) {
        GitConfigSingleton.saveConfig();
      }
      return changed;
    } catch (Exception e) {
      throw new HopException("Error handling git plugin configuration options", e);
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
        case WIDGET_ID_GIT_ENABLE:
          gitEnabled = ((Button) control).getSelection();
          GitConfigSingleton.getConfig().setEnabled(gitEnabled);
          break;
        case WIDGET_ID_GIT_SEARCH_PARENT_FOLDERS:
          searchingParentFolders = ((Button) control).getSelection();
          GitConfigSingleton.getConfig().setSearchingParentFolders(searchingParentFolders);
          break;
        default:
          break;
      }
    }
    // Save the project...
    //
    try {
      GitConfigSingleton.saveConfig();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "GitConfig.SavingOption.ErrorDialog.Header"),
          BaseMessages.getString(PKG, "GitConfig.SavingOption.ErrorDialog.Message"),
          e);
    }
  }
}
