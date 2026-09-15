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

package org.apache.hop.ui.hopgui.search.config;

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
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigPluginOptionsTab;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import picocli.CommandLine;

@Getter
@Setter
@ConfigPlugin(
    id = "SearchConfigPlugin",
    description = "Configuration options for Hop GUI search",
    category = ConfigPlugin.CATEGORY_CONFIG)
@GuiPlugin(
    description = "Search" // Tab label in options dialog
    )
public class SearchConfigPlugin implements IConfigOptions, IGuiPluginCompositeWidgetsListener {

  private static final String WIDGET_ID_MIN_CONTENT_QUERY_LENGTH =
      "20000-search-min-content-query-length";
  private static final String WIDGET_ID_MAX_RESULTS = "20010-search-max-results";
  private static final String WIDGET_ID_MAX_MATCHES_PER_FILE = "20020-search-max-matches-per-file";
  private static final String WIDGET_ID_MAX_TEXT_FILE_SIZE_MB =
      "20030-search-max-text-file-size-mb";
  private static final String WIDGET_ID_INCLUDE_PROJECT_TEXT_FILES =
      "20040-search-include-project-text-files";
  private static final String WIDGET_ID_SEARCH_AS_YOU_TYPE = "20050-search-as-you-type";
  private static final String WIDGET_ID_DEBOUNCE_MS = "20060-search-debounce-ms";

  @GuiWidgetElement(
      id = WIDGET_ID_MIN_CONTENT_QUERY_LENGTH,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "i18n::SearchConfig.MinContentQueryLength.Label",
      toolTip = "i18n::SearchConfig.MinContentQueryLength.Tooltip")
  @CommandLine.Option(
      names = {"-smin", "--search-min-content-query-length"},
      description = "Minimum characters before content search runs in Hop GUI")
  private String minContentQueryLength;

  @GuiWidgetElement(
      id = WIDGET_ID_MAX_RESULTS,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "i18n::SearchConfig.MaxResults.Label",
      toolTip = "i18n::SearchConfig.MaxResults.Tooltip")
  @CommandLine.Option(
      names = {"-smax", "--search-max-results"},
      description = "Maximum number of search results built in Hop GUI")
  private String maxResults;

  @GuiWidgetElement(
      id = WIDGET_ID_MAX_MATCHES_PER_FILE,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "i18n::SearchConfig.MaxMatchesPerFile.Label",
      toolTip = "i18n::SearchConfig.MaxMatchesPerFile.Tooltip")
  @CommandLine.Option(
      names = {"-smpf", "--search-max-matches-per-file"},
      description = "Maximum content matches per text file in Hop GUI search")
  private String maxMatchesPerFile;

  @GuiWidgetElement(
      id = WIDGET_ID_MAX_TEXT_FILE_SIZE_MB,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "i18n::SearchConfig.MaxTextFileSizeMb.Label",
      toolTip = "i18n::SearchConfig.MaxTextFileSizeMb.Tooltip")
  @CommandLine.Option(
      names = {"-sfs", "--search-max-text-file-size-mb"},
      description = "Skip text files larger than this many MB in Hop GUI search")
  private String maxTextFileSizeMb;

  @GuiWidgetElement(
      id = WIDGET_ID_INCLUDE_PROJECT_TEXT_FILES,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::SearchConfig.IncludeProjectTextFiles.Label",
      toolTip = "i18n::SearchConfig.IncludeProjectTextFiles.Tooltip")
  @CommandLine.Option(
      names = {"-sit", "--search-include-project-text-files"},
      description = "Include project text files (CSV, JSON, …) in Hop GUI content search",
      negatable = true)
  private Boolean includeProjectTextFiles = true;

  @GuiWidgetElement(
      id = WIDGET_ID_SEARCH_AS_YOU_TYPE,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::SearchConfig.SearchAsYouType.Label",
      toolTip = "i18n::SearchConfig.SearchAsYouType.Tooltip")
  @CommandLine.Option(
      names = {"-sat", "--search-as-you-type"},
      description = "Run Hop GUI search while typing (debounced)",
      negatable = true)
  private Boolean searchAsYouType = true;

  @GuiWidgetElement(
      id = WIDGET_ID_DEBOUNCE_MS,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "i18n::SearchConfig.DebounceMs.Label",
      toolTip = "i18n::SearchConfig.DebounceMs.Tooltip")
  @CommandLine.Option(
      names = {"-sdb", "--search-debounce-ms"},
      description = "Debounce in ms between keystrokes and Hop GUI live search")
  private String debounceMs;

  public static SearchConfigPlugin getInstance() {
    SearchConfigPlugin instance = new SearchConfigPlugin();
    SearchConfig config = SearchConfigSingleton.getConfig();
    instance.minContentQueryLength = config.getMinContentQueryLength();
    instance.maxResults = config.getMaxResults();
    instance.maxMatchesPerFile = config.getMaxMatchesPerFile();
    instance.maxTextFileSizeMb = config.getMaxTextFileSizeMb();
    instance.includeProjectTextFiles =
        config.getIncludeProjectTextFiles() == null || config.getIncludeProjectTextFiles();
    instance.searchAsYouType = config.getSearchAsYouType() == null || config.getSearchAsYouType();
    instance.debounceMs = config.getDebounceMs();
    return instance;
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    SearchConfig config = SearchConfigSingleton.getConfig();
    try {
      boolean changed = false;

      if (minContentQueryLength != null) {
        config.setMinContentQueryLength(minContentQueryLength);
        log.logBasic("Search: min content query length set to '" + minContentQueryLength + "'");
        changed = true;
      }
      if (maxResults != null) {
        config.setMaxResults(maxResults);
        log.logBasic("Search: max results set to '" + maxResults + "'");
        changed = true;
      }
      if (maxMatchesPerFile != null) {
        config.setMaxMatchesPerFile(maxMatchesPerFile);
        log.logBasic("Search: max matches per file set to '" + maxMatchesPerFile + "'");
        changed = true;
      }
      if (maxTextFileSizeMb != null) {
        config.setMaxTextFileSizeMb(maxTextFileSizeMb);
        log.logBasic("Search: max text file size (MB) set to '" + maxTextFileSizeMb + "'");
        changed = true;
      }
      if (includeProjectTextFiles != null) {
        config.setIncludeProjectTextFiles(includeProjectTextFiles);
        log.logBasic("Search: include project text files set to '" + includeProjectTextFiles + "'");
        changed = true;
      }
      if (searchAsYouType != null) {
        config.setSearchAsYouType(searchAsYouType);
        log.logBasic("Search: search as you type set to '" + searchAsYouType + "'");
        changed = true;
      }
      if (debounceMs != null) {
        config.setDebounceMs(debounceMs);
        log.logBasic("Search: debounce ms set to '" + debounceMs + "'");
        changed = true;
      }

      if (changed) {
        SearchConfigSingleton.saveConfig();
      }
      return changed;
    } catch (Exception e) {
      throw new HopException("Error handling search configuration options", e);
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
    SearchConfig config = SearchConfigSingleton.getConfig();
    for (String widgetId : compositeWidgets.getWidgetsMap().keySet()) {
      Control control = compositeWidgets.getWidgetsMap().get(widgetId);
      switch (widgetId) {
        case WIDGET_ID_MIN_CONTENT_QUERY_LENGTH:
          minContentQueryLength = ((TextVar) control).getText();
          config.setMinContentQueryLength(minContentQueryLength);
          break;
        case WIDGET_ID_MAX_RESULTS:
          maxResults = ((TextVar) control).getText();
          config.setMaxResults(maxResults);
          break;
        case WIDGET_ID_MAX_MATCHES_PER_FILE:
          maxMatchesPerFile = ((TextVar) control).getText();
          config.setMaxMatchesPerFile(maxMatchesPerFile);
          break;
        case WIDGET_ID_MAX_TEXT_FILE_SIZE_MB:
          maxTextFileSizeMb = ((TextVar) control).getText();
          config.setMaxTextFileSizeMb(maxTextFileSizeMb);
          break;
        case WIDGET_ID_INCLUDE_PROJECT_TEXT_FILES:
          includeProjectTextFiles = ((Button) control).getSelection();
          config.setIncludeProjectTextFiles(includeProjectTextFiles);
          break;
        case WIDGET_ID_SEARCH_AS_YOU_TYPE:
          searchAsYouType = ((Button) control).getSelection();
          config.setSearchAsYouType(searchAsYouType);
          break;
        case WIDGET_ID_DEBOUNCE_MS:
          debounceMs = ((TextVar) control).getText();
          config.setDebounceMs(debounceMs);
          break;
        default:
          break;
      }
    }
    try {
      SearchConfigSingleton.saveConfig();
    } catch (Exception e) {
      new ErrorDialog(HopGui.getInstance().getShell(), "Error", "Error saving search options", e);
    }
  }
}
