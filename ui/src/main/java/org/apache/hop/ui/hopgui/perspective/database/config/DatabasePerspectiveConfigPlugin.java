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

package org.apache.hop.ui.hopgui.perspective.database.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigPluginOptionsTab;
import org.eclipse.swt.widgets.Control;
import picocli.CommandLine;

/**
 * Configuration-perspective Plugins entry for the Database perspective. Values are stored in
 * hop-config.json.
 */
@Getter
@Setter
@ConfigPlugin(
    id = "DatabasePerspectiveConfigPlugin",
    description = "Configuration options for the Database perspective",
    category = ConfigPlugin.CATEGORY_CONFIG)
@GuiPlugin(description = "i18n::DatabasePerspectiveConfigPlugin.Name")
public class DatabasePerspectiveConfigPlugin
    implements IConfigOptions, IGuiPluginCompositeWidgetsListener {

  private static final Class<?> PKG = DatabasePerspectiveConfigPlugin.class;

  private static final String PARENT = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID;

  @GuiWidgetElement(
      id = "0100-auto-connect-when-executing-sql",
      order = "0100",
      parentId = PARENT,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::DatabasePerspectiveConfigPlugin.AutoConnect.Label",
      toolTip = "i18n::DatabasePerspectiveConfigPlugin.AutoConnect.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "SQL")
  @CommandLine.Option(
      names = {"--database-auto-connect-when-executing-sql"},
      description = "Connect the SQL editor's database automatically when executing SQL",
      negatable = true)
  private Boolean autoConnectWhenExecutingSql;

  @GuiWidgetElement(
      id = "0110-select-executed-sql",
      order = "0110",
      parentId = PARENT,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::DatabasePerspectiveConfigPlugin.SelectExecuted.Label",
      toolTip = "i18n::DatabasePerspectiveConfigPlugin.SelectExecuted.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "SQL")
  @CommandLine.Option(
      names = {"--database-select-executed-sql"},
      description = "Select the executed SQL in the editor after Run or Run all",
      negatable = true)
  private Boolean selectExecutedSql;

  @GuiWidgetElement(
      id = "0120-query-row-limit",
      order = "0120",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::DatabasePerspectiveConfigPlugin.QueryRowLimit.Label",
      toolTip = "i18n::DatabasePerspectiveConfigPlugin.QueryRowLimit.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "SQL")
  @CommandLine.Option(
      names = {"--database-query-row-limit"},
      description = "Maximum rows shown for a SELECT in the SQL editor and for table preview")
  private String queryRowLimit;

  public static DatabasePerspectiveConfigPlugin getInstance() {
    DatabasePerspectiveConfigPlugin instance = new DatabasePerspectiveConfigPlugin();
    DatabasePerspectiveConfig config = DatabasePerspectiveConfigSingleton.getConfig();
    instance.autoConnectWhenExecutingSql = config.isAutoConnectWhenExecutingSql();
    instance.selectExecutedSql = config.isSelectExecutedSql();
    instance.queryRowLimit = Integer.toString(config.resolvedQueryRowLimit());
    return instance;
  }

  public DatabasePerspectiveConfigPlugin() {}

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException {
    try {
      DatabasePerspectiveConfig config = DatabasePerspectiveConfigSingleton.getConfig();
      boolean changed = false;
      if (autoConnectWhenExecutingSql != null
          && config.isAutoConnectWhenExecutingSql() != autoConnectWhenExecutingSql) {
        config.setAutoConnectWhenExecutingSql(autoConnectWhenExecutingSql);
        log.logBasic(
            "Database perspective: auto-connect when executing SQL is set to '"
                + autoConnectWhenExecutingSql
                + "'");
        changed = true;
      }
      if (selectExecutedSql != null && config.isSelectExecutedSql() != selectExecutedSql) {
        config.setSelectExecutedSql(selectExecutedSql);
        log.logBasic(
            "Database perspective: select executed SQL is set to '" + selectExecutedSql + "'");
        changed = true;
      }
      if (queryRowLimit != null) {
        int parsed = parseQueryRowLimit(queryRowLimit);
        if (config.resolvedQueryRowLimit() != parsed) {
          config.setQueryRowLimit(parsed);
          log.logBasic("Database perspective: query row limit is set to '" + parsed + "'");
          changed = true;
        }
      }
      if (changed) {
        DatabasePerspectiveConfigSingleton.saveConfig();
      }
      return changed;
    } catch (Exception e) {
      throw new HopException("Error handling Database perspective configuration options", e);
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
    compositeWidgets.getWidgetsContents(this, PARENT);
    DatabasePerspectiveConfig config = DatabasePerspectiveConfigSingleton.getConfig();
    if (autoConnectWhenExecutingSql != null) {
      config.setAutoConnectWhenExecutingSql(autoConnectWhenExecutingSql);
    }
    if (selectExecutedSql != null) {
      config.setSelectExecutedSql(selectExecutedSql);
    }
    if (queryRowLimit != null) {
      config.setQueryRowLimit(parseQueryRowLimit(queryRowLimit));
    }
    try {
      DatabasePerspectiveConfigSingleton.saveConfig();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "DatabasePerspectiveConfigPlugin.Save.Error.Title"),
          BaseMessages.getString(PKG, "DatabasePerspectiveConfigPlugin.Save.Error.Message"),
          e);
    }
  }

  static int parseQueryRowLimit(String raw) {
    int value =
        Const.toInt(Const.NVL(raw, "").trim(), DatabasePerspectiveConfig.DEFAULT_QUERY_ROW_LIMIT);
    return value > 0 ? value : DatabasePerspectiveConfig.DEFAULT_QUERY_ROW_LIMIT;
  }
}
