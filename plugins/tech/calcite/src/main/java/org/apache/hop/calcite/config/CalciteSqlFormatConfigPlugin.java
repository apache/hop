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

package org.apache.hop.calcite.config;

import lombok.Getter;
import lombok.Setter;
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
 * Configuration-perspective Plugins entry for Apache Calcite {@code SqlFormatOptions}. Values are
 * stored in hop-config.json and applied when Format SQL is clicked.
 */
@Getter
@Setter
@ConfigPlugin(
    id = "CalciteSqlFormatConfigPlugin",
    description = "Apache Calcite SQL formatter options",
    category = ConfigPlugin.CATEGORY_CONFIG)
@GuiPlugin(description = "i18n::CalciteSqlFormatConfigPlugin.Name")
public class CalciteSqlFormatConfigPlugin
    implements IConfigOptions, IGuiPluginCompositeWidgetsListener {

  private static final Class<?> PKG = CalciteSqlFormatConfigPlugin.class;

  private static final String PARENT = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID;
  private static final String GROUP_LAYOUT = "i18n::CalciteSqlFormatConfigPlugin.Group.Layout";
  private static final String GROUP_KEYWORDS = "i18n::CalciteSqlFormatConfigPlugin.Group.Keywords";

  @GuiWidgetElement(
      id = "0100-sql-format-indentation",
      order = "0100",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::CalciteSqlFormatConfigPlugin.Indentation.Label",
      toolTip = "i18n::CalciteSqlFormatConfigPlugin.Indentation.Tooltip",
      group = GROUP_LAYOUT,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @CommandLine.Option(
      names = {"--sql-format-indentation"},
      description = "Number of spaces to indent SQL")
  private String indentation;

  @GuiWidgetElement(
      id = "0110-sql-format-line-length",
      order = "0110",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::CalciteSqlFormatConfigPlugin.LineLength.Label",
      toolTip = "i18n::CalciteSqlFormatConfigPlugin.LineLength.Tooltip",
      group = GROUP_LAYOUT,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @CommandLine.Option(
      names = {"--sql-format-line-length"},
      description = "Maximum line length (0 means no maximum)")
  private String lineLength;

  @GuiWidgetElement(
      id = "0120-sql-format-clause-starts-line",
      order = "0120",
      parentId = PARENT,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::CalciteSqlFormatConfigPlugin.ClauseStartsLine.Label",
      toolTip = "i18n::CalciteSqlFormatConfigPlugin.ClauseStartsLine.Tooltip",
      group = GROUP_LAYOUT,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @CommandLine.Option(
      names = {"--sql-format-clause-starts-line"},
      description = "Start FROM, WHERE, GROUP BY and similar clauses on a new line",
      negatable = true)
  private Boolean clauseStartsLine;

  @GuiWidgetElement(
      id = "0130-sql-format-always-use-parentheses",
      order = "0130",
      parentId = PARENT,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::CalciteSqlFormatConfigPlugin.AlwaysUseParentheses.Label",
      toolTip = "i18n::CalciteSqlFormatConfigPlugin.AlwaysUseParentheses.Tooltip",
      group = GROUP_LAYOUT,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @CommandLine.Option(
      names = {"--sql-format-always-use-parentheses"},
      description = "Always wrap expressions in parentheses",
      negatable = true)
  private Boolean alwaysUseParentheses;

  @GuiWidgetElement(
      id = "0140-sql-format-select-list-items-on-separate-lines",
      order = "0140",
      parentId = PARENT,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::CalciteSqlFormatConfigPlugin.SelectListItemsOnSeparateLines.Label",
      toolTip = "i18n::CalciteSqlFormatConfigPlugin.SelectListItemsOnSeparateLines.Tooltip",
      group = GROUP_LAYOUT,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @CommandLine.Option(
      names = {"--sql-format-select-list-items-on-separate-lines"},
      description = "Put each SELECT list item on its own line",
      negatable = true)
  private Boolean selectListItemsOnSeparateLines;

  @GuiWidgetElement(
      id = "0150-sql-format-where-list-items-on-separate-lines",
      order = "0150",
      parentId = PARENT,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::CalciteSqlFormatConfigPlugin.WhereListItemsOnSeparateLines.Label",
      toolTip = "i18n::CalciteSqlFormatConfigPlugin.WhereListItemsOnSeparateLines.Tooltip",
      group = GROUP_LAYOUT,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @CommandLine.Option(
      names = {"--sql-format-where-list-items-on-separate-lines"},
      description = "Put each WHERE condition on its own line",
      negatable = true)
  private Boolean whereListItemsOnSeparateLines;

  @GuiWidgetElement(
      id = "0160-sql-format-case-clauses-on-new-lines",
      order = "0160",
      parentId = PARENT,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::CalciteSqlFormatConfigPlugin.CaseClausesOnNewLines.Label",
      toolTip = "i18n::CalciteSqlFormatConfigPlugin.CaseClausesOnNewLines.Tooltip",
      group = GROUP_LAYOUT,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @CommandLine.Option(
      names = {"--sql-format-case-clauses-on-new-lines"},
      description = "Put WHEN, THEN and ELSE of a CASE on new lines",
      negatable = true)
  private Boolean caseClausesOnNewLines;

  @GuiWidgetElement(
      id = "0170-sql-format-window-declaration-starts-line",
      order = "0170",
      parentId = PARENT,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::CalciteSqlFormatConfigPlugin.WindowDeclarationStartsLine.Label",
      toolTip = "i18n::CalciteSqlFormatConfigPlugin.WindowDeclarationStartsLine.Tooltip",
      group = GROUP_LAYOUT,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @CommandLine.Option(
      names = {"--sql-format-window-declaration-starts-line"},
      description = "Start a WINDOW declaration on a new line",
      negatable = true)
  private Boolean windowDeclarationStartsLine;

  @GuiWidgetElement(
      id = "0180-sql-format-window-list-items-on-separate-lines",
      order = "0180",
      parentId = PARENT,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::CalciteSqlFormatConfigPlugin.WindowListItemsOnSeparateLines.Label",
      toolTip = "i18n::CalciteSqlFormatConfigPlugin.WindowListItemsOnSeparateLines.Tooltip",
      group = GROUP_LAYOUT,
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS)
  @CommandLine.Option(
      names = {"--sql-format-window-list-items-on-separate-lines"},
      description = "Put each WINDOW list item on its own line",
      negatable = true)
  private Boolean windowListItemsOnSeparateLines;

  @GuiWidgetElement(
      id = "0200-sql-format-keywords-lowercase",
      order = "0200",
      parentId = PARENT,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::CalciteSqlFormatConfigPlugin.KeywordsLowercase.Label",
      toolTip = "i18n::CalciteSqlFormatConfigPlugin.KeywordsLowercase.Tooltip",
      group = GROUP_KEYWORDS,
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS)
  @CommandLine.Option(
      names = {"--sql-format-keywords-lowercase"},
      description = "Print SQL keywords in lower case",
      negatable = true)
  private Boolean keywordsLowercase;

  @GuiWidgetElement(
      id = "0210-sql-format-quote-all-identifiers",
      order = "0210",
      parentId = PARENT,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::CalciteSqlFormatConfigPlugin.QuoteAllIdentifiers.Label",
      toolTip = "i18n::CalciteSqlFormatConfigPlugin.QuoteAllIdentifiers.Tooltip",
      group = GROUP_KEYWORDS,
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS)
  @CommandLine.Option(
      names = {"--sql-format-quote-all-identifiers"},
      description = "Quote every identifier",
      negatable = true)
  private Boolean quoteAllIdentifiers;

  public static CalciteSqlFormatConfigPlugin getInstance() {
    return new CalciteSqlFormatConfigPlugin(CalciteSqlFormatConfigSingleton.getConfig());
  }

  public CalciteSqlFormatConfigPlugin() {}

  public CalciteSqlFormatConfigPlugin(CalciteSqlFormatConfig config) {
    this.indentation = Integer.toString(config.getIndentation());
    this.lineLength = Integer.toString(config.getLineLength());
    this.clauseStartsLine = config.isClauseStartsLine();
    this.alwaysUseParentheses = config.isAlwaysUseParentheses();
    this.selectListItemsOnSeparateLines = config.isSelectListItemsOnSeparateLines();
    this.whereListItemsOnSeparateLines = config.isWhereListItemsOnSeparateLines();
    this.caseClausesOnNewLines = config.isCaseClausesOnNewLines();
    this.windowDeclarationStartsLine = config.isWindowDeclarationStartsLine();
    this.windowListItemsOnSeparateLines = config.isWindowListItemsOnSeparateLines();
    this.keywordsLowercase = config.isKeywordsLowercase();
    this.quoteAllIdentifiers = config.isQuoteAllIdentifiers();
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException {
    try {
      boolean changed = CalciteSqlFormatConfigSingleton.getConfig().applyFrom(this);
      if (changed) {
        CalciteSqlFormatConfigSingleton.saveConfig();
        log.logBasic("Apache Calcite SQL formatter options updated");
      }
      return changed;
    } catch (Exception e) {
      throw new HopException("Error handling Apache Calcite SQL formatter options", e);
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
    CalciteSqlFormatConfigSingleton.getConfig().applyFrom(this);
    try {
      CalciteSqlFormatConfigSingleton.saveConfig();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "CalciteSqlFormatConfigPlugin.Save.Error.Title"),
          BaseMessages.getString(PKG, "CalciteSqlFormatConfigPlugin.Save.Error.Message"),
          e);
    }
  }
}
