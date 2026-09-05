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

package org.apache.hop.naming.metadata;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * Reusable naming rules for identifiers such as Hop field names, transform/action names, database
 * tables/columns, and file/folder names. Apply with CTRL-SHIFT-N on a TextVar (or related widget),
 * via the TableView toolbar, or programmatically with {@link
 * org.apache.hop.naming.engine.NamingEngine}.
 */
@Getter
@Setter
@GuiPlugin(description = "Naming Scheme editor widgets")
@HopMetadata(
    key = "naming-scheme",
    name = "i18n::NamingScheme.Name",
    description = "i18n::NamingScheme.Description",
    image = "naming.svg",
    category = HopMetadataCategory.DATA_DEFINITION,
    documentationUrl = "/metadata-types/naming-scheme.html",
    hopMetadataPropertyType = HopMetadataPropertyType.NAMING_SCHEME)
public class NamingScheme extends HopMetadataBase implements Serializable, IHopMetadata {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "NamingSchemeEditor";
  public static final String WIDGET_DESCRIPTION = "description";
  public static final String WIDGET_TYPE = "type";
  public static final String WIDGET_CASE_STYLE = "caseStyle";
  public static final String WIDGET_WORD_SEPARATOR = "wordSeparator";
  public static final String WIDGET_CAPITALIZE_FIRST_WORD = "capitalizeFirstWord";
  public static final String WIDGET_EXTRA_DELIMITERS = "extraDelimiters";
  public static final String WIDGET_REMOVE_SPECIAL = "removeSpecialCharacters";
  public static final String WIDGET_COLLAPSE_SEPARATORS = "collapseRepeatedSeparators";
  public static final String WIDGET_TRIM_EDGES = "trimEdgeSeparators";
  public static final String WIDGET_PREFIX = "prefix";
  public static final String WIDGET_SUFFIX = "suffix";

  private static final String GROUP_SCHEME = "i18n::NamingSchemeEditor.Group.Scheme";
  private static final String GROUP_ORDER = "10";

  @GuiWidgetElement(
      id = WIDGET_DESCRIPTION,
      order = "0100",
      type = GuiElementType.TEXT,
      label = "i18n::NamingSchemeEditor.Description.Label",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      variables = false,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SCHEME,
      groupOrder = GROUP_ORDER)
  @HopMetadataProperty
  private String description;

  /** Target kind code: {@link NamingSchemeType} (default {@code general}). */
  @GuiWidgetElement(
      id = WIDGET_TYPE,
      order = "0200",
      type = GuiElementType.COMBO,
      label = "i18n::NamingSchemeEditor.Type.Label",
      toolTip = "i18n::NamingSchemeEditor.Type.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      variables = false,
      comboValuesMethod = "getTypeLabels",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SCHEME,
      groupOrder = GROUP_ORDER)
  @HopMetadataProperty
  private String type;

  /** Case style code: {@link NamingCaseStyle}. */
  @GuiWidgetElement(
      id = WIDGET_CASE_STYLE,
      order = "0300",
      type = GuiElementType.COMBO,
      label = "i18n::NamingSchemeEditor.CaseStyle.Label",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      variables = false,
      comboValuesMethod = "getCaseStyleLabels",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SCHEME,
      groupOrder = GROUP_ORDER)
  @HopMetadataProperty
  private String caseStyle;

  /** Word separator code: {@link NamingWordSeparator}. */
  @GuiWidgetElement(
      id = WIDGET_WORD_SEPARATOR,
      order = "0400",
      type = GuiElementType.COMBO,
      label = "i18n::NamingSchemeEditor.WordSeparator.Label",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      variables = false,
      comboValuesMethod = "getWordSeparatorLabels",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SCHEME,
      groupOrder = GROUP_ORDER)
  @HopMetadataProperty
  private String wordSeparator;

  /**
   * Uppercase the first character of the first word after joining. Typical for transform and action
   * names with a space separator ({@code Table input}).
   */
  @GuiWidgetElement(
      id = WIDGET_CAPITALIZE_FIRST_WORD,
      order = "0450",
      type = GuiElementType.CHECKBOX,
      label = "i18n::NamingSchemeEditor.CapitalizeFirstWord.Label",
      toolTip = "i18n::NamingSchemeEditor.CapitalizeFirstWord.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SCHEME,
      groupOrder = GROUP_ORDER)
  @HopMetadataProperty
  private boolean capitalizeFirstWord;

  /**
   * Extra characters treated as word boundaries in addition to whitespace, underscore, dash and
   * camelCase edges (for example {@code .#}).
   */
  @GuiWidgetElement(
      id = WIDGET_EXTRA_DELIMITERS,
      order = "0500",
      type = GuiElementType.TEXT,
      label = "i18n::NamingSchemeEditor.ExtraDelimiters.Label",
      toolTip = "i18n::NamingSchemeEditor.ExtraDelimiters.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      variables = false,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SCHEME,
      groupOrder = GROUP_ORDER)
  @HopMetadataProperty
  private String extraDelimiters;

  /** Strip characters that are not letters or digits from each word. */
  @GuiWidgetElement(
      id = WIDGET_REMOVE_SPECIAL,
      order = "0600",
      type = GuiElementType.CHECKBOX,
      label = "i18n::NamingSchemeEditor.RemoveSpecialCharacters.Label",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SCHEME,
      groupOrder = GROUP_ORDER)
  @HopMetadataProperty
  private boolean removeSpecialCharacters;

  /** Collapse repeated word separators (for example {@code __} → {@code _}). */
  @GuiWidgetElement(
      id = WIDGET_COLLAPSE_SEPARATORS,
      order = "0700",
      type = GuiElementType.CHECKBOX,
      label = "i18n::NamingSchemeEditor.CollapseSeparators.Label",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SCHEME,
      groupOrder = GROUP_ORDER)
  @HopMetadataProperty
  private boolean collapseRepeatedSeparators;

  /** Remove leading/trailing word separators from the result. */
  @GuiWidgetElement(
      id = WIDGET_TRIM_EDGES,
      order = "0800",
      type = GuiElementType.CHECKBOX,
      label = "i18n::NamingSchemeEditor.TrimEdgeSeparators.Label",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SCHEME,
      groupOrder = GROUP_ORDER)
  @HopMetadataProperty
  private boolean trimEdgeSeparators;

  @GuiWidgetElement(
      id = WIDGET_PREFIX,
      order = "0900",
      type = GuiElementType.TEXT,
      label = "i18n::NamingSchemeEditor.Prefix.Label",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      variables = false,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SCHEME,
      groupOrder = GROUP_ORDER)
  @HopMetadataProperty
  private String prefix;

  @GuiWidgetElement(
      id = WIDGET_SUFFIX,
      order = "1000",
      type = GuiElementType.TEXT,
      label = "i18n::NamingSchemeEditor.Suffix.Label",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      variables = false,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SCHEME,
      groupOrder = GROUP_ORDER)
  @HopMetadataProperty
  private String suffix;

  public NamingScheme() {
    this.type = NamingSchemeType.GENERAL.getCode();
    this.caseStyle = NamingCaseStyle.LOWER.getCode();
    this.wordSeparator = NamingWordSeparator.UNDERSCORE.getCode();
    this.capitalizeFirstWord = false;
    this.extraDelimiters = "";
    this.removeSpecialCharacters = true;
    this.collapseRepeatedSeparators = true;
    this.trimEdgeSeparators = true;
    this.prefix = "";
    this.suffix = "";
  }

  public NamingScheme(String name) {
    this();
    this.name = name;
  }

  public List<String> getTypeLabels(ILogChannel log, IHopMetadataProvider metadataProvider) {
    return Arrays.asList(NamingSchemeType.getPluginLabels());
  }

  public List<String> getCaseStyleLabels(ILogChannel log, IHopMetadataProvider metadataProvider) {
    return Arrays.asList(NamingCaseStyle.getLabels());
  }

  public List<String> getWordSeparatorLabels(
      ILogChannel log, IHopMetadataProvider metadataProvider) {
    return Arrays.asList(NamingWordSeparator.getLabels());
  }
}
