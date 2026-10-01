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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.naming.NamingSchemeTypePluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.i18n.BaseMessages;

/**
 * Target kind for a {@link NamingScheme}. Stored as a stable string code on the metadata element.
 */
public enum NamingSchemeType {
  GENERAL("general", "NamingSchemeType.General"),
  HOP_FIELD("hop-field", "NamingSchemeType.HopField"),
  HOP_TRANSFORM("hop-transform", "NamingSchemeType.HopTransform"),
  HOP_ACTION("hop-action", "NamingSchemeType.HopAction"),
  HOP_PIPELINE("hop-pipeline", "NamingSchemeType.HopPipeline"),
  HOP_WORKFLOW("hop-workflow", "NamingSchemeType.HopWorkflow"),
  HOP_METADATA("hop-metadata", "NamingSchemeType.HopMetadata"),
  HOP_VARIABLE("hop-variable", "NamingSchemeType.HopVariable"),
  DATABASE_TABLE("database-table", "NamingSchemeType.DatabaseTable"),
  DATABASE_COLUMN("database-column", "NamingSchemeType.DatabaseColumn"),
  FILE("file", "NamingSchemeType.File"),
  FOLDER("folder", "NamingSchemeType.Folder");

  private static final Class<?> PKG = NamingScheme.class;

  private final String code;
  private final String i18nKey;

  NamingSchemeType(String code, String i18nKey) {
    this.code = code;
    this.i18nKey = i18nKey;
  }

  public String getCode() {
    return code;
  }

  public String getLabel() {
    return BaseMessages.getString(PKG, i18nKey);
  }

  /**
   * Map a known built-in code to an enum constant. Unknown plugin codes (for example {@code
   * dv-hub}) are <em>not</em> coerced to {@link #HOP_FIELD} for matching — use {@link
   * NamingSchemeSelector#matching(Iterable, String)} with the raw code instead. This method still
   * returns {@link #HOP_FIELD} for unknown values so existing enum callers keep compiling.
   */
  public static NamingSchemeType fromCode(String code) {
    if (StringUtils.isEmpty(code)) {
      return HOP_FIELD;
    }
    for (NamingSchemeType type : values()) {
      if (type.code.equalsIgnoreCase(code.trim())) {
        return type;
      }
    }
    return HOP_FIELD;
  }

  public static String[] getCodes() {
    NamingSchemeType[] values = values();
    String[] codes = new String[values.length];
    for (int i = 0; i < values.length; i++) {
      codes[i] = values[i].code;
    }
    return codes;
  }

  public static String[] getLabels() {
    NamingSchemeType[] values = values();
    String[] labels = new String[values.length];
    for (int i = 0; i < values.length; i++) {
      labels[i] = values[i].getLabel();
    }
    return labels;
  }

  public static String[] getPluginLabels() {
    List<IPlugin> plugins = pluginTypes();
    if (plugins.isEmpty()) {
      return getLabels();
    }
    plugins.sort(
        Comparator.comparing((IPlugin p) -> !"general".equalsIgnoreCase(p.getIds()[0]))
            .thenComparing(IPlugin::getName, String.CASE_INSENSITIVE_ORDER));
    String[] labels = new String[plugins.size()];
    for (int i = 0; i < plugins.size(); i++) {
      labels[i] = plugins.get(i).getName();
    }
    return labels;
  }

  public static String codeFromDisplay(String display) {
    if (StringUtils.isEmpty(display)) {
      return GENERAL.getCode();
    }
    for (IPlugin plugin : pluginTypes()) {
      if (display.equals(plugin.getName()) || display.equalsIgnoreCase(plugin.getIds()[0])) {
        return plugin.getIds()[0];
      }
    }
    return fromLabel(display).getCode();
  }

  public static String displayFromCode(String code) {
    if (StringUtils.isEmpty(code)) {
      return GENERAL.getLabel();
    }
    for (IPlugin plugin : pluginTypes()) {
      if (code.equalsIgnoreCase(plugin.getIds()[0])) {
        return plugin.getName();
      }
    }
    NamingSchemeType type = fromCode(code);
    return type.getCode().equalsIgnoreCase(code) ? type.getLabel() : code;
  }

  private static List<IPlugin> pluginTypes() {
    try {
      return new ArrayList<>(
          PluginRegistry.getInstance().getPlugins(NamingSchemeTypePluginType.class));
    } catch (Exception e) {
      return List.of();
    }
  }

  public static NamingSchemeType fromLabel(String label) {
    if (StringUtils.isEmpty(label)) {
      return HOP_FIELD;
    }
    for (NamingSchemeType type : values()) {
      if (type.getLabel().equals(label) || type.code.equalsIgnoreCase(label.trim())) {
        return type;
      }
    }
    return HOP_FIELD;
  }
}
