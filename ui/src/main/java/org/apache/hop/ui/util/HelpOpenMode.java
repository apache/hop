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

package org.apache.hop.ui.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;

/** Where Hop GUI should open documentation / help URLs. */
public enum HelpOpenMode {
  /** System (or RAP) browser. */
  BROWSER,
  /** HTML tab in the File Explorer perspective. */
  TAB,
  /** Modeless dialog parented to the current context shell. */
  DIALOG;

  private static final Class<?> PKG = HelpOpenMode.class;

  public String getLabel() {
    return BaseMessages.getString(PKG, "HelpOpenMode." + name());
  }

  @JsonValue
  public String toConfigValue() {
    return name();
  }

  /**
   * Resolve a combo label or enum name. Unknown values fall back to {@link #BROWSER}.
   *
   * @param label translated combo text, enum name, or {@code null}
   * @return matching mode, never {@code null}
   */
  public static HelpOpenMode fromLabel(String label) {
    if (Utils.isEmpty(label)) {
      return BROWSER;
    }
    String trimmed = label.trim();
    for (HelpOpenMode mode : values()) {
      if (mode.name().equalsIgnoreCase(trimmed) || mode.getLabel().equalsIgnoreCase(trimmed)) {
        return mode;
      }
    }
    return BROWSER;
  }

  /**
   * Parse a hop-config / CLI value. Accepts enum names and the legacy boolean {@code true} (tab
   * mode). Unknown values fall back to {@link #BROWSER}.
   *
   * @param value stored string or {@code null}
   * @return matching mode, never {@code null}
   */
  @JsonCreator
  public static HelpOpenMode fromConfigValue(String value) {
    if (Utils.isEmpty(value)) {
      return BROWSER;
    }
    String trimmed = value.trim();
    if ("true".equalsIgnoreCase(trimmed)) {
      return TAB;
    }
    if ("false".equalsIgnoreCase(trimmed)) {
      return BROWSER;
    }
    return fromLabel(trimmed);
  }
}
