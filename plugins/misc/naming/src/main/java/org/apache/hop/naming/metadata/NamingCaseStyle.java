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

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.i18n.BaseMessages;

/** Case transformation applied when joining name words. */
public enum NamingCaseStyle {
  AS_IS("as-is", "NamingCaseStyle.AsIs"),
  LOWER("lower", "NamingCaseStyle.Lower"),
  UPPER("upper", "NamingCaseStyle.Upper"),
  CAMEL("camel", "NamingCaseStyle.Camel"),
  PASCAL("pascal", "NamingCaseStyle.Pascal");

  private static final Class<?> PKG = NamingScheme.class;

  private final String code;
  private final String i18nKey;

  NamingCaseStyle(String code, String i18nKey) {
    this.code = code;
    this.i18nKey = i18nKey;
  }

  public String getCode() {
    return code;
  }

  public String getLabel() {
    return BaseMessages.getString(PKG, i18nKey);
  }

  public static NamingCaseStyle fromCode(String code) {
    if (StringUtils.isEmpty(code)) {
      return LOWER;
    }
    for (NamingCaseStyle style : values()) {
      if (style.code.equalsIgnoreCase(code.trim())) {
        return style;
      }
    }
    return LOWER;
  }

  public static String[] getLabels() {
    NamingCaseStyle[] values = values();
    String[] labels = new String[values.length];
    for (int i = 0; i < values.length; i++) {
      labels[i] = values[i].getLabel();
    }
    return labels;
  }

  public static NamingCaseStyle fromLabel(String label) {
    if (StringUtils.isEmpty(label)) {
      return LOWER;
    }
    for (NamingCaseStyle style : values()) {
      if (style.getLabel().equals(label) || style.code.equalsIgnoreCase(label.trim())) {
        return style;
      }
    }
    return LOWER;
  }
}
