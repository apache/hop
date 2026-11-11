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

package org.apache.hop.workflow.actions.mssqlbulkload;

import java.util.List;
import java.util.regex.Pattern;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/** The CODEPAGE option of the BULK INSERT statement. */
public enum MssqlBulkLoadCodePage implements IEnumHasCodeAndDescription {
  ACP("ACP", null),
  OEM("OEM", null),
  RAW("RAW", null),
  /** The value of Specific codepage is sent. "Specific" is the value stored before #9167. */
  SPECIFIC("Specific", "ActionMssqlBulkLoad.CodePage.Specific");

  private static final Class<?> PKG = ActionMssqlBulkLoad.class;

  /**
   * Older dialogs stored the translated label of Specific: fr_FR, es_AR and pt_BR, it_IT, ja_JP,
   * zh_CN.
   */
  private static final List<String> LEGACY_SPECIFIC_LABELS =
      List.of("Spécifique", "Específico", "Specifico", "任意", "自定义");

  /** What SQL Server accepts as CODEPAGE: a code page number, ACP, OEM or RAW. */
  private static final Pattern VALID_CODE_PAGE =
      Pattern.compile("\\d+|ACP|OEM|RAW", Pattern.CASE_INSENSITIVE);

  private final String code;
  private final String descriptionKey;

  MssqlBulkLoadCodePage(String code, String descriptionKey) {
    this.code = code;
    this.descriptionKey = descriptionKey;
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public String getDescription() {
    return descriptionKey == null ? code : BaseMessages.getString(PKG, descriptionKey);
  }

  /**
   * Find the code page for a stored value or a label shown in the dialog.
   *
   * @return the code page, or null when the value is empty or unknown
   */
  public static MssqlBulkLoadCodePage lookup(String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }
    for (MssqlBulkLoadCodePage codePage : values()) {
      if (codePage.code.equalsIgnoreCase(value)
          || codePage.name().equalsIgnoreCase(value)
          || codePage.getDescription().equalsIgnoreCase(value)) {
        return codePage;
      }
    }
    if (LEGACY_SPECIFIC_LABELS.contains(value)) {
      return SPECIFIC;
    }
    return null;
  }

  /** True when SQL Server accepts the value as CODEPAGE, so it can go into the statement. */
  public static boolean isValidCodePage(String value) {
    return value != null && VALID_CODE_PAGE.matcher(value.trim()).matches();
  }
}
