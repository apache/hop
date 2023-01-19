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

package org.apache.hop.core.logging;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

public enum LogLevel implements IEnumHasCodeAndDescription {
  NOTHING(0, "Nothing", "LogWriter.Level.Nothing.LongDesc"),
  ERROR(1, "Error", "LogWriter.Level.Error.LongDesc"),
  MINIMAL(2, "Minimal", "LogWriter.Level.Minimal.LongDesc"),
  BASIC(3, "Basic", "LogWriter.Level.Basic.LongDesc"),
  DETAILED(4, "Detailed", "LogWriter.Level.Detailed.LongDesc"),
  DEBUG(5, "Debug", "LogWriter.Level.Debug.LongDesc"),
  ROWLEVEL(6, "Rowlevel", "LogWriter.Level.Rowlevel.LongDesc");

  private final int level;
  private final String code;
  private final String description;

  private LogLevel(int level, String code, String description) {
    this.level = level;
    this.code = code;
    this.description = BaseMessages.getString(LogLevel.class, description);
  }

  public int getLevel() {
    return level;
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public String getDescription() {
    return description;
  }
  
  /**
   * Return the log level for a certain log level code
   *
   * @param code the code to look for
   * @return the log level or BASIC if nothing matches.
   */
  @Deprecated
  public static LogLevel getLogLevelForCode(String code) {
    return IEnumHasCode.lookupCode(LogLevel.class, code, LogLevel.BASIC);
  }
  
  /**
   * Return the log level for a certain log level code
   *
   * @param code the code to look for
   * @return the log level or BASIC if nothing matches.
   */  
  public static LogLevel lookupCode(final String code) {
    return IEnumHasCode.lookupCode(LogLevel.class, code, LogLevel.BASIC);
  }
  
  /**
   * Return the log level for a certain log level description
   *
   * @param description the description to look for
   * @return the log level or BASIC if nothing matches.
   */  
  public static LogLevel lookupDescription(final String description) {
    return IEnumHasCodeAndDescription.lookupDescription(LogLevel.class, description, LogLevel.BASIC);
  }

  /**
   * @param filterLogLevel the filter log level
   * @return true if the log level is visible compared to the filter log level specified
   */
  public boolean isVisible(LogLevel filterLogLevel) {
    return getLevel() <= filterLogLevel.getLevel();
  }

  /** @return true if this level is Error or lower */
  public boolean isError() {
    return this == ERROR;
  }

  /** @return True if this level is Minimal or lower (which is nothing) */
  public boolean isNothing() {
    return this.level >= NOTHING.level;
  }

  /** @return True if this level is Minimal */
  public boolean isMinimal() {
    return this.level >= MINIMAL.level;
  }

  /** @return True if this level is Basic */
  public boolean isBasic() {
    return this.level >= BASIC.level;
  }

  /** @return True if this level is Detailed */
  public boolean isDetailed() {
    return this.level >= DETAILED.level;
  }

  /** @return True if this level is Debug */
  public boolean isDebug() {
    return this.level >= DEBUG.level;
  }

  /** @return True if this level is Row level */
  public boolean isRowlevel() {
    return this.level >= ROWLEVEL.level;
  }

  /** 
   * Return an array of log level descriptions, sorted by level (0==Nothing, 6=Row Level)
   * @return An array of log level descriptions
   */
  public static String[] getLogLevelDescriptions() {
    return IEnumHasCodeAndDescription.getDescriptions(LogLevel.class);
  }

  /** @return An array of log level codes, sorted by level (0==Nothing, 6=Row Level) */
  public static String[] logLogLevelCodes() {
    return IEnumHasCode.getCodes(LogLevel.class);
  }
}
