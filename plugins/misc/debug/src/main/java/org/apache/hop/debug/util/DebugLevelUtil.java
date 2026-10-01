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

package org.apache.hop.debug.util;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Condition;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.debug.action.ActionDebugLevel;
import org.apache.hop.debug.transform.TransformDebugLevel;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

public class DebugLevelUtil {

  public static void storeTransformDebugLevel(
      Map<String, String> debugGroupAttributesMap,
      String transformName,
      TransformDebugLevel debugLevel)
      throws HopValueException, UnsupportedEncodingException {
    debugGroupAttributesMap.put(
        transformName + " : " + Defaults.TRANSFORM_ATTR_LOGLEVEL, debugLevel.getLogLevel());
    debugGroupAttributesMap.put(
        transformName + " : " + Defaults.TRANSFORM_ATTR_START_ROW,
        Integer.toString(debugLevel.getStartRow()));
    debugGroupAttributesMap.put(
        transformName + " : " + Defaults.TRANSFORM_ATTR_END_ROW,
        Integer.toString(debugLevel.getEndRow()));

    String conditionXmlString =
        Base64.getEncoder()
            .encodeToString(debugLevel.getCondition().getXml().getBytes(StandardCharsets.UTF_8));
    debugGroupAttributesMap.put(
        transformName + " : " + Defaults.TRANSFORM_ATTR_CONDITION, conditionXmlString);
  }

  public static TransformDebugLevel getTransformDebugLevel(
      Map<String, String> debugGroupAttributesMap, String transformName)
      throws UnsupportedEncodingException, HopXmlException {

    String logLevelCode =
        debugGroupAttributesMap.get(transformName + " : " + Defaults.TRANSFORM_ATTR_LOGLEVEL);
    String startRowString =
        debugGroupAttributesMap.get(transformName + " : " + Defaults.TRANSFORM_ATTR_START_ROW);
    String endRowString =
        debugGroupAttributesMap.get(transformName + " : " + Defaults.TRANSFORM_ATTR_END_ROW);
    String conditionString =
        debugGroupAttributesMap.get(transformName + " : " + Defaults.TRANSFORM_ATTR_CONDITION);

    if (StringUtils.isEmpty(logLevelCode)) {
      // Nothing to load
      //
      return null;
    }

    TransformDebugLevel debugLevel = new TransformDebugLevel();
    debugLevel.setLogLevel(logLevelCode);
    debugLevel.setStartRow(Const.toInt(startRowString, -1));
    debugLevel.setEndRow(Const.toInt(endRowString, -1));

    if (StringUtils.isNotEmpty(conditionString)) {
      String conditionXml =
          new String(Base64.getDecoder().decode(conditionString), StandardCharsets.UTF_8);
      debugLevel.setCondition(new Condition(conditionXml));
    }
    return debugLevel;
  }

  /**
   * Does the given transform read rows from other transforms, or does it only generate them? Custom
   * logging hooks into the read event of the former and into the write event of the latter, so both
   * the runtime and the dialog use this to know which rows the start/end row range counts and which
   * fields a condition can be built on.
   *
   * @param pipelineMeta the pipeline the transform lives in
   * @param transformName the name of the transform
   * @return true if the transform has one or more (info) input transforms
   */
  public static boolean isReadingRows(PipelineMeta pipelineMeta, String transformName) {
    return isReadingRows(pipelineMeta, pipelineMeta.findTransform(transformName));
  }

  /**
   * @see #isReadingRows(PipelineMeta, String)
   * @param pipelineMeta the pipeline the transform lives in
   * @param transformMeta the transform
   * @return true if the transform has one or more (info) input transforms
   */
  public static boolean isReadingRows(PipelineMeta pipelineMeta, TransformMeta transformMeta) {
    return transformMeta != null
        && !pipelineMeta.findPreviousTransforms(transformMeta, true).isEmpty();
  }

  /**
   * Get the fields a custom logging condition is evaluated against for the given transform: the
   * incoming fields for a transform that reads rows, its own output fields for a transform that
   * only generates rows.
   *
   * @param variables the variables to resolve the metadata with
   * @param pipelineMeta the pipeline the transform lives in
   * @param transformMeta the transform
   * @return the fields to build a condition on, never null
   */
  public static IRowMeta getConditionFields(
      IVariables variables, PipelineMeta pipelineMeta, TransformMeta transformMeta)
      throws HopTransformException {
    IRowMeta rowMeta =
        isReadingRows(pipelineMeta, transformMeta)
            ? pipelineMeta.getPrevTransformFields(variables, transformMeta)
            : pipelineMeta.getTransformFields(variables, transformMeta);
    return rowMeta == null ? new RowMeta() : rowMeta;
  }

  /**
   * Get the attributes group custom logging is stored in, creating it if the pipeline or workflow
   * doesn't have one yet.
   *
   * @param attributesMap the attributes map of a pipeline or workflow
   * @return the debug group attributes, never null
   */
  public static Map<String, String> getOrCreateDebugGroup(
      Map<String, Map<String, String>> attributesMap) {
    return attributesMap.computeIfAbsent(Defaults.DEBUG_GROUP, key -> new HashMap<>());
  }

  public static void clearDebugLevel(
      Map<String, String> debugGroupAttributesMap, String transformName) {
    if (debugGroupAttributesMap == null) {
      // Nothing was ever configured on this pipeline or workflow, so nothing to clear.
      //
      return;
    }
    debugGroupAttributesMap.remove(transformName + " : " + Defaults.TRANSFORM_ATTR_LOGLEVEL);
    debugGroupAttributesMap.remove(transformName + " : " + Defaults.TRANSFORM_ATTR_START_ROW);
    debugGroupAttributesMap.remove(transformName + " : " + Defaults.TRANSFORM_ATTR_END_ROW);
    debugGroupAttributesMap.remove(transformName + " : " + Defaults.TRANSFORM_ATTR_CONDITION);

    debugGroupAttributesMap.remove(transformName + " : " + Defaults.ACTION_ATTR_LOGLEVEL);
    debugGroupAttributesMap.remove(transformName + " : " + Defaults.ACTION_ATTR_LOG_RESULT);
    debugGroupAttributesMap.remove(transformName + " : " + Defaults.ACTION_ATTR_LOG_VARIABLES);
    debugGroupAttributesMap.remove(transformName + " : " + Defaults.ACTION_ATTR_LOG_RESULT_ROWS);
    debugGroupAttributesMap.remove(transformName + " : " + Defaults.ACTION_ATTR_LOG_RESULT_FILES);
  }

  public static void storeActionDebugLevel(
      Map<String, String> debugGroupAttributesMap, String entryName, ActionDebugLevel debugLevel) {
    debugGroupAttributesMap.put(
        entryName + " : " + Defaults.ACTION_ATTR_LOGLEVEL, debugLevel.getLogLevel());
    debugGroupAttributesMap.put(
        entryName + " : " + Defaults.ACTION_ATTR_LOG_RESULT,
        debugLevel.isLoggingResult() ? "Y" : "N");
    debugGroupAttributesMap.put(
        entryName + " : " + Defaults.ACTION_ATTR_LOG_VARIABLES,
        debugLevel.isLoggingVariables() ? "Y" : "N");
    debugGroupAttributesMap.put(
        entryName + " : " + Defaults.ACTION_ATTR_LOG_RESULT_ROWS,
        debugLevel.isLoggingResultRows() ? "Y" : "N");
    debugGroupAttributesMap.put(
        entryName + " : " + Defaults.ACTION_ATTR_LOG_RESULT_FILES,
        debugLevel.isLoggingResultFiles() ? "Y" : "N");
  }

  public static ActionDebugLevel getActionDebugLevel(
      Map<String, String> debugGroupAttributesMap, String entryName) {

    String logLevelCode =
        debugGroupAttributesMap.get(entryName + " : " + Defaults.ACTION_ATTR_LOGLEVEL);
    boolean loggingResult =
        "Y"
            .equalsIgnoreCase(
                debugGroupAttributesMap.get(entryName + " : " + Defaults.ACTION_ATTR_LOG_RESULT));
    boolean loggingVariables =
        "Y"
            .equalsIgnoreCase(
                debugGroupAttributesMap.get(
                    entryName + " : " + Defaults.ACTION_ATTR_LOG_VARIABLES));
    boolean loggingResultRows =
        "Y"
            .equalsIgnoreCase(
                debugGroupAttributesMap.get(
                    entryName + " : " + Defaults.ACTION_ATTR_LOG_RESULT_ROWS));
    boolean loggingResultFiles =
        "Y"
            .equalsIgnoreCase(
                debugGroupAttributesMap.get(
                    entryName + " : " + Defaults.ACTION_ATTR_LOG_RESULT_FILES));

    if (StringUtils.isEmpty(logLevelCode)) {
      // Nothing to load
      //
      return null;
    }

    ActionDebugLevel debugLevel = new ActionDebugLevel();
    debugLevel.setLogLevel(logLevelCode);
    debugLevel.setLoggingResult(loggingResult);
    debugLevel.setLoggingVariables(loggingVariables);
    debugLevel.setLoggingResultRows(loggingResultRows);
    debugLevel.setLoggingResultFiles(loggingResultFiles);

    return debugLevel;
  }

  /**
   * Resolve a log level specification (code, description, or variable expression) to a {@link
   * LogLevel}. Variables are expanded first, then matched against codes (preferred) and
   * descriptions. Falls back to {@link LogLevel#BASIC} when unresolved or unrecognized (same
   * default as {@link LogLevel#lookupCode(String)}).
   *
   * @param variables variable space used to resolve expressions (may be null)
   * @param logLevelSpec stored code, description, or variable expression
   * @return resolved log level
   */
  public static LogLevel resolveLogLevel(IVariables variables, String logLevelSpec) {
    String resolved =
        variables != null
            ? variables.resolve(Const.NVL(logLevelSpec, ""))
            : Const.NVL(logLevelSpec, "");
    if (StringUtils.isEmpty(resolved)) {
      return LogLevel.BASIC;
    }
    // Prefer exact code match so unknown values do not silently become BASIC via lookupCode
    for (LogLevel level : LogLevel.values()) {
      if (level.getCode().equalsIgnoreCase(resolved)) {
        return level;
      }
    }
    for (LogLevel level : LogLevel.values()) {
      if (level.getDescription().equalsIgnoreCase(resolved)) {
        return level;
      }
    }
    return LogLevel.BASIC;
  }

  /**
   * Map a stored log level code to its description for UI display. If the value is not a known code
   * (e.g. a variable expression), return it unchanged.
   *
   * @param logLevelSpec stored code or variable expression
   * @return description for a known code, otherwise the original string
   */
  public static String logLevelCodeToDisplay(String logLevelSpec) {
    if (StringUtils.isEmpty(logLevelSpec)) {
      return Const.NVL(logLevelSpec, "");
    }
    for (LogLevel level : LogLevel.values()) {
      if (level.getCode().equalsIgnoreCase(logLevelSpec)) {
        return level.getDescription();
      }
    }
    return logLevelSpec;
  }

  /**
   * Map dialog text to a value suitable for storage. If the text matches a log level description,
   * store the stable code; otherwise store the text as-is (variable or free-form).
   *
   * @param displayText combo text from the dialog
   * @return code for a known description, otherwise the original text
   */
  public static String logLevelDisplayToCode(String displayText) {
    if (StringUtils.isEmpty(displayText)) {
      return displayText;
    }
    for (LogLevel level : LogLevel.values()) {
      if (level.getDescription().equalsIgnoreCase(displayText)) {
        return level.getCode();
      }
    }
    // Already a code?
    for (LogLevel level : LogLevel.values()) {
      if (level.getCode().equalsIgnoreCase(displayText)) {
        return level.getCode();
      }
    }
    return displayText;
  }

  public static String getDurationHMS(double seconds) {
    int day = (int) TimeUnit.SECONDS.toDays((long) seconds);
    long hours = TimeUnit.SECONDS.toHours((long) seconds) - (day * 24);
    long minute =
        TimeUnit.SECONDS.toMinutes((long) seconds)
            - (TimeUnit.SECONDS.toHours((long) seconds) * 60);
    long second =
        TimeUnit.SECONDS.toSeconds((long) seconds)
            - (TimeUnit.SECONDS.toMinutes((long) seconds) * 60);
    long ms = (long) ((seconds - ((long) seconds)) * 1000);

    StringBuilder hms = new StringBuilder();
    if (day > 0) {
      hms.append(day + "d ");
    }
    if (day > 0 || hours > 0) {
      hms.append(hours + "h ");
    }
    if (day > 0 || hours > 0 || minute > 0) {
      hms.append(String.format("%2d", minute) + "' ");
    }
    hms.append(String.format("%2d", second) + ".");
    hms.append(String.format("%03d", ms) + "\"");

    return hms.toString();
  }
}
