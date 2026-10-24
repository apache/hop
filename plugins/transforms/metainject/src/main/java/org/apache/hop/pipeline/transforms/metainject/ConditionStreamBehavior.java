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

package org.apache.hop.pipeline.transforms.metainject;

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/**
 * Controls under which stream conditions the injected template pipeline is executed.
 *
 * <p>This replaces the legacy boolean {@code allowEmptyStreamOnExecution}: {@link #ALL_NON_EMPTY}
 * and {@link #AT_LEAST_ONE_NON_EMPTY} preserve the old {@code false}/{@code true} semantics, while
 * {@link #ALWAYS} adds the ability to run even when every input stream is empty (for templates that
 * can run from constant mappings alone).
 */
public enum ConditionStreamBehavior implements IEnumHasCodeAndDescription {
  /** Every input stream must produce at least one row. Equivalent to legacy {@code false}. */
  ALL_NON_EMPTY("all-non-empty", "ConditionStreamBehavior.AllNonEmpty.Description"),

  /** Run when at least one stream is non-empty even if others are empty. Legacy {@code true}. */
  AT_LEAST_ONE_NON_EMPTY(
      "at-least-one-non-empty", "ConditionStreamBehavior.AtLeastOneNonEmpty.Description"),

  /** Run regardless of the stream contents, even when every input stream is empty. */
  ALWAYS("always", "ConditionStreamBehavior.Always.Description");

  private static final Class<?> PKG = ConditionStreamBehavior.class;

  private final String code;
  private final String descriptionKey;

  ConditionStreamBehavior(String code, String descriptionKey) {
    this.code = code;
    this.descriptionKey = descriptionKey;
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(PKG, descriptionKey);
  }

  /**
   * @return {@code true} when every stream is required to be non-empty ({@link #ALL_NON_EMPTY}).
   */
  public boolean requiresAllNonEmpty() {
    return this == ALL_NON_EMPTY;
  }

  /**
   * @return {@code true} when execution is allowed even if every stream is empty ({@link #ALWAYS}).
   */
  public boolean allowsAllEmpty() {
    return this == ALWAYS;
  }

  /**
   * Maps the legacy {@code allow_empty_stream_on_execution} boolean value onto this enum. Before
   * this enum existed the transform stored a plain boolean (as {@code Y}/{@code N} in XML and
   * {@code true}/{@code false} in JSON metadata / injection values).
   *
   * <p>A truthy value ({@code Y}/{@code yes}/{@code true}/{@code t}, per {@link Const#toBoolean})
   * meant "at least one non-empty stream was required" and maps to {@link #AT_LEAST_ONE_NON_EMPTY};
   * anything else maps to {@link #ALL_NON_EMPTY}.
   *
   * @param legacyValue the raw legacy boolean value, or {@code null}
   * @return the equivalent enum constant, never {@code null}
   */
  public static ConditionStreamBehavior fromLegacyBoolean(String legacyValue) {
    return Const.toBoolean(legacyValue)
        ? ConditionStreamBehavior.AT_LEAST_ONE_NON_EMPTY
        : ConditionStreamBehavior.ALL_NON_EMPTY;
  }

  /**
   * Returns {@code true} when the given value is a legacy boolean token for the {@code
   * allow_empty_stream_on_execution} setting: {@code Y}/{@code N}/{@code yes}/{@code no}/{@code
   * true}/{@code false}/{@code t}/{@code f} (case-insensitive). This deliberately excludes the
   * current enum codes ({@code all-non-empty}, {@code at-least-one-non-empty}, {@code always}) so
   * they are never mistaken for a legacy value.
   *
   * @param value the value to test, or {@code null}
   * @return {@code true} when the value is a legacy boolean token
   */
  public static boolean isLegacyBoolean(String value) {
    if (value == null) {
      return false;
    }
    // Case-insensitive comparison is locale-safe (matching Const.toBoolean), so an uppercase
    // "TRUE" still matches even when the JVM default locale (e.g. Turkish) mangles lowercasing.
    return "y".equalsIgnoreCase(value)
        || "n".equalsIgnoreCase(value)
        || "yes".equalsIgnoreCase(value)
        || "no".equalsIgnoreCase(value)
        || "true".equalsIgnoreCase(value)
        || "false".equalsIgnoreCase(value)
        || "t".equalsIgnoreCase(value)
        || "f".equalsIgnoreCase(value);
  }

  public static String[] getDescriptions() {
    return IEnumHasCodeAndDescription.getDescriptions(ConditionStreamBehavior.class);
  }

  public static ConditionStreamBehavior lookupCode(String code) {
    return IEnumHasCode.lookupCode(
        ConditionStreamBehavior.class, code, ConditionStreamBehavior.ALL_NON_EMPTY);
  }

  public static ConditionStreamBehavior lookupDescription(String description) {
    return IEnumHasCodeAndDescription.lookupDescription(
        ConditionStreamBehavior.class, description, ConditionStreamBehavior.ALL_NON_EMPTY);
  }
}
