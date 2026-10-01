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
package org.apache.hop.lint;

/** Enumeration of condition types that can be applied to rules */
public enum RuleCondition {
  // Numeric conditions
  MAX_VALUE("Maximum Value", "Field value must not exceed specified maximum", true),
  MIN_VALUE("Minimum Value", "Field value must meet specified minimum", true),
  EXACT_VALUE("Exact Value", "Field value must match exactly", true),

  // String conditions
  NOT_EMPTY("Not Empty", "Field must not be empty or null", false),
  NOT_NULL("Not Null", "Field must not be null", false),
  NO_HARDCODED("No Hardcoded Values", "Field must use variables, not hardcoded values", false),
  MATCHES_PATTERN("Matches Pattern", "Field must match specified regex pattern", true),
  NOT_MATCHES_PATTERN(
      "Does Not Match Pattern", "Field must not match specified regex pattern", true),
  CONTAINS("Contains", "Field must contain specified text", true),
  NOT_CONTAINS("Does Not Contain", "Field must not contain specified text", true),
  STARTS_WITH("Starts With", "Field must start with specified text", true),
  ENDS_WITH("Ends With", "Field must end with specified text", true),

  // Boolean conditions
  MUST_BE_TRUE("Must Be True", "Field must be true", false),
  MUST_BE_FALSE("Must Be False", "Field must be false", false),

  // Collection conditions
  NOT_EMPTY_COLLECTION("Collection Not Empty", "Collection must contain at least one item", false),
  MAX_COLLECTION_SIZE("Maximum Collection Size", "Collection size must not exceed maximum", true),
  MIN_COLLECTION_SIZE("Minimum Collection Size", "Collection size must meet minimum", true);

  // NO_DISABLED_ITEMS, NO_DEFAULT_NAMES, HAS_DESCRIPTION and VALID_CONNECTIONS used to sit here.
  // None was ever implemented: they fell through to the default branch and silently reported
  // "no findings", while still appearing in the rule-builder dropdown. The first three duplicate
  // conditions that already work against the boolean helper fields:
  //
  //   NO_DISABLED_ITEMS  ->  hasDisabledHops   MUST_BE_FALSE
  //   NO_DEFAULT_NAMES   ->  hasDefaultName    MUST_BE_FALSE
  //   HAS_DESCRIPTION    ->  description       NOT_EMPTY
  //
  // VALID_CONNECTIONS had no definition at all. Removed rather than implemented: two ways to
  // express one check is a maintenance and documentation cost with no benefit.

  private final String displayName;
  private final String description;
  private final boolean requiresValue;

  RuleCondition(String displayName, String description, boolean requiresValue) {
    this.displayName = displayName;
    this.description = description;
    this.requiresValue = requiresValue;
  }

  public String getDisplayName() {
    return displayName;
  }

  public String getDescription() {
    return description;
  }

  public boolean requiresValue() {
    return requiresValue;
  }

  @Override
  public String toString() {
    return displayName;
  }
}
