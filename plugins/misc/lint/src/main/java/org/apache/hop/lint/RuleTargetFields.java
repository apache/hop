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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Defines available fields for each rule target type */
public class RuleTargetFields {

  private static final Map<RuleTarget, List<String>> TARGET_FIELDS = new HashMap<>();

  static {
    // Pipeline fields
    TARGET_FIELDS.put(
        RuleTarget.PIPELINE,
        Arrays.asList(
            "name",
            "description",
            "transformCount",
            "hopCount",
            "filename",
            "created",
            "modified",
            "createdUser",
            "modifiedUser",
            "parameters",
            "variables",
            "transforms",
            "hops",
            "hasDisabledHops",
            "hasOrphanedTransforms",
            "noteCount",
            "hasNotes",
            "isReferenced"));

    // Workflow fields
    TARGET_FIELDS.put(
        RuleTarget.WORKFLOW,
        Arrays.asList(
            "name",
            "description",
            "actionCount",
            "hopCount",
            "filename",
            "created",
            "modified",
            "createdUser",
            "modifiedUser",
            "parameters",
            "variables",
            "actions",
            "hops",
            "hasDisabledHops",
            "hasOrphanedActions",
            "noteCount",
            "hasNotes",
            "isReferenced"));

    // Database Connection fields
    TARGET_FIELDS.put(
        RuleTarget.DATABASE_CONNECTION,
        Arrays.asList(
            "name",
            "description",
            "databaseType",
            "hostname",
            "port",
            "databaseName",
            "username",
            "password",
            "servername",
            "dataTablespace",
            "indexTablespace",
            "attributes"));

    // Transform fields
    TARGET_FIELDS.put(
        RuleTarget.TRANSFORM,
        Arrays.asList(
            "name",
            "description",
            "pluginId",
            "copies",
            "distributes",
            "errorHandling",
            "targetTransforms",
            "isDummy",
            "isOrphaned",
            "isBlockingTransform",
            "hasDefaultName",
            "password",
            "secret",
            "credential",
            "apiKey",
            "token"));

    // Action fields
    TARGET_FIELDS.put(
        RuleTarget.ACTION,
        Arrays.asList(
            "name",
            "description",
            "pluginId",
            "errorHandling",
            "targetActions",
            "isStart",
            "isOrphaned",
            "hasDefaultName",
            "password",
            "secret",
            "credential",
            "apiKey",
            "token"));

    // Hop fields (pipeline hops use from/toTransform, workflow hops use from/toAction)
    TARGET_FIELDS.put(
        RuleTarget.HOP,
        Arrays.asList(
            "name",
            "enabled",
            "fromTransform",
            "toTransform",
            "fromAction",
            "toAction",
            "unconditional",
            "evaluation"));
  }

  /** Get available fields for a target type */
  public static List<String> getFieldsForTarget(RuleTarget target) {
    return TARGET_FIELDS.getOrDefault(target, Arrays.asList());
  }

  /** Get a human-readable description for a field */
  public static String getFieldDescription(RuleTarget target, String field) {
    // This could be expanded to provide detailed descriptions
    switch (field) {
      case "transformCount":
        return "Number of transforms in the pipeline";
      case "actionCount":
        return "Number of actions in the workflow";
      case "hasDisabledHops":
        return "Whether the pipeline/workflow contains disabled hops";
      case "hasOrphanedTransforms":
        return "Whether the pipeline contains transforms with no connections";
      case "hasOrphanedActions":
        return "Whether the workflow contains actions with no connections";
      case "hasDefaultName":
        return "Whether the item uses a default generated name";
      case "password":
        return "Password field (checks all password-related fields in transforms/actions)";
      case "secret":
        return "Secret field (checks all secret-related fields in transforms/actions)";
      case "credential":
        return "Credential field (checks all credential-related fields in transforms/actions)";
      case "username":
        return "Database connection username";
      case "databaseType":
        return "Database type, the plugin id such as POSTGRESQL";
      case "attributes":
        return "Connection attributes, extra options included";
      case "errorHandling":
        return "Whether errors go to an error hop (transform) or a failure hop (action)";
      case "targetTransforms":
        return "Names of the transforms this transform sends rows to";
      case "targetActions":
        return "Names of the actions this action leads to";
      case "apiKey":
        return "API key field";
      case "token":
        return "Token field";
      case "description":
        return "Description field";
      case "name":
        return "Name field";
      default:
        return field.substring(0, 1).toUpperCase()
            + field.substring(1).replaceAll("([A-Z])", " $1");
    }
  }

  /** Get compatible conditions for a field type */
  public static List<RuleCondition> getCompatibleConditions(String field) {
    // Determine field type and return compatible conditions
    if (field.endsWith("Count") || field.equals("port") || field.equals("copies")) {
      // Numeric fields
      return Arrays.asList(
          RuleCondition.MAX_VALUE, RuleCondition.MIN_VALUE, RuleCondition.EXACT_VALUE);
    } else if (isFlagName(field)
        || field.equals("enabled")
        || field.equals("distributes")
        || field.equals("errorHandling")
        || field.equals("unconditional")
        || field.equals("evaluation")) {
      // Boolean fields
      return Arrays.asList(RuleCondition.MUST_BE_TRUE, RuleCondition.MUST_BE_FALSE);
    } else if (field.equals("transforms")
        || field.equals("actions")
        || field.equals("hops")
        || field.equals("parameters")
        || field.equals("variables")
        || field.equals("attributes")
        || field.equals("targetTransforms")
        || field.equals("targetActions")) {
      // Collection fields
      return Arrays.asList(
          RuleCondition.NOT_EMPTY_COLLECTION,
          RuleCondition.MAX_COLLECTION_SIZE,
          RuleCondition.MIN_COLLECTION_SIZE);
    } else {
      // String fields
      return Arrays.asList(
          RuleCondition.NOT_EMPTY,
          RuleCondition.NOT_NULL,
          RuleCondition.IS_EMPTY,
          RuleCondition.NO_HARDCODED,
          RuleCondition.MATCHES_PATTERN,
          RuleCondition.NOT_MATCHES_PATTERN,
          RuleCondition.CONTAINS,
          RuleCondition.NOT_CONTAINS,
          RuleCondition.STARTS_WITH,
          RuleCondition.ENDS_WITH);
    }
  }

  /**
   * {@code hasNotes}, {@code isDummy}: a yes-or-no question, but not {@code issuer} or {@code
   * hash}.
   */
  private static boolean isFlagName(String field) {
    for (String prefix : new String[] {"has", "is"}) {
      if (field.length() > prefix.length()
          && field.startsWith(prefix)
          && Character.isUpperCase(field.charAt(prefix.length()))) {
        return true;
      }
    }
    return false;
  }

  /**
   * The fields the rule editor offers, with the one the rule already reads among them.
   *
   * <p>A rule may read a field the list does not know: a transform's own setting such as a Table
   * Input's {@code sql}, or any field of a metadata object. Leaving it out opened such a rule with
   * no field selected, and the editor then refused to save it at all, even for a severity change.
   */
  public static List<String> getFieldChoices(RuleTarget target, String currentField) {
    List<String> fields = new ArrayList<>(getFieldsForTarget(target));
    if (currentField != null && !currentField.isEmpty() && !fields.contains(currentField)) {
      fields.add(currentField);
    }
    return fields;
  }

  /**
   * The conditions the rule editor offers for a field, with the one the rule already uses among
   * them, for the same reason as {@link #getFieldChoices}: a rule from a pack is not wrong just
   * because the editor would not have suggested its condition.
   */
  public static List<RuleCondition> getConditionChoices(
      String field, RuleCondition currentCondition) {
    List<RuleCondition> conditions =
        new ArrayList<>(getCompatibleConditions(field == null ? "" : field));
    if (currentCondition != null && !conditions.contains(currentCondition)) {
      conditions.add(currentCondition);
    }
    return conditions;
  }
}
