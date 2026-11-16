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
  private static final String CONST_TRANSFORM_COUNT = "transformCount";
  private static final String CONST_HOP_COUNT = "hopCount";
  private static final String CONST_FILENAME = "filename";
  private static final String CONST_HAS_ORPHANED_TRANSFORMS = "hasOrphanedTransforms";
  private static final String CONST_NOTE_COUNT = "noteCount";
  private static final String CONST_HAS_NOTES = "hasNotes";
  private static final String CONST_IS_REFERENCED = "isReferenced";
  private static final String CONST_ACTION_COUNT = "actionCount";
  private static final String CONST_HAS_ORPHANED_ACTIONS = "hasOrphanedActions";
  private static final String CONST_DATABASE_TYPE = "databaseType";
  private static final String CONST_USERNAME = "username";
  private static final String CONST_PLUGIN_ID = "pluginId";
  private static final String CONST_COPIES = "copies";
  private static final String CONST_DISTRIBUTES = "distributes";
  private static final String CONST_IS_ORPHANED = "isOrphaned";
  private static final String CONST_ENABLED = "enabled";
  private static final String CONST_UNCONDITIONAL = "unconditional";
  private static final String CONST_EVALUATION = "evaluation";
  private static final String CONST_ATTRIBUTES = "attributes";
  private static final String CONST_ERROR_HANDLING = "errorHandling";
  private static final String CONST_TARGET_TRANSFORMS = "targetTransforms";
  private static final String CONST_TARGET_ACTIONS = "targetActions";
  private static final String CONST_DESCRIPTION = "description";
  private static final String CONST_PARAMETERS = "parameters";
  private static final String CONST_VARIABLES = "variables";
  private static final String CONST_HAS_DISABLED_HOPS = "hasDisabledHops";
  private static final String CONST_PASSWORD = "password";
  private static final String CONST_HAS_DEFAULT_NAME = "hasDefaultName";
  private static final String CONST_SECRET = "secret";
  private static final String CONST_CREDENTIAL = "credential";
  private static final String CONST_API_KEY = "apiKey";
  private static final String CONST_TOKEN = "token";

  private static final Map<RuleTarget, List<String>> TARGET_FIELDS = new HashMap<>();

  static {
    // Pipeline fields
    TARGET_FIELDS.put(
        RuleTarget.PIPELINE,
        Arrays.asList(
            "name",
            CONST_DESCRIPTION,
            CONST_TRANSFORM_COUNT,
            CONST_HOP_COUNT,
            CONST_FILENAME,
            "created",
            "modified",
            "createdUser",
            "modifiedUser",
            CONST_PARAMETERS,
            CONST_VARIABLES,
            "transforms",
            "hops",
            CONST_HAS_DISABLED_HOPS,
            CONST_HAS_ORPHANED_TRANSFORMS,
            CONST_NOTE_COUNT,
            CONST_HAS_NOTES,
            CONST_IS_REFERENCED));

    // Workflow fields
    TARGET_FIELDS.put(
        RuleTarget.WORKFLOW,
        Arrays.asList(
            "name",
            CONST_DESCRIPTION,
            CONST_ACTION_COUNT,
            CONST_HOP_COUNT,
            CONST_FILENAME,
            "created",
            "modified",
            "createdUser",
            "modifiedUser",
            CONST_PARAMETERS,
            CONST_VARIABLES,
            "actions",
            "hops",
            CONST_HAS_DISABLED_HOPS,
            CONST_HAS_ORPHANED_ACTIONS,
            CONST_NOTE_COUNT,
            CONST_HAS_NOTES,
            CONST_IS_REFERENCED));

    // Database Connection fields
    TARGET_FIELDS.put(
        RuleTarget.DATABASE_CONNECTION,
        Arrays.asList(
            "name",
            CONST_DESCRIPTION,
            CONST_DATABASE_TYPE,
            "hostname",
            "port",
            "databaseName",
            CONST_USERNAME,
            CONST_PASSWORD,
            "servername",
            "dataTablespace",
            "indexTablespace",
            CONST_ATTRIBUTES));

    // Transform fields
    TARGET_FIELDS.put(
        RuleTarget.TRANSFORM,
        Arrays.asList(
            "name",
            CONST_DESCRIPTION,
            CONST_PLUGIN_ID,
            CONST_COPIES,
            CONST_DISTRIBUTES,
            CONST_ERROR_HANDLING,
            CONST_TARGET_TRANSFORMS,
            "isDummy",
            CONST_IS_ORPHANED,
            "isBlockingTransform",
            CONST_HAS_DEFAULT_NAME,
            CONST_PASSWORD,
            CONST_SECRET,
            CONST_CREDENTIAL,
            CONST_API_KEY,
            CONST_TOKEN));

    // Action fields
    TARGET_FIELDS.put(
        RuleTarget.ACTION,
        Arrays.asList(
            "name",
            CONST_DESCRIPTION,
            CONST_PLUGIN_ID,
            CONST_ERROR_HANDLING,
            CONST_TARGET_ACTIONS,
            "isStart",
            CONST_IS_ORPHANED,
            CONST_HAS_DEFAULT_NAME,
            CONST_PASSWORD,
            CONST_SECRET,
            CONST_CREDENTIAL,
            CONST_API_KEY,
            CONST_TOKEN));

    // Hop fields (pipeline hops use from/toTransform, workflow hops use from/toAction)
    TARGET_FIELDS.put(
        RuleTarget.HOP,
        Arrays.asList(
            "name",
            CONST_ENABLED,
            "fromTransform",
            "toTransform",
            "fromAction",
            "toAction",
            CONST_UNCONDITIONAL,
            CONST_EVALUATION));
  }

  /** Get available fields for a target type */
  public static List<String> getFieldsForTarget(RuleTarget target) {
    return TARGET_FIELDS.getOrDefault(target, Arrays.asList());
  }

  /** Get a human-readable description for a field */
  public static String getFieldDescription(RuleTarget target, String field) {
    // This could be expanded to provide detailed descriptions
    switch (field) {
      case CONST_TRANSFORM_COUNT:
        return "Number of transforms in the pipeline";
      case CONST_ACTION_COUNT:
        return "Number of actions in the workflow";
      case CONST_HAS_DISABLED_HOPS:
        return "Whether the pipeline/workflow contains disabled hops";
      case CONST_HAS_ORPHANED_TRANSFORMS:
        return "Whether the pipeline contains transforms with no connections";
      case CONST_HAS_ORPHANED_ACTIONS:
        return "Whether the workflow contains actions with no connections";
      case CONST_HAS_DEFAULT_NAME:
        return "Whether the item uses a default generated name";
      case CONST_PASSWORD:
        return "Password field (checks all password-related fields in transforms/actions)";
      case CONST_SECRET:
        return "Secret field (checks all secret-related fields in transforms/actions)";
      case CONST_CREDENTIAL:
        return "Credential field (checks all credential-related fields in transforms/actions)";
      case CONST_USERNAME:
        return "Database connection username";
      case CONST_DATABASE_TYPE:
        return "Database type, the plugin id such as POSTGRESQL";
      case CONST_ATTRIBUTES:
        return "Connection attributes, extra options included";
      case CONST_ERROR_HANDLING:
        return "Whether errors go to an error hop (transform) or a failure hop (action)";
      case CONST_TARGET_TRANSFORMS:
        return "Names of the transforms this transform sends rows to";
      case CONST_TARGET_ACTIONS:
        return "Names of the actions this action leads to";
      case CONST_API_KEY:
        return "API key field";
      case CONST_TOKEN:
        return "Token field";
      case CONST_DESCRIPTION:
        return "Description field";
      case "name":
        return "Name field";
      default:
        return field.substring(0, 1).toUpperCase()
            + field.substring(1).replaceAll("([A-Z])", " $1");
    }
  }

  /** What kind of value a field the linter provides holds, as the rule executor reads it. */
  public enum ValueKind {
    /** A number that is always there, such as {@code transformCount}. */
    COUNT,
    /** True or false. */
    FLAG,
    /** A list or a map, such as {@code targetTransforms} or a connection's {@code attributes}. */
    LIST,
    /** Text, or null when not set. A connection's {@code port} is text. */
    TEXT
  }

  private static final Map<String, ValueKind> VALUE_KINDS = new HashMap<>();

  static {
    for (String field :
        List.of(
            CONST_TRANSFORM_COUNT,
            CONST_ACTION_COUNT,
            CONST_HOP_COUNT,
            CONST_NOTE_COUNT,
            CONST_COPIES)) {
      VALUE_KINDS.put(field, ValueKind.COUNT);
    }
    for (String field :
        List.of(
            CONST_HAS_DISABLED_HOPS,
            CONST_HAS_ORPHANED_TRANSFORMS,
            CONST_HAS_ORPHANED_ACTIONS,
            CONST_HAS_NOTES,
            CONST_IS_REFERENCED,
            CONST_DISTRIBUTES,
            CONST_ERROR_HANDLING,
            "isDummy",
            CONST_IS_ORPHANED,
            "isBlockingTransform",
            CONST_HAS_DEFAULT_NAME,
            "isStart",
            CONST_ENABLED,
            CONST_UNCONDITIONAL,
            CONST_EVALUATION)) {
      VALUE_KINDS.put(field, ValueKind.FLAG);
    }
    for (String field : List.of(CONST_TARGET_TRANSFORMS, CONST_TARGET_ACTIONS, CONST_ATTRIBUTES)) {
      VALUE_KINDS.put(field, ValueKind.LIST);
    }
    for (String field :
        List.of(
            "name",
            CONST_DESCRIPTION,
            CONST_FILENAME,
            CONST_PLUGIN_ID,
            CONST_DATABASE_TYPE,
            "hostname",
            "port",
            "databaseName",
            CONST_USERNAME,
            CONST_PASSWORD,
            "servername",
            "dataTablespace",
            "indexTablespace",
            "fromTransform",
            "toTransform",
            "fromAction",
            "toAction")) {
      VALUE_KINDS.put(field, ValueKind.TEXT);
    }
  }

  /**
   * The kind of value one of the target's own fields holds, or null when it is not known: a field
   * the target does not provide, or one whose value the linter does not read itself, such as the
   * secret fields of a transform.
   */
  public static ValueKind getValueKind(RuleTarget target, String field) {
    if (target == RuleTarget.TRANSFORM || target == RuleTarget.ACTION) {
      // Matched against every setting of the plugin by name, whatever its type.
      if (List.of(CONST_PASSWORD, CONST_SECRET, CONST_CREDENTIAL, CONST_API_KEY, CONST_TOKEN)
          .contains(field)) {
        return null;
      }
    }
    return getFieldsForTarget(target).contains(field) ? VALUE_KINDS.get(field) : null;
  }

  /** Get compatible conditions for a field type */
  public static List<RuleCondition> getCompatibleConditions(String field) {
    // Determine field type and return compatible conditions
    if (field.endsWith("Count") || field.equals("port") || field.equals(CONST_COPIES)) {
      // Numeric fields
      return Arrays.asList(
          RuleCondition.MAX_VALUE, RuleCondition.MIN_VALUE, RuleCondition.EXACT_VALUE);
    } else if (isFlagName(field)
        || field.equals(CONST_ENABLED)
        || field.equals(CONST_DISTRIBUTES)
        || field.equals(CONST_ERROR_HANDLING)
        || field.equals(CONST_UNCONDITIONAL)
        || field.equals(CONST_EVALUATION)) {
      // Boolean fields
      return Arrays.asList(RuleCondition.MUST_BE_TRUE, RuleCondition.MUST_BE_FALSE);
    } else if (field.equals("transforms")
        || field.equals("actions")
        || field.equals("hops")
        || field.equals(CONST_PARAMETERS)
        || field.equals(CONST_VARIABLES)
        || field.equals(CONST_ATTRIBUTES)
        || field.equals(CONST_TARGET_TRANSFORMS)
        || field.equals(CONST_TARGET_ACTIONS)) {
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
