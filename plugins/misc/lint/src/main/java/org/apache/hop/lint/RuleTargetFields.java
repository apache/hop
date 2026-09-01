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
            "attributes",
            "connectionPoolingParameters",
            "partitioningInformation",
            "maxConnectionPoolSize",
            "initialPoolSize"));

    // Transform fields
    TARGET_FIELDS.put(
        RuleTarget.TRANSFORM,
        Arrays.asList(
            "name",
            "description",
            "pluginId",
            "copies",
            "distributes",
            "location",
            "selected",
            "errorHandling",
            "targetTransforms",
            "isStart",
            "isDummy",
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
            "location",
            "selected",
            "errorHandling",
            "targetActions",
            "isStart",
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
    } else if (field.startsWith("has")
        || field.equals("enabled")
        || field.equals("distributes")
        || field.equals("unconditional")
        || field.equals("evaluation")) {
      // Boolean fields
      return Arrays.asList(RuleCondition.MUST_BE_TRUE, RuleCondition.MUST_BE_FALSE);
    } else if (field.equals("transforms")
        || field.equals("actions")
        || field.equals("hops")
        || field.equals("parameters")
        || field.equals("variables")) {
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
          RuleCondition.NO_HARDCODED,
          RuleCondition.MATCHES_PATTERN,
          RuleCondition.NOT_MATCHES_PATTERN,
          RuleCondition.CONTAINS,
          RuleCondition.NOT_CONTAINS,
          RuleCondition.STARTS_WITH,
          RuleCondition.ENDS_WITH);
    }
  }
}
