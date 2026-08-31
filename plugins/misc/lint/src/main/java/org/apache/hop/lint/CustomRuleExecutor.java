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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

/** Executes custom lint rules against Hop objects */
public class CustomRuleExecutor {

  private static final ILogChannel log = LogChannel.GENERAL;

  /**
   * Marks "this object has no such field", as distinct from "the field is there and null".
   *
   * <p>Collapsing the two made a typo in a rule's {@code targetField} indistinguishable from a
   * clean result, which matters most for plugin-scoped rules: those exist precisely to read a field
   * that only one kind of transform has.
   */
  private static final Object FIELD_NOT_FOUND = new Object();

  /**
   * A field which can only be answered with a project index, evaluated somewhere that has none.
   *
   * <p>Distinct from {@link #FIELD_NOT_FOUND}: the field is real and the rule is correct, we simply
   * cannot answer it for a single file. Reporting "never referenced" on the strength of a project
   * we never looked at would be a false positive on every single-file lint.
   */
  private static final Object NEEDS_PROJECT_CONTEXT = new Object();

  /** The project index for the current run, or an empty one outside a project lint. */
  private static final ThreadLocal<LintProjectIndex> PROJECT_INDEX =
      ThreadLocal.withInitial(LintProjectIndex::empty);

  /**
   * Make a project index available to the rules evaluated on this thread.
   *
   * @param index the index, or null to clear it
   */
  public static void setProjectIndex(LintProjectIndex index) {
    if (index == null) {
      PROJECT_INDEX.remove();
    } else {
      PROJECT_INDEX.set(index);
    }
  }

  /** Execute a custom rule against a Hop object */
  public static List<LintResult> executeRule(
      CustomLintRule rule, Object hopObject, String fileName) {
    List<LintResult> results = new ArrayList<>();

    if (!rule.isEnabled()) {
      return results;
    }

    try {
      // Determine if the object matches the rule target
      if (!matchesTarget(rule.getTarget(), hopObject)) {
        return results;
      }

      // A rule may narrow itself to specific transform or action types.
      if (!rule.appliesToPlugin(pluginIdOf(hopObject))) {
        return results;
      }

      // For password field pattern matching, check multiple fields
      if (rule.getTarget() == RuleTarget.TRANSFORM || rule.getTarget() == RuleTarget.ACTION) {
        if (rule.getCondition() == RuleCondition.NO_HARDCODED
            && (rule.getTargetField().equals("password")
                || rule.getTargetField().equals("secret")
                || rule.getTargetField().equals("credential"))) {
          // Check all password-related fields
          results.addAll(checkPasswordFields(rule, hopObject, fileName));
          return results;
        }
      }

      // Every rule is evaluated as a list of clauses; a rule which checks one thing simply has a
      // list of one. That keeps the composed and simple paths from drifting apart.
      List<RuleClause> clauses = rule.getClauses();
      List<String> violated = new ArrayList<>();
      Object firstFieldValue = null;

      for (int i = 0; i < clauses.size(); i++) {
        RuleClause clause = clauses.get(i);
        Object clauseValue = extractFieldValue(hopObject, clause.getTargetField(), rule);

        if (clauseValue == NEEDS_PROJECT_CONTEXT || clauseValue == FIELD_NOT_FOUND) {
          // Handled below, by the existing reporting, using the first clause's outcome.
          if (i == 0) {
            firstFieldValue = clauseValue;
            break;
          }
          // A later clause which cannot be read makes the whole rule unanswerable rather than
          // silently reducing it to the clauses that could be read.
          firstFieldValue = clauseValue;
          break;
        }
        if (i == 0) {
          firstFieldValue = clauseValue;
        }
        if (evaluateCondition(
            clause.getCondition(), clauseValue, clause.getConditionValue(), rule)) {
          violated.add(clause.describe() + " (actual: " + describeValue(clauseValue) + ")");
        } else if (rule.getCombinator() == RuleCombinator.ALL_OF && rule.isComposed()) {
          // allOf needs every clause broken, so one satisfied clause ends it.
          return results;
        }
      }

      Object fieldValue = firstFieldValue;

      if (fieldValue == NEEDS_PROJECT_CONTEXT) {
        // Quietly, and only here: "hop lint <one file>" and lint-on-save have no project to look
        // at, and a rule the run cannot answer must not become a finding either way.
        log.logDetailed(
            "Skipping rule "
                + rule.generateRuleId()
                + " for "
                + fileName
                + ": '"
                + rule.getTargetField()
                + "' can only be answered by a project lint.");
        return results;
      }

      if (fieldValue == FIELD_NOT_FOUND) {
        // A rule that named specific plugin types asserted the field exists on them, so a
        // missing field is a mistake in the rule and is reported rather than passed over.
        // Unscoped rules stay opportunistic: they run across every transform, most of which
        // legitimately do not have the field.
        if (!rule.getAppliesTo().isEmpty()) {
          results.add(
              createResult(
                  rule,
                  "Rule '"
                      + rule.generateRuleId()
                      + "' reads field '"
                      + rule.getTargetField()
                      + "', which does not exist on "
                      + describe(hopObject)
                      + ". Check the field name against this transform or action.",
                  fileName,
                  hopObject,
                  "ERROR"));
        }
        return results;
      }

      boolean violatesRule = rule.isComposed() ? !violated.isEmpty() : violated.size() == 1;

      if (violatesRule) {
        String message = generateErrorMessage(rule, fieldValue);
        if (rule.isComposed()) {
          message =
              message
                  + " ["
                  + String.join(
                      rule.getCombinator() == RuleCombinator.ALL_OF ? " and " : " or ", violated)
                  + "]";
        }
        results.add(createResult(rule, message, fileName, hopObject));
      }

    } catch (Exception e) {
      log.logError("Error executing custom rule " + rule.getName() + ": " + e.getMessage(), e);
      results.add(
          createResult(
              rule, "Rule execution failed: " + e.getMessage(), fileName, hopObject, "ERROR"));
    }

    return results;
  }

  private static LintResult createResult(
      CustomLintRule rule, String message, String fileName, Object hopObject) {
    return createResult(rule, message, fileName, hopObject, rule.getSeverity());
  }

  private static LintResult createResult(
      CustomLintRule rule, String message, String fileName, Object hopObject, String severity) {
    return new LintResult(
        rule.generateRuleId(),
        rule.getName(),
        severity,
        message,
        fileName,
        sourceFrom(hopObject),
        LintResult.Origin.LINT);
  }

  /**
   * The plugin id of a transform or action, or null for anything else.
   *
   * <p>Only transforms and actions have a plugin type worth narrowing by; a pipeline, workflow or
   * connection is already a single kind of thing, so a rule targeting one of those is unaffected by
   * {@code appliesTo}.
   */
  private static String pluginIdOf(Object hopObject) {
    if (hopObject instanceof TransformMeta) {
      return ((TransformMeta) hopObject).getTransformPluginId();
    }
    if (hopObject instanceof ActionMeta actionMeta && actionMeta.getAction() != null) {
      return actionMeta.getAction().getPluginId();
    }
    // For metadata objects the equivalent of a plugin id is the type's registered key, which is
    // what a rule names in appliesTo and what the metadata/<key>/ folder is called.
    if (hopObject instanceof IHopMetadata) {
      HopMetadata annotation = hopObject.getClass().getAnnotation(HopMetadata.class);
      if (annotation != null) {
        return annotation.key();
      }
    }
    return null;
  }

  private static LintSourceRef sourceFrom(Object hopObject) {
    if (hopObject instanceof TransformMeta) {
      return LintSourceRef.transform(((TransformMeta) hopObject).getName());
    }
    if (hopObject instanceof ActionMeta) {
      return LintSourceRef.action(((ActionMeta) hopObject).getName());
    }
    if (hopObject instanceof PipelineMeta) {
      return LintSourceRef.pipeline(((PipelineMeta) hopObject).getName());
    }
    if (hopObject instanceof WorkflowMeta) {
      return LintSourceRef.workflow(((WorkflowMeta) hopObject).getName());
    }
    if (hopObject instanceof DatabaseMeta) {
      return LintSourceRef.metadata(((DatabaseMeta) hopObject).getName());
    }
    if (hopObject instanceof PipelineHopMeta || hopObject instanceof WorkflowHopMeta) {
      return LintSourceRef.hop(hopLabel(hopObject));
    }
    return null;
  }

  /** Build a human-readable label for a hop, e.g. "From -> To". */
  private static String hopLabel(Object hopObject) {
    if (hopObject instanceof PipelineHopMeta) {
      PipelineHopMeta hop = (PipelineHopMeta) hopObject;
      String from = hop.getFromTransform() != null ? hop.getFromTransform().getName() : "?";
      String to = hop.getToTransform() != null ? hop.getToTransform().getName() : "?";
      return from + " -> " + to;
    }
    if (hopObject instanceof WorkflowHopMeta) {
      WorkflowHopMeta hop = (WorkflowHopMeta) hopObject;
      String from = hop.getFromAction() != null ? hop.getFromAction().getName() : "?";
      String to = hop.getToAction() != null ? hop.getToAction().getName() : "?";
      return from + " -> " + to;
    }
    return "";
  }

  /** Check if the object matches the rule target type */
  private static boolean matchesTarget(RuleTarget target, Object hopObject) {
    switch (target) {
      case PIPELINE:
        return hopObject instanceof PipelineMeta;
      case WORKFLOW:
        return hopObject instanceof WorkflowMeta;
      case DATABASE_CONNECTION:
        return hopObject instanceof DatabaseMeta;
      case TRANSFORM:
        return hopObject instanceof TransformMeta
            || (hopObject != null && hopObject.getClass().getName().contains("TransformMeta"));
      case ACTION:
        return hopObject instanceof ActionMeta
            || (hopObject != null && hopObject.getClass().getName().contains("ActionMeta"));
      case HOP:
        return hopObject instanceof PipelineHopMeta || hopObject instanceof WorkflowHopMeta;
      case METADATA:
        // Any registered metadata type; appliesTo narrows it to specific ones.
        return hopObject instanceof IHopMetadata;
      default:
        return false;
    }
  }

  /** Extract field value from the Hop object */
  private static Object extractFieldValue(Object hopObject, String fieldName, CustomLintRule rule) {
    try {
      if (hopObject instanceof PipelineMeta) {
        PipelineMeta pipeline = (PipelineMeta) hopObject;
        switch (fieldName) {
          case "name":
            return pipeline.getName();
          case "description":
            return pipeline.getDescription();
          case "transformCount":
            return pipeline.getTransforms().size();
          case "hopCount":
            return pipeline.getPipelineHops().size();
          case "filename":
            return pipeline.getFilename();
          case "hasDisabledHops":
            return pipeline.getPipelineHops().stream().anyMatch(hop -> !hop.isEnabled());
          case "hasOrphanedTransforms":
            return hasOrphanedTransforms(pipeline);
          case "noteCount":
            return pipeline.getNotes().size();
          case "hasNotes":
            return !pipeline.getNotes().isEmpty();
          case "isReferenced":
            return referencedInProject(pipeline.getFilename());
          default:
            log.logDetailed("Unknown pipeline field: " + fieldName);
            return null;
        }
      } else if (hopObject instanceof WorkflowMeta) {
        WorkflowMeta workflow = (WorkflowMeta) hopObject;
        switch (fieldName) {
          case "name":
            return workflow.getName();
          case "description":
            return workflow.getDescription();
          case "actionCount":
            return workflow.getActions().size();
          case "hopCount":
            return workflow.getWorkflowHops().size();
          case "filename":
            return workflow.getFilename();
          case "hasDisabledHops":
            return workflow.getWorkflowHops().stream().anyMatch(hop -> !hop.isEnabled());
          case "hasOrphanedActions":
            return hasOrphanedActions(workflow);
          case "noteCount":
            return workflow.getNotes().size();
          case "hasNotes":
            return !workflow.getNotes().isEmpty();
          case "isReferenced":
            return referencedInProject(workflow.getFilename());
          default:
            log.logDetailed("Unknown workflow field: " + fieldName);
            return null;
        }
      } else if (hopObject instanceof DatabaseMeta) {
        DatabaseMeta dbMeta = (DatabaseMeta) hopObject;
        switch (fieldName) {
          case "name":
            return dbMeta.getName();
          case "description":
            // DatabaseMeta has no dedicated description field; expose it via
            // a custom connection attribute if one was set.
            return dbMeta.getAttributes() != null
                ? dbMeta.getAttributes().getOrDefault("description", "")
                : "";
          case "hostname":
            return dbMeta.getHostname();
          case "port":
            return dbMeta.getPort();
          case "databaseName":
            return dbMeta.getDatabaseName();
          case "username":
            return dbMeta.getUsername();
          case "password":
            return dbMeta.getPassword();
          default:
            log.logDetailed("Unknown database field: " + fieldName);
            return null;
        }
      } else if (hopObject instanceof PipelineHopMeta) {
        PipelineHopMeta hop = (PipelineHopMeta) hopObject;
        switch (fieldName) {
          case "name":
            return hopLabel(hop);
          case "enabled":
            return hop.isEnabled();
          case "fromTransform":
            return hop.getFromTransform() != null ? hop.getFromTransform().getName() : null;
          case "toTransform":
            return hop.getToTransform() != null ? hop.getToTransform().getName() : null;
          default:
            log.logDetailed("Unknown pipeline hop field: " + fieldName);
            return null;
        }
      } else if (hopObject instanceof WorkflowHopMeta) {
        WorkflowHopMeta hop = (WorkflowHopMeta) hopObject;
        switch (fieldName) {
          case "name":
            return hopLabel(hop);
          case "enabled":
            return hop.isEnabled();
          case "unconditional":
            return hop.isUnconditional();
          case "evaluation":
            return hop.isEvaluation();
          case "fromAction":
            return hop.getFromAction() != null ? hop.getFromAction().getName() : null;
          case "toAction":
            return hop.getToAction() != null ? hop.getToAction().getName() : null;
          default:
            log.logDetailed("Unknown workflow hop field: " + fieldName);
            return null;
        }
      } else if (hopObject instanceof TransformMeta) {
        TransformMeta transformMeta = (TransformMeta) hopObject;
        return extractFieldFromTransform(transformMeta, fieldName, rule);
      } else if (hopObject instanceof ActionMeta) {
        ActionMeta actionMeta = (ActionMeta) hopObject;
        return extractFieldFromAction(actionMeta, fieldName);
      } else if (hopObject instanceof IHopMetadata) {
        // Any other metadata type: read the property by getter or field. There is no
        // hard-coded field list here on purpose, so a rule can target a type this code has
        // never heard of, including one from a third-party plugin.
        return extractFieldFromObject(hopObject, fieldName);
      }
    } catch (Exception e) {
      log.logError(
          "Error extracting field "
              + fieldName
              + " from "
              + (hopObject != null ? hopObject.getClass().getSimpleName() : "null")
              + ": "
              + e.getMessage(),
          e);
    }

    // Return special marker to indicate field not found vs. null value
    return null;
  }

  /** Extract field value from a transform using reflection */
  private static Object extractFieldFromTransform(
      TransformMeta transformMeta, String fieldName, CustomLintRule rule) {
    try {
      // First check TransformMeta level fields
      switch (fieldName) {
        case "name":
          return transformMeta.getName();
        case "description":
          return transformMeta.getDescription();
        case "pluginId":
          return transformMeta.getTransformPluginId();
        case "copies":
          return transformMeta.getCopies(
              org.apache.hop.core.variables.Variables.getADefaultVariableSpace());
        case "isDummy":
          return "Dummy".equalsIgnoreCase(transformMeta.getTransformPluginId());
        case "hasDefaultName":
          return hasDefaultGeneratedName(transformMeta.getName());
        case "isBlockingTransform":
          return isBlockingTransformPlugin(transformMeta.getTransformPluginId(), rule);
        default:
          // Try to get from the transform implementation
          Object transform = transformMeta.getTransform();
          if (transform != null) {
            return extractFieldFromObject(transform, fieldName);
          }
          return FIELD_NOT_FOUND;
      }
    } catch (Exception e) {
      log.logDetailed("Error extracting field " + fieldName + " from transform: " + e.getMessage());
      return null;
    }
  }

  /** Extract field value from an action using reflection */
  private static Object extractFieldFromAction(ActionMeta actionMeta, String fieldName) {
    try {
      // First check ActionMeta level fields
      switch (fieldName) {
        case "name":
          return actionMeta.getName();
        case "description":
          return actionMeta.getDescription();
        case "pluginId":
          return actionMeta.getAction().getPluginId();
        case "hasDefaultName":
          return hasDefaultGeneratedName(actionMeta.getName());
        default:
          // Try to get from the action implementation
          Object action = actionMeta.getAction();
          if (action != null) {
            return extractFieldFromObject(action, fieldName);
          }
          return FIELD_NOT_FOUND;
      }
    } catch (Exception e) {
      log.logDetailed("Error extracting field " + fieldName + " from action: " + e.getMessage());
      return null;
    }
  }

  private static boolean hasDefaultGeneratedName(String name) {
    if (Utils.isEmpty(name)) {
      return false;
    }
    return name.matches("(?i)(Transform|Action)\\s+\\d+");
  }

  /**
   * Default list of known blocking transform plugin IDs. A rule may override or extend this list
   * via the "blockingTransforms" additional parameter in its YAML configuration.
   */
  private static final List<String> BLOCKING_TRANSFORM_PLUGINS =
      Arrays.asList(
          "SortRows",
          "BlockingTransform",
          "GroupBy",
          "AggregateRows",
          "AnalyticQuery",
          "FuzzyMatch",
          "JoinRows",
          "MergeJoin",
          "MergeRowsDiff",
          "StreamLookup",
          "SynchronizedTransform",
          "WebService",
          "HTTPClient",
          "Mail",
          "MailInput");

  /**
   * Check if a transform plugin ID represents a blocking transform. The list of blocking plugin IDs
   * can be overridden per-rule via the "blockingTransforms" additional parameter.
   */
  private static boolean isBlockingTransformPlugin(String pluginId, CustomLintRule rule) {
    if (Utils.isEmpty(pluginId)) {
      return false;
    }
    return getBlockingTransformPlugins(rule).contains(pluginId);
  }

  /**
   * Resolve the blocking-transform plugin IDs for a rule, falling back to the built-in defaults.
   */
  private static List<String> getBlockingTransformPlugins(CustomLintRule rule) {
    if (rule != null && rule.getAdditionalParameters() != null) {
      Object configured = rule.getAdditionalParameters().get("blockingTransforms");
      if (configured instanceof List) {
        @SuppressWarnings("unchecked")
        List<String> ids = (List<String>) configured;
        if (!ids.isEmpty()) {
          return ids;
        }
      }
    }
    return BLOCKING_TRANSFORM_PLUGINS;
  }

  /** Check if a pipeline has orphaned transforms (transforms with no incoming or outgoing hops) */
  private static boolean hasOrphanedTransforms(PipelineMeta pipeline) {
    if (pipeline == null
        || pipeline.getTransforms() == null
        || pipeline.getTransforms().isEmpty()) {
      return false;
    }

    List<PipelineHopMeta> hops = pipeline.getPipelineHops();
    if (hops == null || hops.isEmpty()) {
      // If no hops exist, all transforms with more than 0 transforms are orphaned
      return pipeline.getTransforms().size() > 0;
    }

    // Build sets of transforms that have incoming and outgoing connections
    java.util.Set<TransformMeta> transformsWithIncoming = new java.util.HashSet<>();
    java.util.Set<TransformMeta> transformsWithOutgoing = new java.util.HashSet<>();

    for (PipelineHopMeta hop : hops) {
      if (hop.isEnabled()) {
        TransformMeta fromTransform = hop.getFromTransform();
        TransformMeta toTransform = hop.getToTransform();

        if (fromTransform != null) {
          transformsWithOutgoing.add(fromTransform);
        }
        if (toTransform != null) {
          transformsWithIncoming.add(toTransform);
        }
      }
    }

    // A transform is orphaned if it has no incoming AND no outgoing hops
    for (TransformMeta transform : pipeline.getTransforms()) {
      boolean hasIncoming = transformsWithIncoming.contains(transform);
      boolean hasOutgoing = transformsWithOutgoing.contains(transform);

      if (!hasIncoming && !hasOutgoing) {
        return true; // Found at least one orphaned transform
      }
    }

    return false;
  }

  /**
   * Check if a workflow has orphaned actions (actions with no incoming or outgoing workflow hops)
   */
  private static boolean hasOrphanedActions(WorkflowMeta workflow) {
    if (workflow == null || workflow.getActions() == null || workflow.getActions().isEmpty()) {
      return false;
    }

    List<WorkflowHopMeta> hops = workflow.getWorkflowHops();
    if (hops == null || hops.isEmpty()) {
      // If no hops exist, all actions with more than 0 actions are orphaned
      return workflow.getActions().size() > 0;
    }

    // Build sets of actions that have incoming and outgoing connections
    java.util.Set<ActionMeta> actionsWithIncoming = new java.util.HashSet<>();
    java.util.Set<ActionMeta> actionsWithOutgoing = new java.util.HashSet<>();

    for (WorkflowHopMeta hop : hops) {
      if (hop.isEnabled()) {
        ActionMeta fromAction = hop.getFromAction();
        ActionMeta toAction = hop.getToAction();

        if (fromAction != null) {
          actionsWithOutgoing.add(fromAction);
        }
        if (toAction != null) {
          actionsWithIncoming.add(toAction);
        }
      }
    }

    // An action is orphaned if it has no incoming AND no outgoing hops
    for (ActionMeta action : workflow.getActions()) {
      boolean hasIncoming = actionsWithIncoming.contains(action);
      boolean hasOutgoing = actionsWithOutgoing.contains(action);

      if (!hasIncoming && !hasOutgoing) {
        return true; // Found at least one orphaned action
      }
    }

    return false;
  }

  /** Extract field value from an object using reflection, supporting field name patterns */
  private static Object extractFieldFromObject(Object obj, String fieldName) {
    if (obj == null) {
      return FIELD_NOT_FOUND;
    }

    // Hop groups related settings into nested objects — a Text File Output's file name lives at
    // fileSettings.fileName — so a rule can walk into them with a dotted path.
    int dot = fieldName.indexOf('.');
    if (dot > 0) {
      Object parent = extractFieldFromObject(obj, fieldName.substring(0, dot));
      if (parent == FIELD_NOT_FOUND || parent == null) {
        return FIELD_NOT_FOUND;
      }
      return extractFieldFromObject(parent, fieldName.substring(dot + 1));
    }

    try {
      Class<?> clazz = obj.getClass();

      List<Field> fields = getAllFields(clazz);

      // The name Hop serialises the property under comes first. That is the name a rule author
      // actually sees, in the .hpl or .hwf file and in the metadata JSON, and it is the one that
      // survives a rename of the Java field behind it.
      for (Field field : fields) {
        if (fieldName.equals(serialisedNameOf(field))) {
          field.setAccessible(true);
          return field.get(obj);
        }
      }

      // Then a declared field anywhere in the hierarchy, and its value is returned as-is.
      // Skipping null or empty values here used to make them indistinguishable from a missing
      // field, so a rule like "url NOT_EMPTY" could never fire.
      for (Field field : fields) {
        if (field.getName().equals(fieldName)) {
          field.setAccessible(true);
          return field.get(obj);
        }
      }
      // Then the same match ignoring case, because rules are hand-written YAML and Hop's own
      // field names are inconsistent about it ("fileName" here, "filename" there).
      for (Field field : fields) {
        if (field.getName().equalsIgnoreCase(fieldName)) {
          field.setAccessible(true);
          return field.get(obj);
        }
      }

      // Then a getter, which covers metas that expose a value they do not store directly.
      String getterName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
      for (String candidate : new String[] {getterName, fieldName}) {
        try {
          java.lang.reflect.Method getter = clazz.getMethod(candidate);
          return getter.invoke(obj);
        } catch (NoSuchMethodException e) {
          // Try the next shape.
        }
      }

      // "password", "secret" and "credential" are aliases for a family of field names rather
      // than fields in their own right, so fall back to a pattern search for those.
      String lowerFieldName = fieldName.toLowerCase();
      if (lowerFieldName.equals("password")
          || lowerFieldName.equals("secret")
          || lowerFieldName.equals("credential")) {
        List<String> passwordPatterns =
            Arrays.asList(
                "password",
                "pwd",
                "passwd",
                "secret",
                "secretkey",
                "credential",
                "credentials",
                "apikey",
                "token",
                "accesstoken",
                "authtoken");

        for (Field field : getAllFields(clazz)) {
          String candidate = field.getName().toLowerCase();
          for (String pattern : passwordPatterns) {
            if (candidate.contains(pattern)) {
              field.setAccessible(true);
              Object value = field.get(obj);
              if (value != null && !Utils.isEmpty(value.toString())) {
                return value;
              }
            }
          }
        }
        // No secret-bearing field at all is a pass, not a configuration error.
        return null;
      }

    } catch (Exception e) {
      log.logDetailed("Error extracting field " + fieldName + " via reflection: " + e.getMessage());
      return null;
    }

    return FIELD_NOT_FOUND;
  }

  /**
   * Whether the project refers to this file, or a marker saying we are in no position to know.
   *
   * @param filename the file to ask about
   * @return true or false when a project index is available, the sentinel when it is not
   */
  private static Object referencedInProject(String filename) {
    LintProjectIndex index = PROJECT_INDEX.get();
    if (index == null || !index.isPopulated()) {
      return NEEDS_PROJECT_CONTEXT;
    }
    return index.isFileReferenced(filename);
  }

  /** A field value as it should read inside a composed rule's message. */
  private static String describeValue(Object value) {
    if (value == null) {
      return "null";
    }
    String text = value.toString();
    return text.length() > 60 ? text.substring(0, 57) + "..." : text;
  }

  /** Human-readable label for a transform or action, used in configuration error messages. */
  private static String describe(Object hopObject) {
    if (hopObject instanceof TransformMeta transformMeta) {
      return "transform '"
          + transformMeta.getName()
          + "' ("
          + transformMeta.getTransformPluginId()
          + ")";
    }
    if (hopObject instanceof ActionMeta actionMeta) {
      String pluginId =
          actionMeta.getAction() != null ? actionMeta.getAction().getPluginId() : "unknown";
      return "action '" + actionMeta.getName() + "' (" + pluginId + ")";
    }
    return hopObject != null ? hopObject.getClass().getSimpleName() : "null";
  }

  /**
   * The name a property is stored under in the file, which is what a rule should be able to name.
   *
   * <p>Hop's serialisation is driven by {@link HopMetadataProperty}: the {@code key} when it gives
   * one, otherwise the field name. A rule written against this keeps working when the Java field is
   * renamed, and matches what a user reads in the pipeline or metadata file.
   *
   * @param field the field to name
   * @return the serialised name, or null when the field is not a serialised property
   */
  static String serialisedNameOf(Field field) {
    HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
    if (property == null) {
      return null;
    }
    return Utils.isEmpty(property.key()) ? field.getName() : property.key();
  }

  /** Get all fields from a class hierarchy */
  static List<Field> getAllFields(Class<?> clazz) {
    List<Field> fields = new ArrayList<>();
    while (clazz != null && clazz != Object.class) {
      fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
      clazz = clazz.getSuperclass();
    }
    return fields;
  }

  /** Check password fields in transform/action for hardcoded values */
  private static List<LintResult> checkPasswordFields(
      CustomLintRule rule, Object hopObject, String fileName) {
    List<LintResult> results = new ArrayList<>();

    try {
      Object transformOrAction = null;
      String objectName = "";

      if (hopObject instanceof TransformMeta) {
        TransformMeta transformMeta = (TransformMeta) hopObject;
        transformOrAction = transformMeta.getTransform();
        objectName = transformMeta.getName();
      } else if (hopObject instanceof ActionMeta) {
        ActionMeta actionMeta = (ActionMeta) hopObject;
        transformOrAction = actionMeta.getAction();
        objectName = actionMeta.getName();
      }

      if (transformOrAction == null) {
        return results;
      }

      // Get field patterns from rule parameters or use defaults
      List<String> fieldPatterns = getPasswordFieldPatterns(rule);

      // Check all password-related fields
      Class<?> clazz = transformOrAction.getClass();
      for (Field field : getAllFields(clazz)) {
        String fieldName = field.getName().toLowerCase();
        for (String pattern : fieldPatterns) {
          if (fieldName.contains(pattern.toLowerCase())) {
            try {
              field.setAccessible(true);
              Object value = field.get(transformOrAction);
              if (value != null && !Utils.isEmpty(value.toString())) {
                String strValue = value.toString();
                // Check if it's hardcoded (not a variable)
                if (!isVariable(strValue)) {
                  String message =
                      String.format(
                          "%s '%s' has hardcoded value in field '%s'. Consider using a variable instead (e.g., ${%s})",
                          rule.getTarget() == RuleTarget.TRANSFORM ? "Transform" : "Action",
                          objectName,
                          field.getName(),
                          field.getName().toUpperCase().replaceAll("[^A-Z0-9]", "_"));
                  results.add(createResult(rule, message, fileName, hopObject));
                }
              }
            } catch (Exception e) {
              log.logDetailed("Error checking field " + field.getName() + ": " + e.getMessage());
            }
          }
        }
      }

    } catch (Exception e) {
      log.logError("Error checking password fields: " + e.getMessage(), e);
    }

    return results;
  }

  /** Get password field patterns from rule parameters or return defaults */
  private static List<String> getPasswordFieldPatterns(CustomLintRule rule) {
    List<String> defaultPatterns =
        Arrays.asList(
            "password",
            "pwd",
            "passwd",
            "secret",
            "secretKey",
            "credential",
            "credentials",
            "apiKey",
            "apikey",
            "token",
            "accessToken",
            "authToken");

    if (rule.getAdditionalParameters() != null) {
      Object patternsObj = rule.getAdditionalParameters().get("fieldPatterns");
      if (patternsObj instanceof List) {
        @SuppressWarnings("unchecked")
        List<String> patterns = (List<String>) patternsObj;
        if (!patterns.isEmpty()) {
          return patterns;
        }
      }
    }

    return defaultPatterns;
  }

  /** Check if a string is a Hop variable (enclosed in ${...}) */
  private static boolean isVariable(String value) {
    if (Utils.isEmpty(value)) {
      return false;
    }

    // Check if it's a simple variable like ${VAR_NAME}
    if (value.startsWith("${") && value.endsWith("}")) {
      return true;
    }

    // Check if it contains variables (might be mixed with other text)
    return value.contains("${") && value.contains("}");
  }

  /** Evaluate a condition against a field value */
  private static boolean evaluateCondition(
      RuleCondition condition, Object fieldValue, String conditionValue, CustomLintRule rule) {
    if (fieldValue == null) {
      // An absent value is the strongest form of "missing", so the presence conditions must
      // fire on it. Hop returns null for an unset description, which is exactly the case
      // "description NOT_EMPTY" exists to catch; treating null as passing made those rules
      // fire only on a description explicitly set to "".
      return condition == RuleCondition.NOT_NULL || condition == RuleCondition.NOT_EMPTY;
    }

    // Handle null condition value for conditions that don't need it
    if (conditionValue == null) {
      conditionValue = "";
    }

    switch (condition) {
      case MAX_VALUE:
        return evaluateNumericCondition(
            fieldValue, conditionValue, (field, target) -> field > target);

      case MIN_VALUE:
        return evaluateNumericCondition(
            fieldValue, conditionValue, (field, target) -> field < target);

      case EXACT_VALUE:
        return evaluateNumericCondition(
            fieldValue, conditionValue, (field, target) -> field != target);

      case NOT_EMPTY:
        return Utils.isEmpty(fieldValue.toString());

      case NOT_NULL:
        return fieldValue == null;

      case NO_HARDCODED:
        String strValue = fieldValue.toString();
        return !Utils.isEmpty(strValue) && !isVariable(strValue);

      case MATCHES_PATTERN:
        if (conditionValue == null || conditionValue.isEmpty()) {
          log.logBasic(
              "MATCHES_PATTERN condition requires a non-empty regex pattern in rule: "
                  + rule.getName());
          return false;
        }
        try {
          Pattern pattern = Pattern.compile(conditionValue);
          return !pattern.matcher(fieldValue.toString()).matches();
        } catch (Exception e) {
          log.logError(
              "Invalid regex pattern '" + conditionValue + "' in rule: " + rule.getName(), e);
          return false;
        }

      case NOT_MATCHES_PATTERN:
        if (conditionValue == null || conditionValue.isEmpty()) {
          log.logBasic(
              "NOT_MATCHES_PATTERN condition requires a non-empty regex pattern in rule: "
                  + rule.getName());
          return false;
        }
        try {
          Pattern pattern = Pattern.compile(conditionValue);
          return pattern.matcher(fieldValue.toString()).matches();
        } catch (Exception e) {
          log.logError(
              "Invalid regex pattern '" + conditionValue + "' in rule: " + rule.getName(), e);
          return false;
        }

      case CONTAINS:
        if (conditionValue == null || conditionValue.isEmpty()) {
          log.logBasic(
              "CONTAINS condition requires a non-empty value to search for in rule: "
                  + rule.getName());
          return false; // Don't flag as violation if condition is invalid
        }
        return !fieldValue.toString().contains(conditionValue);

      case NOT_CONTAINS:
        if (conditionValue == null || conditionValue.isEmpty()) {
          log.logBasic(
              "NOT_CONTAINS condition requires a non-empty value to search for in rule: "
                  + rule.getName());
          return false; // Don't flag as violation if condition is invalid
        }
        return fieldValue.toString().contains(conditionValue);

      case STARTS_WITH:
        return !fieldValue.toString().startsWith(conditionValue);

      case ENDS_WITH:
        return !fieldValue.toString().endsWith(conditionValue);

      case MUST_BE_TRUE:
        return !(fieldValue instanceof Boolean) || !((Boolean) fieldValue);

      case MUST_BE_FALSE:
        return !(fieldValue instanceof Boolean) || ((Boolean) fieldValue);

      case NOT_EMPTY_COLLECTION:
        if (fieldValue instanceof List) {
          return ((List<?>) fieldValue).isEmpty();
        }
        return false;

      case MAX_COLLECTION_SIZE:
        if (fieldValue instanceof List) {
          int size = ((List<?>) fieldValue).size();
          try {
            int maxSize = Integer.parseInt(conditionValue);
            return size > maxSize;
          } catch (NumberFormatException e) {
            return false;
          }
        }
        return false;

      case MIN_COLLECTION_SIZE:
        if (fieldValue instanceof List) {
          int size = ((List<?>) fieldValue).size();
          try {
            int minSize = Integer.parseInt(conditionValue);
            return size < minSize;
          } catch (NumberFormatException e) {
            return false;
          }
        }
        return false;

      default:
        // Returning false here would report the rule as passing, which is the worst
        // outcome: a rule that cannot run looks like a rule that found nothing. Fail
        // loudly instead, so executeRule() turns it into a visible finding against the
        // configuration.
        throw new IllegalStateException(
            "Rule '"
                + rule.generateRuleId()
                + "' uses condition "
                + condition.name()
                + ", which this version of the linter cannot evaluate. Remove the rule or"
                + " use a supported condition.");
    }
  }

  /** Helper method for numeric condition evaluation */
  private static boolean evaluateNumericCondition(
      Object fieldValue, String conditionValue, NumericComparator comparator) {
    try {
      double fieldNum;
      if (fieldValue instanceof Number) {
        fieldNum = ((Number) fieldValue).doubleValue();
      } else {
        fieldNum = Double.parseDouble(fieldValue.toString());
      }

      double targetNum = Double.parseDouble(conditionValue);
      return comparator.compare(fieldNum, targetNum);

    } catch (NumberFormatException e) {
      log.logError(
          "Invalid numeric values for comparison: field="
              + fieldValue
              + ", target="
              + conditionValue,
          e);
      return false;
    }
  }

  /** Generate an error message for a rule violation */
  private static String generateErrorMessage(CustomLintRule rule, Object fieldValue) {
    StringBuilder message = new StringBuilder();
    // The parser defaults a missing description to "", so a null check alone left the message
    // starting with blank text. Fall back to the rule name, then to its id.
    String headline = rule.getDescription();
    if (Utils.isEmpty(headline)) {
      headline = Utils.isEmpty(rule.getName()) ? rule.generateRuleId() : rule.getName();
    }
    message.append(headline);

    if (fieldValue != null) {
      message.append(" (current value: ").append(fieldValue).append(")");
    }

    if (!Utils.isEmpty(rule.getConditionValue())) {
      message
          .append(" (expected: ")
          .append(rule.getCondition().getDisplayName().toLowerCase())
          .append(" ")
          .append(rule.getConditionValue())
          .append(")");
    }

    return message.toString();
  }

  @FunctionalInterface
  private interface NumericComparator {
    boolean compare(double field, double target);
  }
}
