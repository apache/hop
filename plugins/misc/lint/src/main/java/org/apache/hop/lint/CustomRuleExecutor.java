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
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
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
   * The pipeline or workflow the transform or action being evaluated belongs to.
   *
   * <p>Rules are handed one transform at a time and a {@link TransformMeta} does not know its
   * pipeline, so a question about how a transform is connected has nowhere to look. Holding the
   * file being linted here is what lets a finding name the transform rather than the file: the
   * alternative is a rule on the pipeline reporting "something here is orphaned", which is not a
   * thing anyone can click on.
   */
  private static final ThreadLocal<Object> SUBJECT = new ThreadLocal<>();

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

  /** Whether a project index is in place for the rules evaluated on this thread. */
  public static boolean hasProjectIndex() {
    return PROJECT_INDEX.get().isPopulated();
  }

  /**
   * Make the file being linted available to the rules evaluated on this thread.
   *
   * @param subject the pipeline or workflow, or null to clear it
   */
  public static void setSubject(Object subject) {
    if (subject == null) {
      SUBJECT.remove();
    } else {
      SUBJECT.set(subject);
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
          violated.add(
              clause.describe()
                  + " (actual: "
                  + describeValue(
                      clauseValue, holdsASecret(rule, hopObject, clause.getTargetField()))
                  + ")");
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
        // legitimately do not have the field. Every connection has the same fields, so there a
        // missing one is always a mistake in the rule.
        if (!rule.getAppliesTo().isEmpty() || hopObject instanceof DatabaseMeta) {
          results.add(
              createResult(
                  rule,
                  "Rule '"
                      + rule.generateRuleId()
                      + "' reads field '"
                      + rule.getTargetField()
                      + "', which does not exist on "
                      + describe(hopObject)
                      + ". Check the field name against "
                      + (hopObject instanceof DatabaseMeta
                          ? "the fields offered for database connections."
                          : "this transform or action."),
                  fileName,
                  hopObject,
                  "ERROR"));
        }
        return results;
      }

      boolean violatesRule = rule.isComposed() ? !violated.isEmpty() : violated.size() == 1;

      if (violatesRule) {
        String message =
            generateErrorMessage(
                rule, holdsASecret(rule, hopObject, rule.getTargetField()) ? null : fieldValue);
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
          case "databaseType":
            return dbMeta.getPluginId();
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
          case "servername":
            return dbMeta.getServername();
          case "dataTablespace":
            return dbMeta.getDataTablespace();
          case "indexTablespace":
            return dbMeta.getIndexTablespace();
          case "attributes":
            // The extra options are attributes too, stored as EXTRA_OPTION_<type>.<option>.
            return dbMeta.getAttributes();
          default:
            return connectionField(dbMeta, fieldName);
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

  /**
   * Any other connection property, such as manualUrl or Oracle's walletPassword. The settings live
   * on the database plugin's own meta, which is also what the connection's file stores, so that is
   * looked at first; then the getters DatabaseMeta adds on top.
   */
  private static Object connectionField(DatabaseMeta dbMeta, String fieldName) {
    Object value =
        dbMeta.getIDatabase() != null
            ? extractFieldFromObject(dbMeta.getIDatabase(), fieldName)
            : FIELD_NOT_FOUND;
    return value != FIELD_NOT_FOUND ? value : extractFieldFromObject(dbMeta, fieldName);
  }

  /**
   * The fields every transform has, whatever its plugin, with their types. The linter works them
   * out itself rather than reading them from the plugin, so {@code hop lint --list-fields} lists
   * them from here. Keep in step with {@link #extractFieldFromTransform}.
   */
  public static final Map<String, String> TRANSFORM_FIELDS =
      orderedFields(
          "name", "String",
          "description", "String",
          "pluginId", "String",
          "copies", "int",
          "distributes", "boolean",
          "errorHandling", "boolean",
          "targetTransforms", "List",
          "isDummy", "boolean",
          "hasDefaultName", "boolean",
          "isOrphaned", "boolean",
          "isBlockingTransform", "boolean");

  /**
   * As {@link #TRANSFORM_FIELDS}, for actions. Keep in step with {@link #extractFieldFromAction}.
   */
  public static final Map<String, String> ACTION_FIELDS =
      orderedFields(
          "name", "String",
          "description", "String",
          "pluginId", "String",
          "errorHandling", "boolean",
          "targetActions", "List",
          "isStart", "boolean",
          "hasDefaultName", "boolean",
          "isOrphaned", "boolean");

  private static Map<String, String> orderedFields(String... namesAndTypes) {
    Map<String, String> fields = new LinkedHashMap<>();
    for (int i = 0; i < namesAndTypes.length; i += 2) {
      fields.put(namesAndTypes[i], namesAndTypes[i + 1]);
    }
    return Collections.unmodifiableMap(fields);
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
        case "distributes":
          return transformMeta.isDistributes();
        case "errorHandling":
          return transformMeta.getTransform() != null && transformMeta.isDoingErrorHandling();
        case "targetTransforms":
          // Not PipelineMeta.findNextTransforms: a hop whose transform name matches nothing loads
          // with a null end, and that made it throw for every transform in the pipeline.
          if (!(SUBJECT.get() instanceof PipelineMeta pipeline)) {
            return null;
          }
          List<String> targets = new ArrayList<>();
          for (PipelineHopMeta hop : pipeline.getPipelineHops()) {
            if (hop.isEnabled()
                && hop.getFromTransform() == transformMeta
                && hop.getToTransform() != null) {
              targets.add(hop.getToTransform().getName());
            }
          }
          return targets;
        case "isDummy":
          return "Dummy".equalsIgnoreCase(transformMeta.getTransformPluginId());
        case "hasDefaultName":
          return hasDefaultGeneratedName(
              transformMeta.getName(),
              transformMeta.getTransformPluginId(),
              TransformPluginType.class);
        case "isOrphaned":
          return SUBJECT.get() instanceof PipelineMeta pipeline
              ? isOrphaned(transformMeta, pipeline.getPipelineHops(), pipeline.getTransforms())
              : null;
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
        case "errorHandling":
          // An action handles its errors with a hop followed on failure.
          return SUBJECT.get() instanceof WorkflowMeta workflow
              ? outgoingHops(actionMeta, workflow).stream()
                  .anyMatch(hop -> !hop.isUnconditional() && !hop.isEvaluation())
              : null;
        case "targetActions":
          return SUBJECT.get() instanceof WorkflowMeta workflow
              ? outgoingHops(actionMeta, workflow).stream()
                  .map(hop -> hop.getToAction().getName())
                  .toList()
              : null;
        case "isStart":
          return actionMeta.isStart();
        case "hasDefaultName":
          // Every workflow starts at Start; there is no better name for it.
          return !actionMeta.isStart()
              && hasDefaultGeneratedName(
                  actionMeta.getName(),
                  actionMeta.getAction().getPluginId(),
                  ActionPluginType.class);
        case "isOrphaned":
          return SUBJECT.get() instanceof WorkflowMeta workflow
              ? isOrphaned(actionMeta, workflow.getWorkflowHops(), workflow.getActions())
              : null;
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

  /**
   * The enabled hops leaving this action that lead somewhere. A hop whose action name matches
   * nothing loads with a null end, and is skipped rather than failing the field for the hops that
   * are fine. The ends are compared by identity: ActionMeta.equals throws for an action without its
   * inner action.
   */
  private static List<WorkflowHopMeta> outgoingHops(ActionMeta actionMeta, WorkflowMeta workflow) {
    List<WorkflowHopMeta> hops = new ArrayList<>();
    for (WorkflowHopMeta hop : workflow.getWorkflowHops()) {
      if (hop.isEnabled() && hop.getFromAction() == actionMeta && hop.getToAction() != null) {
        hops.add(hop);
      }
    }
    return hops;
  }

  /**
   * Whether a transform or action still has the name Hop Gui gave it: the plugin's name, such as
   * "Table input", followed by a number when that name was taken, as in "Dummy (do nothing) 2".
   *
   * <p>Only "Transform 1" and "Action 1" used to count, and Hop Gui never generates those. The
   * plugin's name is the one of the language Hop runs in, so a name Hop Gui gave in another
   * language is not recognised.
   */
  static boolean hasDefaultGeneratedName(
      String name, String pluginId, Class<? extends IPluginType> pluginType) {
    if (Utils.isEmpty(name)) {
      return false;
    }
    String trimmed = name.trim();
    if (trimmed.matches("(?i)(Transform|Action)\\s+\\d+")) {
      return true;
    }
    if (Utils.isEmpty(pluginId)) {
      return false;
    }
    IPlugin plugin = PluginRegistry.getInstance().findPluginWithId(pluginType, pluginId);
    if (plugin == null || Utils.isEmpty(plugin.getName())) {
      return false;
    }
    String pluginName = plugin.getName().trim();
    return trimmed.equalsIgnoreCase(pluginName)
        || (trimmed.length() > pluginName.length() + 1
            && trimmed.substring(0, pluginName.length()).equalsIgnoreCase(pluginName)
            && trimmed.substring(pluginName.length()).matches("\\s+\\d+"));
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
  /**
   * Whether this element is connected to nothing in the file it lives in.
   *
   * <p>Two things it deliberately does not count as orphaned, both of which put a warning on
   * projects that had nothing wrong with them:
   *
   * <ul>
   *   <li>the only element in the file. A one-transform pipeline is a normal thing to write, which
   *       is why the core pack does not ship a rule on transform counts either. With nothing to be
   *       disconnected from, it cannot be disconnected.
   *   <li>an element whose hops are all disabled. It has hops; they are switched off, which is a
   *       different observation and one the core pack ships switched off (STRUCT-003), because a
   *       disabled hop is work in progress to most teams. Reporting it here as "never executes"
   *       made that opinion the default through the back door.
   * </ul>
   */
  private static <T> boolean isOrphaned(T element, List<? extends Object> hops, List<T> elements) {
    if (element == null || elements == null || elements.size() < 2) {
      return false;
    }
    if (hops == null || hops.isEmpty()) {
      return true;
    }
    for (Object hop : hops) {
      if (connects(hop, element)) {
        return false;
      }
    }
    return true;
  }

  /** Whether the hop has this element at either end, enabled or not. */
  private static boolean connects(Object hop, Object element) {
    if (hop instanceof PipelineHopMeta pipelineHop) {
      return element.equals(pipelineHop.getFromTransform())
          || element.equals(pipelineHop.getToTransform());
    }
    if (hop instanceof WorkflowHopMeta workflowHop) {
      return element.equals(workflowHop.getFromAction())
          || element.equals(workflowHop.getToAction());
    }
    return false;
  }

  private static boolean hasOrphanedTransforms(PipelineMeta pipeline) {
    if (pipeline == null || pipeline.getTransforms() == null) {
      return false;
    }
    for (TransformMeta transform : pipeline.getTransforms()) {
      if (isOrphaned(transform, pipeline.getPipelineHops(), pipeline.getTransforms())) {
        return true;
      }
    }
    return false;
  }

  /** Whether a workflow has actions connected to nothing. */
  private static boolean hasOrphanedActions(WorkflowMeta workflow) {
    if (workflow == null || workflow.getActions() == null) {
      return false;
    }
    for (ActionMeta action : workflow.getActions()) {
      if (isOrphaned(action, workflow.getWorkflowHops(), workflow.getActions())) {
        return true;
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

      // The value is returned as-is. Skipping null or empty values here used to make them
      // indistinguishable from a missing field, so a rule like "url NOT_EMPTY" could never fire.
      Field named = fieldNamed(clazz, fieldName);
      if (named != null) {
        named.setAccessible(true);
        return named.get(obj);
      }

      // Then a getter, which covers metas that expose a value they do not store directly.
      String capitalised = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
      for (String candidate : new String[] {"get" + capitalised, "is" + capitalised, fieldName}) {
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
  private static String describeValue(Object value, boolean secret) {
    if (value == null) {
      return "null";
    }
    if (secret) {
      return "hidden";
    }
    String text = shown(value).toString();
    return text.length() > 60 ? text.substring(0, 57) + "..." : text;
  }

  /**
   * A value as a finding may show it. Of a map only the keys: a connection's attributes hold its
   * extra options, and an option such as a token or a key would otherwise land in the report.
   */
  private static Object shown(Object value) {
    if (value instanceof Map<?, ?> map) {
      return new TreeSet<>(map.keySet().stream().map(String::valueOf).toList());
    }
    return value;
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
    if (hopObject instanceof DatabaseMeta databaseMeta) {
      return "connection '" + databaseMeta.getName() + "'";
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

  /**
   * The declared field a rule's name refers to, or null when no field carries that name.
   *
   * @param clazz the class to look in, superclasses included
   * @param fieldName the name the rule uses
   * @return the field, or null
   */
  private static Field fieldNamed(Class<?> clazz, String fieldName) {
    List<Field> fields = getAllFields(clazz);

    // The name Hop serialises the property under comes first. That is the name a rule author
    // actually sees, in the .hpl or .hwf file and in the metadata JSON, and it is the one that
    // survives a rename of the Java field behind it.
    for (Field field : fields) {
      if (fieldName.equals(serialisedNameOf(field))) {
        return field;
      }
    }
    // Then a declared field anywhere in the hierarchy.
    for (Field field : fields) {
      if (field.getName().equals(fieldName)) {
        return field;
      }
    }
    // Then the same match ignoring case, because rules are hand-written YAML and Hop's own
    // field names are inconsistent about it ("fileName" here, "filename" there).
    for (Field field : fields) {
      if (field.getName().equalsIgnoreCase(fieldName)) {
        return field;
      }
    }
    return null;
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
        for (String pattern : fieldPatterns) {
          if (namesASecret(field, pattern)) {
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

  /**
   * Whether this field holds the secret the pattern names, rather than merely mentioning it.
   *
   * <p>Three narrowings, all of which cost nothing in coverage and remove findings that were simply
   * wrong. A substring match reported every Token Replacement transform in the project three times
   * over — {@code tokenStartString} defaults to {@code "${"}, {@code tokenEndString} to {@code "}"}
   * — and every Get Data From XML transform once, for the boolean {@code useToken}. Neither is a
   * credential, and a rule that cries wolf on a stock transform is one people switch off.
   *
   * <ul>
   *   <li>the name has to <em>end</em> with the pattern, so {@code sessionToken} and {@code
   *       trustStorePassword} match while {@code tokenStartString}, {@code oauth2TokenUrl} and
   *       {@code credentialsFile} do not: a secret is what the field is, not what it is about;
   *   <li>the field has to hold a string, because a flag, a count or a list of columns is never a
   *       credential however it is named;
   *   <li>the field has to belong to the instance, because a {@code static} field is part of the
   *       transform's code rather than of the file being linted. The Plugin Catalog transform's
   *       {@code FIELD_PROPERTY_PASSWORD = "property_password"} is a column name it writes, and no
   *       edit to a {@code .hpl} or {@code .hwf} can change it or make it hold a credential.
   * </ul>
   */
  private static boolean namesASecret(Field field, String pattern) {
    if (Utils.isEmpty(pattern)
        || !String.class.equals(field.getType())
        || Modifier.isStatic(field.getModifiers())) {
      return false;
    }
    return field.getName().toLowerCase().endsWith(pattern.trim().toLowerCase());
  }

  /**
   * Whether the value a clause reads is a secret, and so must stay out of the finding.
   *
   * <p>A finding's message ends up in CI build logs, JSON and SARIF reports and the GUI. DB-001
   * used to report a hardcoded database password as "(current value: secret123)", decrypting an
   * {@code Encrypted} one on the way, so the rule meant to catch exposed passwords exposed them.
   *
   * <p>The field decides, not the condition: Hop stores it as a password, or its name ends like a
   * secret's, with the default name patterns as well as the rule's own. Underscores are ignored so
   * the serialised {@code secret_access_key} matches {@code secretAccessKey}.
   */
  private static boolean holdsASecret(CustomLintRule rule, Object hopObject, String fieldName) {
    if (Utils.isEmpty(fieldName)) {
      return false;
    }
    if (storedAsPassword(hopObject, fieldName)) {
      return true;
    }
    String name = withoutUnderscores(fieldName);
    for (List<String> patterns :
        List.of(DEFAULT_SECRET_FIELD_PATTERNS, getPasswordFieldPatterns(rule))) {
      for (String pattern : patterns) {
        if (!Utils.isEmpty(pattern) && name.endsWith(withoutUnderscores(pattern))) {
          return true;
        }
      }
    }
    return false;
  }

  private static String withoutUnderscores(String name) {
    return name.trim().replace("_", "").toLowerCase();
  }

  /**
   * Whether the field a rule reads is one Hop stores as a password ({@code @HopMetadataProperty
   * password = true}), which covers secrets such as {@code accessKey} or {@code
   * oauth_jwt_private_key} that no name pattern catches.
   */
  private static boolean storedAsPassword(Object hopObject, String fieldName) {
    Object holder = hopObject;
    if (hopObject instanceof TransformMeta transformMeta) {
      holder = transformMeta.getTransform();
    } else if (hopObject instanceof ActionMeta actionMeta) {
      holder = actionMeta.getAction();
    } else if (hopObject instanceof DatabaseMeta databaseMeta) {
      // A connection's settings, sshTunnelPassphrase among them, live on its database plugin.
      holder = databaseMeta.getIDatabase();
    }
    String name = fieldName;
    int dot = fieldName.lastIndexOf('.');
    if (dot > 0) {
      holder = extractFieldFromObject(holder, fieldName.substring(0, dot));
      name = fieldName.substring(dot + 1);
    }
    if (holder == null || holder == FIELD_NOT_FOUND) {
      return false;
    }
    Field field = fieldNamed(holder.getClass(), name);
    HopMetadataProperty property =
        field != null ? field.getAnnotation(HopMetadataProperty.class) : null;
    return property != null && property.password();
  }

  private static final List<String> DEFAULT_SECRET_FIELD_PATTERNS =
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
          "secretAccessKey",
          "passphrase",
          "token",
          "accessToken",
          "authToken");

  /** Get password field patterns from rule parameters or return defaults */
  private static List<String> getPasswordFieldPatterns(CustomLintRule rule) {
    List<String> defaultPatterns = DEFAULT_SECRET_FIELD_PATTERNS;

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
      if (condition != RuleCondition.NOT_MATCHES_PATTERN) {
        return condition == RuleCondition.NOT_NULL || condition == RuleCondition.NOT_EMPTY;
      }
      // A pattern that forbids blank values, such as SQL-002's "no row limit", must also see an
      // unset value, so NOT_MATCHES_PATTERN reads null as "".
      fieldValue = "";
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

      case IS_EMPTY:
        // The opposite of NOT_EMPTY, so that a clause can require that a field is not set. Under
        // allOf, url MATCHES_PATTERN ^https://.* and httpLogin IS_EMPTY report a plain http:// URL
        // with a login.
        return !Utils.isEmpty(fieldValue.toString());

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
        return sizeOf(fieldValue) == 0;

      case MAX_COLLECTION_SIZE:
      case MIN_COLLECTION_SIZE:
        int size = sizeOf(fieldValue);
        if (size < 0) {
          return false;
        }
        try {
          int limit = Integer.parseInt(conditionValue);
          return condition == RuleCondition.MAX_COLLECTION_SIZE ? size > limit : size < limit;
        } catch (NumberFormatException e) {
          return false;
        }

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

  /**
   * The number of entries in a list, set or map, or -1 for anything else. A connection's {@code
   * attributes} are a map, which the collection conditions did not count.
   */
  private static int sizeOf(Object value) {
    if (value instanceof Collection<?> collection) {
      return collection.size();
    }
    if (value instanceof Map<?, ?> map) {
      return map.size();
    }
    return -1;
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
      message.append(" (current value: ").append(shown(fieldValue)).append(")");
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
