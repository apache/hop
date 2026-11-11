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

package org.apache.hop.execution.failure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.Result;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.failure.ExecutionFailure;
import org.apache.hop.core.failure.FailureCategory;
import org.apache.hop.core.failure.FailureCause;
import org.apache.hop.core.failure.FailureClassifier;
import org.apache.hop.core.failure.FailureComponent;
import org.apache.hop.core.failure.FailureField;
import org.apache.hop.core.failure.FailurePhase;
import org.apache.hop.core.failure.FailureRun;
import org.apache.hop.core.failure.IExecutionFailureListener;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.HopLoggingEvent;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LogMessage;
import org.apache.hop.core.logging.LoggingBuffer;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.util.CredentialRedactor;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;

/** Builds {@link ExecutionFailure}s from what the engines know, and announces them. */
public final class ExecutionFailures {

  /** How many parents are followed up from a run, in case a logging hierarchy loops. */
  private static final int MAX_PARENTS = 50;

  private ExecutionFailures() {
    // Static helpers only
  }

  /**
   * The failure of a transform.
   *
   * @param transform the transform that failed
   * @param phase INIT or RUN
   * @param throwable what it threw, or null when it only logged an error and set its error count
   */
  public static ExecutionFailure forTransform(
      ITransform transform, FailurePhase phase, Throwable throwable) {
    ExecutionFailure failure = new ExecutionFailure();
    ILoggingObject pipeline = transform.getPipeline();
    failure.setRun(runOf(pipeline));
    failure.setParents(parentsOf(pipeline));
    failure.setComponent(
        new FailureComponent(
            FailureComponent.KIND_TRANSFORM,
            transform.getTransformName(),
            transform.getTransformPluginId(),
            transform.getCopy(),
            transform.getLogChannelId()));
    failure.setPhase(phase);
    setCause(failure, throwable, transform.getLogChannelId(), false, transform);
    if (transform instanceof BaseTransform baseTransform) {
      failure.setInputLayout(FailureField.of(baseTransform.getInputRowMeta()));
    }
    Map<String, Long> counters = failure.getCounters();
    counters.put("read", transform.getLinesRead());
    counters.put("written", transform.getLinesWritten());
    counters.put("input", transform.getLinesInput());
    counters.put("output", transform.getLinesOutput());
    counters.put("updated", transform.getLinesUpdated());
    counters.put("rejected", transform.getLinesRejected());
    counters.put("errors", transform.getErrors());
    return failure;
  }

  /**
   * The failure of an action.
   *
   * @param workflow the workflow running the action
   * @param actionMeta the action as it is in the workflow
   * @param action the copy of the action that ran
   * @param result its result, null when it threw
   * @param throwable what it threw, or null when it returned a result with errors
   */
  public static ExecutionFailure forAction(
      ILoggingObject workflow,
      ActionMeta actionMeta,
      IAction action,
      Result result,
      Throwable throwable) {
    ExecutionFailure failure = new ExecutionFailure();
    failure.setRun(runOf(workflow));
    failure.setParents(parentsOf(workflow));
    String actionLogChannelId =
        action.getLogChannel() == null ? null : action.getLogChannel().getLogChannelId();
    failure.setComponent(
        new FailureComponent(
            FailureComponent.KIND_ACTION,
            actionMeta.getName(),
            action.getPluginId(),
            0,
            actionLogChannelId));
    failure.setPhase(FailurePhase.RUN);

    // A child pipeline or workflow that failed left its failure in the result
    ExecutionFailure childFailure = null;
    if (result != null && result.getFailures() != null && !result.getFailures().isEmpty()) {
      childFailure = result.getFailures().get(0);
    }
    if (throwable == null && childFailure != null) {
      failure.setCausedBy(childFailure);
      failure.setCategory(childFailure.getCategory());
    } else {
      setCause(failure, throwable, actionLogChannelId, true, action);
    }

    if (result != null) {
      Map<String, Long> counters = failure.getCounters();
      counters.put("read", result.getNrLinesRead());
      counters.put("written", result.getNrLinesWritten());
      counters.put("input", result.getNrLinesInput());
      counters.put("output", result.getNrLinesOutput());
      counters.put("updated", result.getNrLinesUpdated());
      counters.put("rejected", result.getNrLinesRejected());
      counters.put("errors", result.getNrErrors());
    }
    return failure;
  }

  /**
   * The failure of a pipeline or workflow as a whole.
   *
   * @param run the pipeline or workflow
   * @param phase when it failed, used when there is no failure that caused it
   * @param causedBy the transform or action failure that brought it down, or null
   * @param throwable what the run itself threw, used when there is no failure that caused it
   */
  public static ExecutionFailure forRun(
      ILoggingObject run, FailurePhase phase, ExecutionFailure causedBy, Throwable throwable) {
    ExecutionFailure failure = new ExecutionFailure();
    failure.setRun(runOf(run));
    failure.setParents(parentsOf(run));
    if (causedBy != null) {
      failure.setCausedBy(causedBy);
      failure.setPhase(causedBy.getPhase());
      failure.setCategory(causedBy.getCategory());
    } else {
      failure.setPhase(phase);
      setCause(
          failure,
          throwable,
          run.getLogChannelId(),
          false,
          run instanceof IVariables variables ? variables : null);
    }
    return failure;
  }

  /**
   * Announce a failure: to the extension point plugins and then to the listeners. Neither can make
   * the failure handling fail: what they throw is logged.
   */
  public static void announce(
      ILogChannel log,
      IVariables variables,
      String extensionPointId,
      ExecutionFailure failure,
      List<IExecutionFailureListener> listeners) {
    try {
      ExtensionPointHandler.callExtensionPoint(log, variables, extensionPointId, failure);
    } catch (Throwable e) {
      log.logError("Error in the " + extensionPointId + " extension point", e);
    }
    if (listeners == null) {
      return;
    }
    List<IExecutionFailureListener> copy;
    synchronized (listeners) {
      copy = new ArrayList<>(listeners);
    }
    for (IExecutionFailureListener listener : copy) {
      try {
        listener.failed(failure);
      } catch (Throwable e) {
        log.logError("Error in an execution failure listener", e);
      }
    }
  }

  /**
   * One line for a person: what failed, in which category, and the message of the root cause. For
   * example {@code Transform 'Read orders' (TableInput) failed [CONNECTIVITY]: Connection refused}.
   * Null for no failure.
   */
  public static String describe(ExecutionFailure failure) {
    if (failure == null) {
      return null;
    }
    ExecutionFailure root = failure.getRootCause();
    StringBuilder description = new StringBuilder();
    FailureComponent component = root.getComponent();
    if (component != null) {
      description
          .append(FailureComponent.KIND_ACTION.equals(component.getKind()) ? "Action" : "Transform")
          .append(" '")
          .append(component.getName())
          .append("'");
      if (component.getPluginId() != null) {
        description.append(" (").append(component.getPluginId()).append(")");
      }
    } else if (root.getRun() != null && root.getRun().getName() != null) {
      description.append("'").append(root.getRun().getName()).append("'");
    } else {
      description.append("The run");
    }
    description.append(" failed [").append(root.getCategory()).append("]");
    String message = messageOf(root.getCause());
    if (message != null) {
      description.append(": ").append(message);
    }
    return description.toString();
  }

  /** The most specific message of a cause: the root cause's, else its own first line. */
  private static String messageOf(FailureCause cause) {
    if (cause == null) {
      return null;
    }
    String message = cause.getRootCauseMessage();
    if (message == null || message.isBlank()) {
      message = cause.getMessage();
    }
    if (message == null || message.isBlank()) {
      return null;
    }
    // Hop exception messages start with a line break and repeat the nested messages
    for (String line : message.split("\\R")) {
      if (!line.isBlank()) {
        return line.trim();
      }
    }
    return null;
  }

  /** The run as a failure knows it. */
  public static FailureRun runOf(ILoggingObject object) {
    if (object == null) {
      return null;
    }
    return new FailureRun(
        object.getLogChannelId(),
        object.getObjectType() == null ? null : object.getObjectType().name(),
        object.getObjectName(),
        object.getFilename(),
        object.getContainerId());
  }

  /** The parents of a run, the top-level execution first. */
  public static List<FailureRun> parentsOf(ILoggingObject object) {
    List<FailureRun> parents = new ArrayList<>();
    if (object == null) {
      return parents;
    }
    ILoggingObject parent = object.getParent();
    while (parent != null && parents.size() < MAX_PARENTS) {
      parents.add(runOf(parent));
      parent = parent.getParent();
    }
    Collections.reverse(parents);
    return parents;
  }

  /** The cause from the exception or, failing that, from the last error logged on a log channel. */
  private static void setCause(
      ExecutionFailure failure,
      Throwable throwable,
      String logChannelId,
      boolean includeChildChannels,
      IVariables variables) {
    if (throwable != null) {
      failure.setCategory(FailureClassifier.classify(throwable));
      failure.setCause(redact(FailureClassifier.causeOf(throwable), variables));
      return;
    }
    LogMessage logMessage = lastErrorLogged(logChannelId, includeChildChannels);
    if (logMessage != null) {
      failure.setCategory(
          FailureClassifier.classifyText(logMessage.getMessage(), logMessage.getStackTrace()));
      failure.setCause(
          redact(
              FailureClassifier.causeFromLog(logMessage.getMessage(), logMessage.getStackTrace()),
              variables));
    } else {
      failure.setCategory(FailureCategory.UNKNOWN);
    }
  }

  /**
   * The cause of an exception, with credentials and the secret values of these variables masked.
   * For a failure outside a pipeline or workflow, like hop-run failing to load a file.
   */
  public static FailureCause causeOf(Throwable throwable, IVariables variables) {
    return redact(FailureClassifier.causeOf(throwable), variables);
  }

  /**
   * Mask credentials, encrypted passwords and the values of secret variables in the texts of a
   * cause: they go to execution information locations, Hop Server clients and result files.
   */
  static FailureCause redact(FailureCause cause, IVariables variables) {
    if (cause == null) {
      return null;
    }
    Set<String> secrets = CredentialRedactor.secretValuesOf(variables);
    cause.setMessage(CredentialRedactor.redact(cause.getMessage(), secrets));
    cause.setRootCauseMessage(CredentialRedactor.redact(cause.getRootCauseMessage(), secrets));
    cause.setStackTrace(CredentialRedactor.redact(cause.getStackTrace(), secrets));
    cause.setRedacted(true);
    return cause;
  }

  /** Was an error logged on this log channel itself? */
  public static boolean hasLoggedError(String logChannelId) {
    return lastErrorLogged(logChannelId, false) != null;
  }

  /**
   * The last error logged on a log channel, or, when that has none and children are included, on
   * one of the channels below it.
   */
  static LogMessage lastErrorLogged(String logChannelId, boolean includeChildChannels) {
    if (logChannelId == null) {
      return null;
    }
    LoggingBuffer buffer = HopLogStore.getAppender();
    int last = buffer.getLastBufferLineNr();
    LogMessage message =
        lastError(buffer.getLogBufferFromTo(List.of(logChannelId), false, 0, last));
    if (message == null && includeChildChannels) {
      List<String> channels = LoggingRegistry.getInstance().getLogChannelChildren(logChannelId);
      if (channels != null) {
        message = lastError(buffer.getLogBufferFromTo(channels, false, 0, last));
      }
    }
    return message;
  }

  private static LogMessage lastError(List<HopLoggingEvent> events) {
    for (int i = events.size() - 1; i >= 0; i--) {
      HopLoggingEvent event = events.get(i);
      if (event.getLevel() == LogLevel.ERROR && event.getMessage() instanceof LogMessage message) {
        return message;
      }
    }
    return null;
  }
}
