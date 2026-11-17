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

package org.apache.hop.lineage;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.metadata.LineageSink;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * The lineage sinks a run reports to, besides the ones configured with variables: the sink its run
 * configuration selects, and the sinks of the run that started it. Worked out once per run, when it
 * is first asked, and kept with the run. The run holds those sinks in the {@link LineageHub} until
 * it ends and calls {@link #release(IPipelineEngine)}.
 */
public final class LineageSinkRoutes {

  /** Where a run keeps its sinks, in its extension data. */
  static final String EXTENSION_DATA_KEY = "lineage.sinks";

  /** Set in a run's extension data once it let go of its sinks. */
  static final String RELEASED_KEY = "lineage.sinks.released";

  private LineageSinkRoutes() {}

  /** The keys of the sinks a pipeline run reports to; empty when there are none. */
  public static Set<String> of(IPipelineEngine<PipelineMeta> pipeline) {
    return of(pipeline, LineageHub.getInstance());
  }

  /** The keys of the sinks a workflow run reports to; empty when there are none. */
  public static Set<String> of(IWorkflowEngine<WorkflowMeta> workflow) {
    return of(workflow, LineageHub.getInstance());
  }

  /**
   * Whether lineage is observed for a pipeline run: lineage is enabled with variables, or the run
   * reports to a sink of its own. Callers that do extra work only to fill in an event check this.
   */
  public static boolean isEnabled(IPipelineEngine<PipelineMeta> pipeline) {
    return LineageHub.getInstance().isEnabled() || !of(pipeline).isEmpty();
  }

  /** Whether lineage is observed for a workflow run; see {@link #isEnabled(IPipelineEngine)}. */
  public static boolean isEnabled(IWorkflowEngine<WorkflowMeta> workflow) {
    return LineageHub.getInstance().isEnabled() || !of(workflow).isEmpty();
  }

  /**
   * Whether lineage is observed for the run of a transform; see {@link
   * #isEnabled(IPipelineEngine)}.
   */
  public static boolean isEnabled(ITransform transform) {
    return isEnabled(transform == null ? null : transform.getPipeline());
  }

  static Set<String> of(IPipelineEngine<PipelineMeta> pipeline, LineageHub hub) {
    if (pipeline == null) {
      return Set.of();
    }
    return cached(
        pipeline.getExtensionDataMap(),
        hub,
        () -> {
          Set<String> parents = parentSinks(pipeline, hub);
          PipelineRunConfiguration runConfiguration = pipeline.getPipelineRunConfiguration();
          String own =
              useSink(
                  runConfiguration == null ? null : runConfiguration.getLineageSinkName(),
                  pipeline,
                  pipeline.getMetadataProvider(),
                  pipeline.getLogChannel(),
                  hub);
          return withParents(parents, own, hub);
        });
  }

  static Set<String> of(IWorkflowEngine<WorkflowMeta> workflow, LineageHub hub) {
    if (workflow == null) {
      return Set.of();
    }
    return cached(
        workflow.getExtensionDataMap(),
        hub,
        () -> {
          Set<String> parents = parentSinks(workflow, hub);
          WorkflowRunConfiguration runConfiguration = workflow.getWorkflowRunConfiguration();
          String own =
              useSink(
                  runConfiguration == null ? null : runConfiguration.getLineageSinkName(),
                  workflow,
                  workflow.getMetadataProvider(),
                  workflow.getLogChannel(),
                  hub);
          return withParents(parents, own, hub);
        });
  }

  /**
   * Lets go of the sinks a pipeline run held, once it ended and emitted its last event: a sink that
   * was replaced by a newer version of itself can then be shut down.
   */
  public static void release(IPipelineEngine<PipelineMeta> pipeline) {
    release(pipeline == null ? null : pipeline.getExtensionDataMap(), LineageHub.getInstance());
  }

  /** Lets go of the sinks a workflow run held; see {@link #release(IPipelineEngine)}. */
  public static void release(IWorkflowEngine<WorkflowMeta> workflow) {
    release(workflow == null ? null : workflow.getExtensionDataMap(), LineageHub.getInstance());
  }

  static void release(Map<String, Object> extensionData, LineageHub hub) {
    if (extensionData == null) {
      return;
    }
    Set<String> sinks;
    synchronized (extensionData) {
      if (!(extensionData.get(EXTENSION_DATA_KEY) instanceof Set<?> known)
          || extensionData.putIfAbsent(RELEASED_KEY, Boolean.TRUE) != null) {
        return;
      }
      @SuppressWarnings("unchecked")
      Set<String> held = (Set<String>) known;
      sinks = held;
    }
    hub.release(sinks);
  }

  /**
   * The sinks of the run that started this one and the run's own, each held once for this run, as
   * {@link #release} lets go of each once. The parent's are safe to hold now: the parent holds them
   * until it ends, after this run.
   *
   * @param own the run's own sink, which {@link LineageHub#useSink} already held, or null
   */
  private static Set<String> withParents(Set<String> parentSinks, String own, LineageHub hub) {
    Set<String> inherited = new LinkedHashSet<>(parentSinks);
    if (own != null) {
      inherited.remove(own);
    }
    hub.hold(inherited);
    Set<String> sinks = new LinkedHashSet<>(parentSinks);
    if (own != null) {
      sinks.add(own);
    }
    return sinks;
  }

  private static Set<String> parentSinks(IPipelineEngine<PipelineMeta> pipeline, LineageHub hub) {
    if (pipeline.getParentPipeline() != null) {
      return of(pipeline.getParentPipeline(), hub);
    }
    if (pipeline.getParentWorkflow() != null) {
      return of(pipeline.getParentWorkflow(), hub);
    }
    return of(actionWorkflow(pipeline.getParent()), hub);
  }

  private static Set<String> parentSinks(IWorkflowEngine<WorkflowMeta> workflow, LineageHub hub) {
    if (workflow.getParentPipeline() != null) {
      return of(workflow.getParentPipeline(), hub);
    }
    if (workflow.getParentWorkflow() != null) {
      return of(workflow.getParentWorkflow(), hub);
    }
    return of(actionWorkflow(workflow.getParent()), hub);
  }

  /**
   * The workflow of an action that started a run without naming its workflow as the parent, such as
   * the pipelines Run pipeline unit tests starts.
   */
  private static IWorkflowEngine<WorkflowMeta> actionWorkflow(Object parent) {
    return parent instanceof IAction action ? action.getParentWorkflow() : null;
  }

  /**
   * Uses and holds the sink of a run configuration. A sink that can't be loaded or resolved is
   * logged on the run and left out: lineage must not stop a run.
   *
   * @return the key of the sink, or null when there is none
   */
  private static String useSink(
      String sinkName,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      ILogChannel log,
      LineageHub hub) {
    String name = variables.resolve(sinkName);
    if (Utils.isEmpty(name) || metadataProvider == null) {
      return null;
    }
    try {
      LineageSink sink = metadataProvider.getSerializer(LineageSink.class).load(name);
      if (sink == null) {
        throw new IllegalArgumentException("lineage sink '" + name + "' does not exist");
      }
      String key = hub.useSink(sink.toTarget(variables, metadataProvider));
      String failure = hub.failure(key);
      if (failure != null && log != null) {
        log.logError(
            "Lineage sink '"
                + name
                + "' failed to start ("
                + failure
                + "). It is tried again a minute after it failed; until then this run's lineage"
                + " doesn't reach it.");
      }
      return key;
    } catch (Exception e) {
      if (log != null) {
        log.logError("Unable to use lineage sink '" + name + "', this run doesn't report to it", e);
      }
      return null;
    }
  }

  private static Set<String> cached(
      Map<String, Object> extensionData, LineageHub hub, Supplier<Set<String>> resolver) {
    if (extensionData == null) {
      return Collections.unmodifiableSet(resolver.get());
    }
    synchronized (extensionData) {
      Object known = extensionData.get(EXTENSION_DATA_KEY);
      if (known instanceof Set<?> set) {
        @SuppressWarnings("unchecked")
        Set<String> sinks = (Set<String>) set;
        return sinks;
      }
    }
    // Resolved outside the lock: it reads metadata, and asks the parent run, which locks its own.
    Set<String> sinks = Collections.unmodifiableSet(resolver.get());
    synchronized (extensionData) {
      Object known = extensionData.putIfAbsent(EXTENSION_DATA_KEY, sinks);
      if (known instanceof Set<?> set) {
        // Another thread of the run got there first: give back what this one took.
        hub.release(sinks);
        @SuppressWarnings("unchecked")
        Set<String> first = (Set<String>) set;
        return first;
      }
      return sinks;
    }
  }
}
