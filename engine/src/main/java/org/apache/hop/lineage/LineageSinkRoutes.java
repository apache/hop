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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.LineageRemoteRuns.RemoteRun;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.hub.LineageSinkTarget;
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

  /**
   * Set to {@code Y} on a pipeline run that runs a unit test, and on the runs it starts (the
   * testing plugin's {@code __UnitTest_Run__}).
   */
  public static final String VAR_RUN_UNIT_TEST = "__UnitTest_Run__";

  /**
   * The keys of the sinks a pipeline run reports to; empty when there are none, or when the run
   * runs a unit test. Such a run runs a copy of the pipeline, with data sets in place of transforms
   * and no file name: its lineage is not the pipeline's. Its sinks get only the test's results, see
   * {@link #ofUnitTest}.
   *
   * <p>This covers the sinks selected in run configurations only. Sinks set up with the {@code
   * HOP_LINEAGE_*} variables get every event of the process, a unit test run's included: {@link
   * #isEnabled(IPipelineEngine)} is true for it when lineage is enabled that way.
   */
  public static Set<String> of(IPipelineEngine<PipelineMeta> pipeline) {
    return ofRun(pipeline, LineageHub.getInstance());
  }

  static Set<String> ofRun(IPipelineEngine<PipelineMeta> pipeline, LineageHub hub) {
    // Held now, so they are released when the run ends, like the sinks of any run
    Set<String> sinks = of(pipeline, hub);
    return isUnitTestRun(pipeline) ? Set.of() : sinks;
  }

  /** The keys of the sinks the results of the unit test a pipeline run runs report to. */
  public static Set<String> ofUnitTest(IPipelineEngine<PipelineMeta> pipeline) {
    return of(pipeline, LineageHub.getInstance());
  }

  /**
   * Whether the results of the unit test a pipeline run runs are observed: lineage is enabled with
   * variables, or the run has sinks for its results ({@link #ofUnitTest}). Unless lineage is
   * enabled with variables, {@link #isEnabled(IPipelineEngine)} is false for such a run, which then
   * reports no lineage of its own.
   */
  public static boolean isUnitTestReported(IPipelineEngine<PipelineMeta> pipeline) {
    return isUnitTestReported(pipeline, LineageHub.getInstance());
  }

  static boolean isUnitTestReported(IPipelineEngine<PipelineMeta> pipeline, LineageHub hub) {
    return hub.isEnabled() || !of(pipeline, hub).isEmpty();
  }

  /**
   * Whether a run is a unit test run, or was started by one: the runs a unit test run starts (a
   * Pipeline executor's or Workflow executor's run, a mapping, the runs of an action in such a
   * workflow) take its variables, {@link #VAR_RUN_UNIT_TEST} included.
   */
  static boolean isUnitTestRun(IVariables run) {
    return run != null && "Y".equalsIgnoreCase(run.getVariable(VAR_RUN_UNIT_TEST));
  }

  /**
   * The keys of the sinks a workflow run reports to; empty when there are none, or when a unit test
   * run started it: its lineage is not a run of the pipelines and files it touches, any more than
   * the unit test run's own (see {@link #of(IPipelineEngine)}). As there, this covers the sinks
   * selected in run configurations only, not the ones set up with the {@code HOP_LINEAGE_*}
   * variables.
   */
  public static Set<String> of(IWorkflowEngine<WorkflowMeta> workflow) {
    return ofRun(workflow, LineageHub.getInstance());
  }

  static Set<String> ofRun(IWorkflowEngine<WorkflowMeta> workflow, LineageHub hub) {
    // Held now, so they are released when the run ends, like the sinks of any run
    Set<String> sinks = of(workflow, hub);
    return isUnitTestRun(workflow) ? Set.of() : sinks;
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
          List<String> own =
              remoteSinks(
                  LineageRemoteRuns.adopted(pipeline),
                  pipeline,
                  pipeline.getMetadataProvider(),
                  pipeline.getLogChannel(),
                  hub);
          own.add(
              useSink(
                  runConfiguration == null ? null : runConfiguration.getLineageSinkName(),
                  pipeline,
                  pipeline.getMetadataProvider(),
                  pipeline.getLogChannel(),
                  hub));
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
          List<String> own =
              remoteSinks(
                  LineageRemoteRuns.adopted(workflow),
                  workflow,
                  workflow.getMetadataProvider(),
                  workflow.getLogChannel(),
                  hub);
          own.add(
              useSink(
                  runConfiguration == null ? null : runConfiguration.getLineageSinkName(),
                  workflow,
                  workflow.getMetadataProvider(),
                  workflow.getLogChannel(),
                  hub));
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
   * @param own the run's own sinks, each of which {@link LineageHub#useSink} already held, with
   *     nulls for those it couldn't use
   */
  private static Set<String> withParents(
      Set<String> parentSinks, List<String> own, LineageHub hub) {
    Set<String> owned = new LinkedHashSet<>();
    for (String key : own) {
      if (key != null && !owned.add(key)) {
        // Used twice, such as a client's sink that is also the run configuration's: held once
        hub.release(Set.of(key));
      }
    }
    Set<String> inherited = new LinkedHashSet<>(parentSinks);
    inherited.removeAll(owned);
    hub.hold(inherited);
    Set<String> sinks = new LinkedHashSet<>(parentSinks);
    sinks.addAll(owned);
    return sinks;
  }

  /**
   * On a Hop Server, the sinks of the client's run this run reports ({@link LineageRemoteRuns}):
   * the ones the server accepted when it registered the run, see {@link #acceptedSinks}.
   *
   * @return the keys of those sinks, each held once; a mutable list the run's own sink is added to
   */
  private static List<String> remoteSinks(
      RemoteRun remoteRun,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      ILogChannel log,
      LineageHub hub) {
    List<String> keys = new ArrayList<>();
    if (remoteRun == null || remoteRun.sinks() == null || metadataProvider == null) {
      return keys;
    }
    for (String name : remoteRun.sinks()) {
      // The client resolved the name: used as it is, not resolved again with the server's variables
      keys.add(useResolvedSink(name, variables, metadataProvider, log, hub));
    }
    return keys;
  }

  /**
   * On a Hop Server, the sinks of a client's run the server can report to: each exists in the run's
   * metadata (the server's, with the client's metadata when the run came as an export), has a sink
   * type and that type's plugin is installed. The server answers the client with these, and the
   * client keeps reporting to the others itself. Whether a sink's URL can be reached from the
   * server only shows when its events are delivered. Each sink left out is logged on the run.
   *
   * @param names the names of the client's sinks, as the client resolved them
   * @return the names of the sinks the server reports the run to
   */
  static List<String> acceptedSinks(
      List<String> names,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      ILogChannel log,
      LineageHub hub) {
    List<String> accepted = new ArrayList<>();
    if (names == null) {
      return accepted;
    }
    for (String name : names) {
      if (Utils.isEmpty(name)) {
        continue;
      }
      String refused = refusal(name, variables, metadataProvider, hub);
      if (refused == null) {
        accepted.add(name);
      } else if (log != null) {
        log.logError(
            "Lineage sink '"
                + name
                + "', which the client selected for this run, can't be used on this Hop Server ("
                + refused
                + "). The client reports the run to it itself, without the run's transforms.");
      }
    }
    return accepted;
  }

  /** Why a Hop Server can't report a run to a sink, or null when it can. */
  private static String refusal(
      String name, IVariables variables, IHopMetadataProvider metadataProvider, LineageHub hub) {
    if (metadataProvider == null) {
      return "the run has no metadata";
    }
    try {
      LineageSink sink = metadataProvider.getSerializer(LineageSink.class).load(name);
      if (sink == null) {
        return "no lineage sink with this name in the server's metadata or in the metadata the run"
            + " came with; check the name and the server's project";
      }
      LineageSinkTarget target = sink.toTarget(variables, metadataProvider);
      if (!hub.hasSinkType(target.pluginId())) {
        return "the sink type '" + target.pluginId() + "' isn't installed on the server";
      }
      return null;
    } catch (Exception e) {
      return String.valueOf(e.getMessage());
    }
  }

  /**
   * The names of the sinks a pipeline run reports to, besides those configured with variables: what
   * a client passes to the Hop Server that runs it ({@link LineageRemoteRuns}).
   */
  static List<String> namesOf(IPipelineEngine<PipelineMeta> pipeline) {
    if (pipeline == null || isUnitTestRun(pipeline)) {
      return List.of();
    }
    Set<String> names = new LinkedHashSet<>(parentNames(pipeline));
    addRemoteNames(names, LineageRemoteRuns.adopted(pipeline));
    PipelineRunConfiguration runConfiguration = pipeline.getPipelineRunConfiguration();
    addName(
        names, runConfiguration == null ? null : runConfiguration.getLineageSinkName(), pipeline);
    return List.copyOf(names);
  }

  /** The names of the sinks a workflow run reports to; see {@link #namesOf(IPipelineEngine)}. */
  static List<String> namesOf(IWorkflowEngine<WorkflowMeta> workflow) {
    if (workflow == null || isUnitTestRun(workflow)) {
      return List.of();
    }
    Set<String> names = new LinkedHashSet<>(parentNames(workflow));
    addRemoteNames(names, LineageRemoteRuns.adopted(workflow));
    WorkflowRunConfiguration runConfiguration = workflow.getWorkflowRunConfiguration();
    addName(
        names, runConfiguration == null ? null : runConfiguration.getLineageSinkName(), workflow);
    return List.copyOf(names);
  }

  private static List<String> parentNames(IPipelineEngine<PipelineMeta> pipeline) {
    IPipelineEngine<PipelineMeta> parentPipeline =
        LineageRunLifecycleEmitter.parentPipelineOf(pipeline);
    if (parentPipeline != null) {
      return namesOf(parentPipeline);
    }
    if (pipeline.getParentWorkflow() != null) {
      return namesOf(pipeline.getParentWorkflow());
    }
    return namesOf(actionWorkflow(pipeline.getParent()));
  }

  private static List<String> parentNames(IWorkflowEngine<WorkflowMeta> workflow) {
    IPipelineEngine<PipelineMeta> parentPipeline =
        LineageRunLifecycleEmitter.parentPipelineOf(workflow);
    if (parentPipeline != null) {
      return namesOf(parentPipeline);
    }
    if (workflow.getParentWorkflow() != null) {
      return namesOf(workflow.getParentWorkflow());
    }
    return namesOf(actionWorkflow(workflow.getParent()));
  }

  private static void addRemoteNames(Set<String> names, RemoteRun remoteRun) {
    if (remoteRun != null && remoteRun.sinks() != null) {
      remoteRun.sinks().stream().filter(name -> !Utils.isEmpty(name)).forEach(names::add);
    }
  }

  private static void addName(Set<String> names, String sinkName, IVariables variables) {
    String name = variables.resolve(sinkName);
    if (!Utils.isEmpty(name)) {
      names.add(name);
    }
  }

  private static Set<String> parentSinks(IPipelineEngine<PipelineMeta> pipeline, LineageHub hub) {
    IPipelineEngine<PipelineMeta> parentPipeline =
        LineageRunLifecycleEmitter.parentPipelineOf(pipeline);
    if (parentPipeline != null) {
      return of(parentPipeline, hub);
    }
    if (pipeline.getParentWorkflow() != null) {
      return of(pipeline.getParentWorkflow(), hub);
    }
    return of(actionWorkflow(pipeline.getParent()), hub);
  }

  private static Set<String> parentSinks(IWorkflowEngine<WorkflowMeta> workflow, LineageHub hub) {
    IPipelineEngine<PipelineMeta> parentPipeline =
        LineageRunLifecycleEmitter.parentPipelineOf(workflow);
    if (parentPipeline != null) {
      return of(parentPipeline, hub);
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
    return useResolvedSink(variables.resolve(sinkName), variables, metadataProvider, log, hub);
  }

  /** Uses and holds a sink by its resolved name; see {@link #useSink}. */
  private static String useResolvedSink(
      String name,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      ILogChannel log,
      LineageHub hub) {
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
      // Nowhere to remember them, so release() can't let go of them later: let go now, or each
      // call would hold the sinks once more, and an edited sink's old version would never retire.
      Set<String> sinks = resolver.get();
      hub.release(sinks);
      return Collections.unmodifiableSet(sinks);
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
