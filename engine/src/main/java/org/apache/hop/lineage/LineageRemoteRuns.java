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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.IExtensionData;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * The lineage of a pipeline or workflow a client runs on a Hop Server. The client passes its run
 * with the variables it sends: its run id, its file, the run that started it and the names of its
 * lineage sinks. The server reports the run it starts under that identity, to those sinks besides
 * its own, so its transforms and their datasets land on the client's run. The server says so in its
 * answer; only then does the client leave the run's own events to the server. A server that doesn't
 * know this ignores the variable, and both report as before.
 */
public final class LineageRemoteRuns {

  /** The variable a client passes its run in, as JSON. */
  public static final String VAR_REMOTE_RUN = "HOP_LINEAGE_REMOTE_RUN";

  /** On a server's run: the client's run it reports as. */
  static final String ADOPTED_KEY = "lineage.remote.adopted";

  /** On a client's run: the run id it passed. */
  static final String PASSED_RUN_ID_KEY = "lineage.remote.passedRunId";

  /** On a client's run: true once the server said it reports the run. */
  static final String HANDED_OVER_KEY = "lineage.remote.handedOver";

  /** On a client's workflow run: set while it is being submitted, before the server answered. */
  static final String SUBMITTING_KEY = "lineage.remote.submitting";

  /** On a client's workflow run: its start, held back until the server answered. */
  static final String START_HELD_KEY = "lineage.remote.startHeld";

  private static final ObjectMapper MAPPER = HopJson.newMapper();

  private LineageRemoteRuns() {}

  /**
   * A run as the client passes it: what the server's run reports instead of its own.
   *
   * @param runId the client's run id
   * @param filename the client's file of the pipeline or workflow
   * @param portableFilename that file as the client's lineage names it ({@code
   *     ${PROJECT_HOME}/...})
   * @param parent the attributes that name the run that started the client's run
   * @param sinks the names of the lineage sinks the client's run reports to
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record RemoteRun(
      String runId,
      String filename,
      String portableFilename,
      Map<String, String> parent,
      List<String> sinks) {}

  // -----------------------------------------------------------------------------------------------
  // Client
  // -----------------------------------------------------------------------------------------------

  /**
   * Adds the client's pipeline run to the variables sent to the server, in place of one the run
   * inherited. Lineage never stops a run: when the run can't be described, nothing is passed.
   */
  public static void pass(IPipelineEngine<PipelineMeta> pipeline, Map<String, String> variables) {
    variables.remove(VAR_REMOTE_RUN);
    try {
      LineageContext context = LineageRunLifecycleEmitter.pipelineRunContext(pipeline).build();
      pass(pipeline, context, LineageSinkRoutes.namesOf(pipeline), variables);
    } catch (Exception e) {
      logDetailed(pipeline.getLogChannel(), e);
    }
  }

  /** Adds the client's workflow run to the variables sent to the server; see {@link #pass}. */
  public static void pass(IWorkflowEngine<WorkflowMeta> workflow, Map<String, String> variables) {
    variables.remove(VAR_REMOTE_RUN);
    try {
      LineageContext context = LineageRunLifecycleEmitter.workflowRunContext(workflow).build();
      pass(workflow, context, LineageSinkRoutes.namesOf(workflow), variables);
    } catch (Exception e) {
      logDetailed(workflow.getLogChannel(), e);
    }
  }

  private static void pass(
      IExtensionData run, LineageContext context, List<String> sinks, Map<String, String> variables)
      throws Exception {
    if (Utils.isEmpty(context.getLogChannelId())) {
      return;
    }
    Map<String, String> parent = new LinkedHashMap<>();
    context
        .getAttributes()
        .forEach(
            (key, value) -> {
              if (LineageRunLifecycleEmitter.PARENT_ATTRIBUTES.contains(key)) {
                parent.put(key, value);
              }
            });
    RemoteRun remoteRun =
        new RemoteRun(
            context.getLogChannelId(),
            context.getHopFilename(),
            context.getHopFilenamePortableKey(),
            parent,
            sinks);
    variables.put(VAR_REMOTE_RUN, MAPPER.writeValueAsString(remoteRun));
    data(run).put(PASSED_RUN_ID_KEY, remoteRun.runId());
  }

  /**
   * Records the server's answer: it reports the run when it names the run id the client passed.
   *
   * @param serverRunId the run id the server reports under, null from a server that doesn't
   */
  public static void answered(IExtensionData run, String serverRunId) {
    Object passed = data(run).get(PASSED_RUN_ID_KEY);
    data(run)
        .put(
            HANDED_OVER_KEY,
            passed != null && !Utils.isEmpty(serverRunId) && passed.equals(serverRunId));
  }

  /** Whether the server reports this client run, so the client leaves the run's events to it. */
  public static boolean isHandedOver(IExtensionData run) {
    return Boolean.TRUE.equals(data(run).get(HANDED_OVER_KEY));
  }

  /**
   * A client workflow fires its start before it is sent, when it is not known yet whether the
   * server reports the run: its start waits until {@link #submitted}.
   */
  public static void submitting(IWorkflowEngine<WorkflowMeta> workflow) {
    data(workflow).put(SUBMITTING_KEY, Boolean.TRUE);
  }

  /** Whether the start of this workflow waits for the server's answer; see {@link #submitting}. */
  static boolean holdsStart(IWorkflowEngine<WorkflowMeta> workflow) {
    if (Boolean.TRUE.equals(data(workflow).get(SUBMITTING_KEY))) {
      data(workflow).put(START_HELD_KEY, Boolean.TRUE);
      return true;
    }
    return false;
  }

  /**
   * The workflow was sent, or sending it failed: its held start goes out now, to the sinks the
   * client still reports to.
   */
  public static void submitted(IWorkflowEngine<WorkflowMeta> workflow) {
    data(workflow).remove(SUBMITTING_KEY);
    if (data(workflow).remove(START_HELD_KEY) != null) {
      LineageRunLifecycleEmitter.emitWorkflow(workflow, RunLifecyclePhase.STARTED, null);
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Server
  // -----------------------------------------------------------------------------------------------

  /**
   * Takes over the client's run, when the variables the server got name one: the server's run then
   * reports under the client's run id, file and parent, also to the client's sinks. Kept with the
   * run, not in its variables, so the runs it starts are not taken for the client's.
   *
   * @return the client's run id, or null when there is no run to take over
   */
  public static String adopt(IExtensionData run, Map<String, String> variables, ILogChannel log) {
    String json = variables == null ? null : variables.get(VAR_REMOTE_RUN);
    if (Utils.isEmpty(json)) {
      return null;
    }
    try {
      RemoteRun remoteRun = MAPPER.readValue(json, RemoteRun.class);
      if (Utils.isEmpty(remoteRun.runId())) {
        return null;
      }
      data(run).put(ADOPTED_KEY, remoteRun);
      return remoteRun.runId();
    } catch (Exception e) {
      logDetailed(log, e);
      return null;
    }
  }

  /** The client's run a server's run reports as, or null. */
  public static RemoteRun adopted(IExtensionData run) {
    return data(run).get(ADOPTED_KEY) instanceof RemoteRun remoteRun ? remoteRun : null;
  }

  /** Where a run keeps what this class records; a run without one keeps nothing. */
  private static Map<String, Object> data(IExtensionData run) {
    Map<String, Object> data = run == null ? null : run.getExtensionDataMap();
    return data != null ? data : new HashMap<>();
  }

  /** The client's run id a server's run reports under, for the server's answer; or null. */
  public static String adoptedRunId(IExtensionData run) {
    RemoteRun remoteRun = adopted(run);
    return remoteRun != null ? remoteRun.runId() : null;
  }

  /** The run id a pipeline run reports under: the client's when it took one over. */
  public static String runIdOf(IPipelineEngine<PipelineMeta> pipeline) {
    RemoteRun remoteRun = adopted(pipeline);
    return remoteRun != null ? remoteRun.runId() : pipeline.getLogChannelId();
  }

  /** The run id a workflow run reports under: the client's when it took one over. */
  public static String runIdOf(IWorkflowEngine<WorkflowMeta> workflow) {
    RemoteRun remoteRun = adopted(workflow);
    return remoteRun != null ? remoteRun.runId() : workflow.getLogChannelId();
  }

  private static void logDetailed(ILogChannel log, Exception e) {
    if (log != null && log.isDetailed()) {
      log.logDetailed("Unable to pass this run's lineage to the Hop Server: " + e.getMessage());
    }
  }
}
