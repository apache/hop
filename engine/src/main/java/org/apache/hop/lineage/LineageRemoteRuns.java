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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hop.core.IExtensionData;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.hub.LineageSinkTarget;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * The lineage of a pipeline or workflow a client runs on a Hop Server. The client passes its run
 * with the variables it sends: its run id, its file, the run that started it, the names of its
 * lineage sinks and where its sinks configured with variables report to. The server reports the run
 * it starts under that identity, to the client's sinks it can use besides its own, so its
 * transforms and their datasets land on the client's run. The server names those sinks in its
 * answer; once the run started on the server, the client leaves the run's own events for those
 * sinks to the server and keeps reporting to the rest. A server that doesn't know this ignores the
 * variable, and both report as before.
 */
public final class LineageRemoteRuns {

  /** The variable a client passes its run in, as JSON. */
  public static final String VAR_REMOTE_RUN = "HOP_LINEAGE_REMOTE_RUN";

  /** On a server's run: the client's run it reports as. */
  static final String ADOPTED_KEY = "lineage.remote.adopted";

  /** On a server's run: true when it reports to the same variable sinks as the client. */
  static final String ADOPTED_VARIABLE_SINKS_KEY = "lineage.remote.adoptedVariableSinks";

  /** On a client's run: the run id it passed. */
  static final String PASSED_RUN_ID_KEY = "lineage.remote.passedRunId";

  /** On a client's run: what the server reports of it, once the run started there. */
  static final String HANDED_OVER_KEY = "lineage.remote.handedOver";

  /** On a client's workflow run: set while it is being submitted, before the server answered. */
  static final String SUBMITTING_KEY = "lineage.remote.submitting";

  /** On a client's workflow run: when it started, while its start waits for the server's answer. */
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
   * @param variableSinks where the client's sinks configured with variables report to, as a digest
   *     ({@link #variableSinksDigest}); null when it has none
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record RemoteRun(
      String runId,
      String filename,
      String portableFilename,
      Map<String, String> parent,
      List<String> sinks,
      String variableSinks) {}

  /**
   * What a Hop Server answers when it reports a client's run.
   *
   * @param runId the client's run id the server reports under
   * @param sinks the names of the client's sinks the server reports the run to
   * @param variableSinks whether the server's sinks configured with variables report to the same
   *     place as the client's, so the server reports the run there too
   */
  public record Answer(String runId, List<String> sinks, boolean variableSinks) {}

  /** What a server reports of a client's run, kept with the client's run. */
  private record HandedOver(Set<String> sinks, boolean variableSinks) {}

  // -----------------------------------------------------------------------------------------------
  // Client
  // -----------------------------------------------------------------------------------------------

  /**
   * Adds the client's pipeline run to the variables sent to the server, in place of one the run
   * inherited. Lineage never stops a run: when the run can't be described, nothing is passed.
   */
  public static void pass(IPipelineEngine<PipelineMeta> pipeline, Map<String, String> variables) {
    variables.remove(VAR_REMOTE_RUN);
    forget(pipeline);
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
    forget(workflow);
    try {
      LineageContext context = LineageRunLifecycleEmitter.workflowRunContext(workflow).build();
      pass(workflow, context, LineageSinkRoutes.namesOf(workflow), variables);
    } catch (Exception e) {
      logDetailed(workflow.getLogChannel(), e);
    }
  }

  /** Drops what an earlier attempt to send the run recorded, such as another server's answer. */
  private static void forget(IExtensionData run) {
    data(run).remove(PASSED_RUN_ID_KEY);
    data(run).remove(HANDED_OVER_KEY);
  }

  private static void pass(
      IExtensionData run, LineageContext context, List<String> sinks, Map<String, String> variables)
      throws Exception {
    if (Utils.isEmpty(context.getLogChannelId())) {
      return;
    }
    RemoteRun remoteRun =
        new RemoteRun(
            context.getLogChannelId(),
            context.getHopFilename(),
            context.getHopFilenamePortableKey(),
            parentOf(context.getAttributes()),
            sinks,
            variableSinksDigest(LineageHub.getInstance(), LineageVariables.engineVariables()));
    variables.put(VAR_REMOTE_RUN, MAPPER.writeValueAsString(remoteRun));
    data(run).put(PASSED_RUN_ID_KEY, remoteRun.runId());
  }

  /** The attributes that name the run that started a run, and only those. */
  private static Map<String, String> parentOf(Map<String, String> attributes) {
    Map<String, String> parent = new LinkedHashMap<>();
    if (attributes != null) {
      attributes.forEach(
          (key, value) -> {
            if (LineageRunLifecycleEmitter.PARENT_ATTRIBUTES.contains(key) && value != null) {
              parent.put(key, value);
            }
          });
    }
    return parent;
  }

  /**
   * Records the server's answer, once the run started on the server: the server reports the run
   * when it names the run id the client passed. Call it only after the start succeeded, so that a
   * run the server never starts stays the client's to report.
   *
   * @param answer what the server answered, null from a server that doesn't report the run
   */
  public static void answered(IExtensionData run, Answer answer) {
    Object passed = data(run).get(PASSED_RUN_ID_KEY);
    if (passed == null
        || answer == null
        || Utils.isEmpty(answer.runId())
        || !passed.equals(answer.runId())) {
      data(run).remove(HANDED_OVER_KEY);
      return;
    }
    Set<String> sinks = new LinkedHashSet<>();
    if (answer.sinks() != null) {
      answer.sinks().stream().filter(name -> !Utils.isEmpty(name)).forEach(sinks::add);
    }
    data(run).put(HANDED_OVER_KEY, new HandedOver(Set.copyOf(sinks), answer.variableSinks()));
  }

  /** Whether the server reports this client run, to some or all of its sinks. */
  public static boolean isHandedOver(IExtensionData run) {
    return data(run).get(HANDED_OVER_KEY) instanceof HandedOver;
  }

  /**
   * The sinks a client's run reports its own events to: of the run's sinks, those the server does
   * not report the run to.
   *
   * @param sinks the keys of the run's sinks
   */
  static Set<String> clientSinks(IExtensionData run, Set<String> sinks) {
    if (!(data(run).get(HANDED_OVER_KEY) instanceof HandedOver handedOver)
        || handedOver.sinks().isEmpty()) {
      return sinks;
    }
    Set<String> left = new LinkedHashSet<>();
    for (String key : sinks) {
      if (!handedOver.sinks().contains(LineageSinkTarget.nameOf(key))) {
        left.add(key);
      }
    }
    return left;
  }

  /**
   * Whether the sinks configured with variables get a client run's own events: not when the server
   * reports the run to sinks configured the same way.
   */
  static boolean reportsToVariableSinks(IExtensionData run) {
    return !(data(run).get(HANDED_OVER_KEY) instanceof HandedOver handedOver)
        || !handedOver.variableSinks();
  }

  /**
   * A client workflow fires its start before it is sent, when it is not known yet whether the
   * server reports the run: its start waits until {@link #submitted}, and keeps its time.
   */
  public static void submitting(IWorkflowEngine<WorkflowMeta> workflow) {
    data(workflow).put(SUBMITTING_KEY, Boolean.TRUE);
  }

  /** Whether the start of this workflow waits for the server's answer; see {@link #submitting}. */
  static boolean holdsStart(IWorkflowEngine<WorkflowMeta> workflow) {
    if (Boolean.TRUE.equals(data(workflow).get(SUBMITTING_KEY))) {
      data(workflow).put(START_HELD_KEY, System.currentTimeMillis());
      return true;
    }
    return false;
  }

  /**
   * The workflow was sent, or sending it failed: its held start goes out now, with the time it
   * started, to the sinks the client still reports to. Lineage never stops a run: a start that
   * can't be emitted is logged.
   */
  public static void submitted(IWorkflowEngine<WorkflowMeta> workflow) {
    data(workflow).remove(SUBMITTING_KEY);
    if (data(workflow).remove(START_HELD_KEY) instanceof Long startedAt) {
      try {
        LineageRunLifecycleEmitter.emitWorkflow(
            workflow, RunLifecyclePhase.STARTED, null, startedAt);
      } catch (Exception e) {
        ILogChannel log = workflow.getLogChannel();
        if (log != null) {
          log.logError("Unable to report the start of this run to its lineage sinks", e);
        }
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Server
  // -----------------------------------------------------------------------------------------------

  /**
   * Takes over the client's run, when the variables the server got name one: the server's run then
   * reports under the client's run id, file and parent, also to the client's sinks it can use. Kept
   * with the run, not in its variables, so the runs it starts are not taken for the client's.
   *
   * @param run the server's run
   * @param runVariables the server run's variables, to check the client's sinks with
   * @param metadataProvider the server run's metadata, to find the client's sinks in
   * @param variables the variables the client sent
   * @return what the server answers the client, or null when there is no run to take over
   */
  public static Answer adopt(
      IExtensionData run,
      IVariables runVariables,
      IHopMetadataProvider metadataProvider,
      Map<String, String> variables,
      ILogChannel log) {
    return adopt(
        run,
        runVariables,
        metadataProvider,
        variables,
        log,
        LineageHub.getInstance(),
        LineageVariables.engineVariables());
  }

  static Answer adopt(
      IExtensionData run,
      IVariables runVariables,
      IHopMetadataProvider metadataProvider,
      Map<String, String> variables,
      ILogChannel log,
      LineageHub hub,
      IVariables engineVariables) {
    String json = variables == null ? null : variables.get(VAR_REMOTE_RUN);
    if (Utils.isEmpty(json)) {
      return null;
    }
    RemoteRun passed;
    try {
      passed = MAPPER.readValue(json, RemoteRun.class);
    } catch (Exception e) {
      if (log != null) {
        log.logError(
            "Unable to read the client's run from "
                + VAR_REMOTE_RUN
                + " ("
                + e.getMessage()
                + "): this run reports its lineage as a run of its own, and the client reports its"
                + " run without its transforms.");
      }
      return null;
    }
    if (Utils.isEmpty(passed.runId())) {
      return null;
    }
    List<String> sinks =
        LineageSinkRoutes.acceptedSinks(passed.sinks(), runVariables, metadataProvider, log, hub);
    String ownVariableSinks = variableSinksDigest(hub, engineVariables);
    boolean sameVariableSinks =
        ownVariableSinks != null && ownVariableSinks.equals(passed.variableSinks());
    RemoteRun adopted =
        new RemoteRun(
            passed.runId(),
            passed.filename(),
            passed.portableFilename(),
            parentOf(passed.parent()),
            List.copyOf(sinks),
            passed.variableSinks());
    data(run).put(ADOPTED_KEY, adopted);
    data(run).put(ADOPTED_VARIABLE_SINKS_KEY, sameVariableSinks);
    return answerOf(run);
  }

  /** The client's run a server's run reports as, or null. */
  public static RemoteRun adopted(IExtensionData run) {
    return data(run).get(ADOPTED_KEY) instanceof RemoteRun remoteRun ? remoteRun : null;
  }

  /** What a server's run answers the client that sent it, or null when it reports its own run. */
  public static Answer answerOf(IExtensionData run) {
    RemoteRun remoteRun = adopted(run);
    if (remoteRun == null) {
      return null;
    }
    return new Answer(
        remoteRun.runId(),
        remoteRun.sinks() == null ? List.of() : remoteRun.sinks(),
        Boolean.TRUE.equals(data(run).get(ADOPTED_VARIABLE_SINKS_KEY)));
  }

  /** Where a run keeps what this class records; a run without one keeps nothing. */
  private static Map<String, Object> data(IExtensionData run) {
    Map<String, Object> data = run == null ? null : run.getExtensionDataMap();
    return data != null ? data : new HashMap<>();
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

  // -----------------------------------------------------------------------------------------------
  // Both
  // -----------------------------------------------------------------------------------------------

  /**
   * Where the sinks configured with variables report to, as a digest of the sink ids and the {@code
   * HOP_LINEAGE_*_URL} variables. A client and a server with the same digest report to the same
   * place, so only the server reports a run it took over there. Hashed, as a URL may hold a secret.
   *
   * @return the digest, or null when no sinks are configured with variables
   */
  static String variableSinksDigest(LineageHub hub, IVariables engineVariables) {
    if (hub == null || !hub.isEnabled() || engineVariables == null) {
      return null;
    }
    Map<String, String> destination = new TreeMap<>();
    for (String name : engineVariables.getVariableNames()) {
      if (LineageVariables.HOP_LINEAGE_SINK_IDS.equals(name)
          || (name.startsWith("HOP_LINEAGE_") && name.endsWith("_URL"))) {
        String value = engineVariables.getVariable(name);
        if (!Utils.isEmpty(value)) {
          destination.put(name, value.trim());
        }
      }
    }
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      for (Map.Entry<String, String> entry : destination.entrySet()) {
        digest.update(entry.getKey().getBytes(StandardCharsets.UTF_8));
        digest.update((byte) 0);
        digest.update(entry.getValue().getBytes(StandardCharsets.UTF_8));
        digest.update((byte) 1);
      }
      return HexFormat.of().formatHex(digest.digest(), 0, 16);
    } catch (Exception e) {
      return null;
    }
  }

  private static void logDetailed(ILogChannel log, Exception e) {
    if (log != null && log.isDetailed()) {
      log.logDetailed("Unable to pass this run's lineage to the Hop Server: " + e.getMessage());
    }
  }
}
