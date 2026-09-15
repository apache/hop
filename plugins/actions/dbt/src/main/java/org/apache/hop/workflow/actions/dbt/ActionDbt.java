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

package org.apache.hop.workflow.actions.dbt;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.LineageVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

/**
 * Workflow action that runs a dbt Core operation against a referenced {@link DbtProject} by
 * shelling out to the dbt CLI. Hop-native value-adds over a raw Shell call: environment→target
 * mapping, a Hop variable→{@code --vars} bridge, secret injection from Hop's resolvers into the dbt
 * run env, and OpenLineage run/dataset stitching into the Hop lineage graph.
 */
@Action(
    id = "DBT",
    name = "i18n::ActionDbt.Name",
    description = "i18n::ActionDbt.Description",
    image = "dbt.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Scripting",
    keywords = "i18n::ActionDbt.keyword",
    documentationUrl = "/workflow/actions/dbt.html")
public class ActionDbt extends ActionBase implements IAction {

  /** Hop variables read from the OpenLineage sink configuration when stitching dbt's lineage. */
  private static final String VAR_OPENLINEAGE_URL = "HOP_LINEAGE_OPENLINEAGE_URL";

  private static final String VAR_OPENLINEAGE_NAMESPACE = "HOP_LINEAGE_OPENLINEAGE_NAMESPACE";
  private static final String VAR_OPENLINEAGE_API_KEY = "HOP_LINEAGE_OPENLINEAGE_API_KEY";

  /** Namespace the Hop OpenLineage sink falls back to, so both sides agree when it is unset. */
  private static final String DEFAULT_OPENLINEAGE_NAMESPACE = "hop";

  /** dbt writes its artifacts here unless {@code DBT_TARGET_PATH} says otherwise. */
  private static final String DEFAULT_TARGET_PATH = "target";

  private static final String VAR_DBT_TARGET_PATH = "DBT_TARGET_PATH";

  /** How often a running dbt process is checked for workflow-stop and timeout. */
  private static final long POLL_INTERVAL_MS = 250L;

  /** Grace period between a polite destroy() and destroyForcibly() when killing dbt. */
  private static final long KILL_GRACE_MS = 5_000L;

  /** Name of the referenced dbt-project metadata object. */
  @HopMetadataProperty(key = "dbt_project")
  private String dbtProjectName;

  @HopMetadataProperty(key = "operation")
  private String operation;

  @HopMetadataProperty(key = "select")
  private String select;

  @HopMetadataProperty(key = "exclude")
  private String exclude;

  @HopMetadataProperty(key = "target")
  private String target;

  @HopMetadataProperty(key = "threads")
  private String threads;

  @HopMetadataProperty(key = "full_refresh")
  private boolean fullRefresh;

  @HopMetadataProperty(key = "emit_openlineage")
  private boolean emitOpenLineage;

  /** Optional wall-clock limit in seconds; blank or 0 waits for dbt indefinitely. */
  @HopMetadataProperty(key = "timeout")
  private String timeout;

  @HopMetadataProperty(groupKey = "vars", key = "var")
  private List<DbtNameValue> vars;

  @HopMetadataProperty(groupKey = "env_vars", key = "env_var")
  private List<DbtNameValue> envVars;

  public ActionDbt() {
    this("");
  }

  public ActionDbt(String name) {
    super(name, "");
    this.operation = DbtOperation.RUN.getCode();
    this.vars = new ArrayList<>();
    this.envVars = new ArrayList<>();
  }

  private static List<DbtNameValue> copyPairs(List<DbtNameValue> source) {
    List<DbtNameValue> copy = new ArrayList<>();
    if (source != null) {
      for (DbtNameValue pair : source) {
        copy.add(new DbtNameValue(pair.getName(), pair.getValue()));
      }
    }
    return copy;
  }

  // ----- Execution -----

  @Override
  public Result execute(Result previousResult, int nr) throws HopException {
    Result result = previousResult;
    result.setResult(false);
    result.setNrErrors(1);

    DbtProject project = loadProject();
    if (project == null) {
      logError("No dbt project metadata named '" + resolve(dbtProjectName) + "' was found");
      return result;
    }

    long timeoutMs;
    try {
      timeoutMs = timeoutMillis();
    } catch (NumberFormatException e) {
      logError("Invalid dbt timeout '" + resolve(timeout) + "': expected a number of seconds");
      return result;
    }

    DbtOperation op = DbtOperation.fromCode(operation);
    boolean lineage = emitOpenLineage;
    String executable =
        lineage
            ? blankToDefault(project.getDbtOlExecutable(), "dbt-ol")
            : blankToDefault(project.getDbtExecutable(), "dbt");

    String projectDir = resolve(project.getProjectDirectory());
    String effectiveTarget =
        !Utils.isEmpty(target) ? resolve(target) : resolve(project.getDefaultTarget());

    DbtCommandBuilder builder =
        new DbtCommandBuilder()
            .executable(resolve(executable))
            .operation(op)
            .projectDir(projectDir)
            .profilesDir(resolve(project.getProfilesDirectory()))
            .target(effectiveTarget)
            .select(resolve(select))
            .exclude(resolve(exclude))
            .threads(resolve(threads))
            .fullRefresh(fullRefresh);

    for (DbtNameValue v : vars) {
      builder.var(resolve(v.getName()), resolve(v.getValue()));
    }
    // Secret injection: values are resolved through Hop's variable + secret resolvers
    // (Vault / Azure Key Vault) so credentials never sit in plaintext profiles.
    for (DbtNameValue e : envVars) {
      builder.envVar(resolve(e.getName()), resolve(e.getValue()));
    }
    if (lineage) {
      applyOpenLineageEnv(builder);
    }

    List<String> command = builder.buildCommand();
    Map<String, String> envAdditions = builder.buildEnv();

    // Vars are secret-resolved by now, so the logged argv is the masked one.
    logBasic("Running dbt: " + String.join(" ", builder.buildLoggableCommand()));

    Path runResultsFile = runResultsFile(projectDir);
    // dbt only rewrites run_results.json once it gets far enough. Removing it up front means a
    // run that dies early can never be reported with the previous run's node results.
    deleteStaleRunResults(runResultsFile);

    RunOutcome outcome;
    try {
      outcome = runProcess(command, envAdditions, projectDir, timeoutMs);
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      logError("Failed to invoke dbt executable '" + command.get(0) + "'", e);
      return result;
    }

    DbtRunResult runResult = parseAndLogResults(runResultsFile);
    boolean failed =
        outcome.abortReason() != null
            || outcome.exitCode() != 0
            || (runResult != null && runResult.hasFailures());
    if (failed) {
      logError(
          "dbt "
              + op.getCode()
              + " failed ("
              + (outcome.abortReason() != null ? outcome.abortReason() + ", " : "")
              + "exit="
              + outcome.exitCode()
              + (runResult != null ? ", failedNodes=" + runResult.countFailures() : "")
              + ")");
      result.setResult(false);
      result.setNrErrors(1);
    } else {
      logBasic("dbt " + op.getCode() + " succeeded (exit=0)");
      result.setResult(true);
      result.setNrErrors(0);
    }
    return result;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (Utils.isEmpty(dbtProjectName)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR, "No dbt project is referenced by this action", this));
    } else if (metadataProvider != null) {
      try {
        DbtProject project =
            metadataProvider
                .getSerializer(DbtProject.class)
                .load(variables == null ? dbtProjectName : variables.resolve(dbtProjectName));
        if (project == null) {
          remarks.add(
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  "dbt project '" + dbtProjectName + "' does not exist",
                  this));
        } else if (Utils.isEmpty(project.getProjectDirectory())) {
          remarks.add(
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  "dbt project '" + project.getName() + "' has no project directory",
                  this));
        } else {
          remarks.add(
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  "dbt project '" + project.getName() + "' was found",
                  this));
        }
      } catch (HopException e) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                "Unable to load dbt project '" + dbtProjectName + "': " + e.getMessage(),
                this));
      }
    }

    if (!Utils.isEmpty(operation) && DbtOperation.fromNullableCode(operation) == null) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              "Unknown dbt operation '" + operation + "', 'run' will be used instead",
              this));
    }
    checkPositiveNumber(remarks, threads, "threads");
    checkPositiveNumber(remarks, timeout, "timeout");
    for (DbtNameValue var : vars) {
      if (Utils.isEmpty(var.getName())) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_WARNING,
                "A dbt variable has no name and is ignored",
                this));
      }
    }
    for (DbtNameValue env : envVars) {
      if (Utils.isEmpty(env.getName())) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_WARNING,
                "An environment variable has no name and is ignored",
                this));
      }
    }
  }

  /** Flags a non-numeric literal; a variable expression can only be judged at run time. */
  private void checkPositiveNumber(List<ICheckResult> remarks, String value, String field) {
    if (Utils.isEmpty(value) || value.contains("${")) {
      return;
    }
    try {
      if (Long.parseLong(value.trim()) < 0) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                "The " + field + " value must not be negative",
                this));
      }
    } catch (NumberFormatException e) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              "The " + field + " value '" + value + "' is not a number",
              this));
    }
  }

  /** Resolved timeout in milliseconds; 0 means "wait for dbt however long it takes". */
  private long timeoutMillis() {
    String resolved = resolve(timeout);
    if (Utils.isEmpty(resolved)) {
      return 0L;
    }
    long seconds = Long.parseLong(resolved.trim());
    return seconds <= 0 ? 0L : TimeUnit.SECONDS.toMillis(seconds);
  }

  /**
   * Sets the OpenLineage env so dbt emits into the SAME collector and as a CHILD of the Hop run.
   * The values are read with {@link #getVariable(String)} rather than resolved from a {@code
   * ${...}} expression: Hop leaves an undefined variable in place, which would hand dbt the literal
   * string {@code ${HOP_LINEAGE_OPENLINEAGE_URL}} as its collector URL.
   */
  // Package-private so the env mapping can be unit-tested without spawning dbt.
  void applyOpenLineageEnv(DbtCommandBuilder builder) {
    IVariables lineage = lineageVariables();
    String url = lineage.getVariable(VAR_OPENLINEAGE_URL);
    if (Utils.isEmpty(url)) {
      logBasic(
          "WARNING: "
              + VAR_OPENLINEAGE_URL
              + " is not set, so dbt is left to its own OpenLineage configuration and its events"
              + " may not reach the same collector as Hop's");
    } else {
      // Hop's variable is the full collector endpoint, but OpenLineage's client reads
      // OPENLINEAGE_URL as the server root and appends the endpoint itself. Handing it Hop's
      // value unchanged makes dbt post to <host>/api/v1/api/v1/lineage, where every event is
      // silently lost, so the two parts are passed separately.
      builder.envVar("OPENLINEAGE_URL", collectorBaseUrl(url));
      String endpoint = collectorEndpoint(url);
      if (!Utils.isEmpty(endpoint)) {
        builder.envVar("OPENLINEAGE_ENDPOINT", endpoint);
      }
    }

    // The Hop sink authenticates with the same key, so a secured collector needs it here too.
    String apiKey = lineage.getVariable(VAR_OPENLINEAGE_API_KEY);
    if (!Utils.isEmpty(apiKey)) {
      builder.envVar("OPENLINEAGE_API_KEY", apiKey);
    }

    String namespace = lineage.getVariable(VAR_OPENLINEAGE_NAMESPACE);
    if (Utils.isEmpty(namespace)) {
      namespace = DEFAULT_OPENLINEAGE_NAMESPACE;
    }
    builder.envVar("OPENLINEAGE_NAMESPACE", namespace);

    // ParentRunFacet: namespace/job/run_id (see the "Run identity" section of the OpenLineage
    // dataset-identity page in the Hop user manual).
    String runId = parentRunId();
    String jobName = parentJobName();
    if (!Utils.isEmpty(runId) && !Utils.isEmpty(jobName)) {
      builder.envVar("OPENLINEAGE_PARENT_ID", namespace + "/" + jobName + "/" + runId);
    }
  }

  /**
   * The settings the OpenLineage sink itself runs on. They are deliberately not read from this
   * action's own variable space: the sink resolves them through {@link
   * LineageVariables#engineVariables()}, which overlays the {@code HOP_LINEAGE_*} OS environment
   * variables that a workflow variable space does not carry. Reading them anywhere else lets dbt
   * end up pointed at a different collector than Hop's own events, which is the one thing this
   * feature exists to prevent.
   */
  IVariables lineageVariables() {
    return LineageVariables.engineVariables();
  }

  /** The {@code scheme://host[:port]} part of the collector URL, which dbt wants on its own. */
  static String collectorBaseUrl(String url) {
    URI uri = toUri(url);
    if (uri == null || uri.getScheme() == null || uri.getHost() == null) {
      return url.trim();
    }
    StringBuilder base = new StringBuilder(uri.getScheme()).append("://").append(uri.getHost());
    if (uri.getPort() != -1) {
      base.append(':').append(uri.getPort());
    }
    return base.toString();
  }

  /**
   * The path part of the collector URL, without surrounding slashes - what OpenLineage calls the
   * endpoint. Empty when the URL is a bare host, in which case dbt's own default applies.
   */
  static String collectorEndpoint(String url) {
    URI uri = toUri(url);
    if (uri == null || uri.getScheme() == null || uri.getHost() == null) {
      return "";
    }
    String path = uri.getPath();
    if (path == null) {
      return "";
    }
    return path.replaceAll("^/+", "").replaceAll("/+$", "");
  }

  private static URI toUri(String url) {
    try {
      return URI.create(url.trim());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private String parentRunId() {
    if (getParentWorkflow() != null && getParentWorkflow().getLogChannelId() != null) {
      return getParentWorkflow().getLogChannelId();
    }
    return getLogChannelId();
  }

  private String parentJobName() {
    if (getParentWorkflow() != null && getParentWorkflow().getWorkflowName() != null) {
      return getParentWorkflow().getWorkflowName();
    }
    if (getParentWorkflowMeta() != null) {
      return getParentWorkflowMeta().getName();
    }
    return getName();
  }

  /** How a dbt process ended: its exit code plus, when Hop cut it short, why. */
  private record RunOutcome(int exitCode, String abortReason) {}

  /**
   * Runs dbt, pumping its output into the Hop log on a separate thread while this thread polls for
   * a workflow stop or a timeout. A dbt run that hangs on an unreachable warehouse must not pin the
   * workflow forever, and stopping the workflow must actually kill dbt.
   */
  private RunOutcome runProcess(
      List<String> command, Map<String, String> envAdditions, String projectDir, long timeoutMs)
      throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder(command);
    if (!Utils.isEmpty(projectDir)) {
      pb.directory(new File(projectDir));
    }
    pb.environment().putAll(envAdditions);
    pb.redirectErrorStream(true);
    Process process = pb.start();

    Thread pump = new Thread(() -> pumpOutput(process), "dbt-output-" + getName());
    pump.setDaemon(true);
    pump.start();

    String abortReason = null;
    try {
      long deadline = timeoutMs > 0 ? System.currentTimeMillis() + timeoutMs : Long.MAX_VALUE;
      while (!process.waitFor(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
        if (isWorkflowStopped()) {
          abortReason = "stopped by the workflow";
          break;
        }
        if (System.currentTimeMillis() >= deadline) {
          abortReason = "timed out after " + TimeUnit.MILLISECONDS.toSeconds(timeoutMs) + "s";
          break;
        }
      }
      if (abortReason != null) {
        logError("Killing dbt: " + abortReason);
        kill(process);
      }
    } finally {
      // Destroying the process closes the stream, so the pump ends on its own.
      pump.join(TimeUnit.SECONDS.toMillis(2));
    }
    return new RunOutcome(process.waitFor(), abortReason);
  }

  private void pumpOutput(Process process) {
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        logBasic("[dbt] " + line);
      }
    } catch (IOException e) {
      logDetailed("Stopped reading dbt output: " + e.getMessage());
    }
  }

  /** Kills dbt and the processes it spawned (dbt-ol is a wrapper around dbt itself). */
  private void kill(Process process) throws InterruptedException {
    // Collected before anything is destroyed: once the parent dies its children are reparented
    // and descendants() no longer reports them.
    List<ProcessHandle> descendants = process.descendants().toList();
    descendants.forEach(ProcessHandle::destroy);
    process.destroy();
    if (!process.waitFor(KILL_GRACE_MS, TimeUnit.MILLISECONDS)) {
      logError("dbt did not exit after " + KILL_GRACE_MS + "ms, forcing it");
      descendants.forEach(ProcessHandle::destroyForcibly);
      process.destroyForcibly();
      process.waitFor();
    }
  }

  private boolean isWorkflowStopped() {
    return getParentWorkflow() != null && getParentWorkflow().isStopped();
  }

  /**
   * Location of {@code run_results.json}. dbt writes to {@code target/} unless {@code
   * DBT_TARGET_PATH} is set, and this action lets the env-var list carry that override.
   */
  private Path runResultsFile(String projectDir) {
    if (Utils.isEmpty(projectDir)) {
      return null;
    }
    String targetPath = DEFAULT_TARGET_PATH;
    for (DbtNameValue env : envVars) {
      if (VAR_DBT_TARGET_PATH.equals(resolve(env.getName()))) {
        String value = resolve(env.getValue());
        if (!Utils.isEmpty(value)) {
          targetPath = value;
        }
      }
    }
    return Path.of(projectDir, targetPath, "run_results.json");
  }

  private void deleteStaleRunResults(Path runResultsFile) {
    if (runResultsFile == null) {
      return;
    }
    try {
      if (Files.deleteIfExists(runResultsFile)) {
        logDetailed("Removed the previous run's " + runResultsFile);
      }
    } catch (IOException e) {
      logDetailed("Could not remove " + runResultsFile + ": " + e.getMessage());
    }
  }

  private DbtRunResult parseAndLogResults(Path runResultsFile) {
    if (runResultsFile == null) {
      return null;
    }
    try {
      DbtRunResult runResult = DbtRunResult.fromFile(runResultsFile);
      for (DbtNodeResult node : runResult.getNodes()) {
        // Structured per-model fields (key=value), not a text blob.
        logBasic(
            "dbt.node"
                + " unique_id="
                + nullToDash(node.getUniqueId())
                + " status="
                + nullToDash(node.getStatus())
                + " execution_time="
                + String.format(Locale.ROOT, "%.3f", node.getExecutionTime())
                + " relation="
                + nullToDash(node.getRelationName())
                + (node.getMessage() != null ? " message=" + node.getMessage() : ""));
      }
      logBasic(
          "dbt.summary nodes="
              + runResult.getNodes().size()
              + " failures="
              + runResult.countFailures()
              + " elapsed_time="
              + String.format(Locale.ROOT, "%.3f", runResult.getElapsedTime()));
      return runResult;
    } catch (IOException e) {
      logDetailed("Could not read dbt run_results.json: " + e.getMessage());
      return null;
    }
  }

  private DbtProject loadProject() throws HopException {
    if (Utils.isEmpty(dbtProjectName)) {
      return null;
    }
    IHopMetadataSerializer<DbtProject> serializer =
        getMetadataProvider().getSerializer(DbtProject.class);
    return serializer.load(resolve(dbtProjectName));
  }

  private static String blankToDefault(String value, String def) {
    return Utils.isEmpty(value) ? def : value;
  }

  private static String nullToDash(String s) {
    return s == null ? "-" : s;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  // ----- getters / setters -----

  public String getDbtProjectName() {
    return dbtProjectName;
  }

  public void setDbtProjectName(String dbtProjectName) {
    this.dbtProjectName = dbtProjectName;
  }

  public String getOperation() {
    return operation;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public String getSelect() {
    return select;
  }

  public void setSelect(String select) {
    this.select = select;
  }

  public String getExclude() {
    return exclude;
  }

  public void setExclude(String exclude) {
    this.exclude = exclude;
  }

  public String getTarget() {
    return target;
  }

  public void setTarget(String target) {
    this.target = target;
  }

  public String getThreads() {
    return threads;
  }

  public void setThreads(String threads) {
    this.threads = threads;
  }

  public boolean isFullRefresh() {
    return fullRefresh;
  }

  public void setFullRefresh(boolean fullRefresh) {
    this.fullRefresh = fullRefresh;
  }

  public boolean isEmitOpenLineage() {
    return emitOpenLineage;
  }

  public void setEmitOpenLineage(boolean emitOpenLineage) {
    this.emitOpenLineage = emitOpenLineage;
  }

  public String getTimeout() {
    return timeout;
  }

  public void setTimeout(String timeout) {
    this.timeout = timeout;
  }

  public List<DbtNameValue> getVars() {
    return vars;
  }

  public void setVars(List<DbtNameValue> vars) {
    this.vars = vars;
  }

  public List<DbtNameValue> getEnvVars() {
    return envVars;
  }

  public void setEnvVars(List<DbtNameValue> envVars) {
    this.envVars = envVars;
  }
}
