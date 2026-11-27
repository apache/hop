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

package org.apache.hop.airflow.bundle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.airflow.engine.AirflowDagTasks;
import org.apache.hop.airflow.launcher.HopAirflowBundle;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.hop.Hop;
import org.apache.hop.hop.plugin.HopCommand;
import org.apache.hop.hop.plugin.IHopCommand;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import picocli.CommandLine;

/** {@code hop airflow-bundle}: makes a project a DAG bundle for Apache Airflow. */
@Getter
@Setter
@CommandLine.Command(
    mixinStandardHelpOptions = true,
    description =
        "Write the stub jar Apache Airflow needs to run the workflows of a project as DAGs. Without"
            + " --target the stub goes into the project folder, to commit for a Git DAG bundle.")
@HopCommand(id = "airflow-bundle", description = "Make a project a DAG bundle for Apache Airflow")
public class AirflowBundleCommand
    implements Callable<Integer>, IHopCommand, IHasHopMetadataProvider {

  @CommandLine.Option(
      names = {"-t", "--target"},
      description =
          "Copy the project to this folder and write the stub there, for a folder DAG bundle. The"
              + " folder must be empty or hold an earlier bundle, which is replaced.")
  private String targetFolder;

  @CommandLine.Option(
      names = {"-hh", "--worker-hop-home"},
      description =
          "Where Hop is installed on the Airflow workers. Default: "
              + AirflowBundleWriter.DEFAULT_WORKER_HOP_HOME)
  private String workerHopHome = AirflowBundleWriter.DEFAULT_WORKER_HOP_HOME;

  @CommandLine.Option(
      names = {"-r", "--runconfig"},
      description = "The workflow run configuration the DAGs use. Default: local")
  private String runConfiguration;

  @CommandLine.Option(
      names = {"-l", "--level"},
      description = "The log level of the runs. Default: Basic")
  private String logLevel;

  @CommandLine.Option(
      names = {"-w", "--workflows"},
      split = ",",
      description =
          "Glob patterns of the workflows that become DAGs, relative to the project, for example"
              + " 'main-*.hwf,etl/**'. Default: every workflow")
  private List<String> workflowPatterns = new ArrayList<>();

  @CommandLine.Option(
      names = {"-ef", "--environment-files"},
      split = ",",
      description = "Environment configuration files for the runs, relative to the project folder")
  private List<String> environmentFiles = new ArrayList<>();

  @CommandLine.Option(
      names = {"--dag-id-prefix"},
      description = "The start of every DAG id. Default: the project name and two underscores")
  private String dagIdPrefix;

  @CommandLine.Option(
      names = {"--tasks"},
      description =
          "The tasks of the DAGs: PER_ACTION, one task per action (default), or WHOLE_WORKFLOW,"
              + " one task that runs the whole workflow")
  private String tasks;

  private CommandLine cmd;
  private IVariables variables;
  private MultiMetadataProvider metadataProvider;
  private ILogChannel log;

  @Override
  public void initialize(
      CommandLine cmd, IVariables variables, MultiMetadataProvider metadataProvider)
      throws HopException {
    this.cmd = cmd;
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.log = new LogChannel("Airflow bundle");
    // The project and environment options (-j, -e), as hop doc has them
    Hop.addMixinPlugins(cmd, ConfigPlugin.CATEGORY_DOC);
  }

  @Override
  public Integer call() {
    try {
      for (Map.Entry<String, Object> mixin : cmd.getMixins().entrySet()) {
        if (mixin.getValue() instanceof IConfigOptions options) {
          options.handleOption(log, this, variables);
        }
      }
      String projectName = variables.getVariable("HOP_PROJECT_NAME");
      String projectHome = variables.getVariable("PROJECT_HOME");
      if (StringUtils.isBlank(projectName) || StringUtils.isBlank(projectHome)) {
        log.logError("Choose the project to make a DAG bundle of with -j");
        return 1;
      }

      AirflowDagTasks dagTasks;
      try {
        dagTasks = AirflowDagTasks.lookup(tasks);
      } catch (IllegalArgumentException e) {
        log.logError(e.getMessage());
        return 1;
      }

      AirflowBundleWriter writer = new AirflowBundleWriter();
      writer.setProjectName(projectName);
      writer.setProjectHome(projectHome);
      writer.setTargetFolder(variables.resolve(targetFolder));
      writer.setWorkerHopHome(workerHopHome);
      writer.setRunConfiguration(runConfiguration);
      writer.setLogLevel(logLevel);
      writer.setWorkflowPatterns(workflowPatterns);
      writer.setEnvironmentFiles(environmentFiles);
      writer.setDagIdPrefix(dagIdPrefix);
      writer.setTasks(dagTasks);
      writer.setHopVersion(HopEnvironment.class.getPackage().getImplementationVersion());
      AirflowBundleWriter.Result result = writer.write();

      if (result.copied()) {
        log.logBasic(
            "Copied project "
                + projectName
                + " to "
                + result.bundleFolder()
                + " and wrote "
                + HopAirflowBundle.STUB_FILE_NAME
                + ". Airflow can read that folder as a DAG bundle.");
      } else {
        log.logBasic(
            "Wrote "
                + result.stubFile()
                + ". Commit it with the project: Airflow can read the repository as a Git DAG"
                + " bundle.");
      }
      return 0;
    } catch (Exception e) {
      log.logError("Unable to write the Airflow DAG bundle", e);
      return 1;
    }
  }
}
