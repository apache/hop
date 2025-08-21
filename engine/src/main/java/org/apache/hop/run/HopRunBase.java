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
 *
 */

package org.apache.hop.run;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.IExecutionConfiguration;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.FileLoggingEventListener;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.parameters.INamedParameterDefinitions;
import org.apache.hop.core.parameters.INamedParameters;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEngineFactory;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.ParameterException;

@Getter
@Setter
public abstract class HopRunBase implements Runnable, IHasHopMetadataProvider {
  @CommandLine.Option(
      names = {"-f", "--file"},
      description = "The filename of the workflow or pipeline to run")
  protected String filename;

  @CommandLine.Option(
      names = {"-l", "--level"},
      description =
          "The debug level, one of NOTHING, ERROR, MINIMAL, BASIC, DETAILED, DEBUG, ROWLEVEL")
  protected String level;

  @CommandLine.Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Displays this help message and quits.")
  protected boolean helpRequested;

  @CommandLine.Option(
      names = {"-v", "--version"},
      versionHelp = true,
      description = "Print version information and exit")
  protected boolean versionRequested;

  @CommandLine.Option(
      names = {"-lf", "--logfile"},
      description = "The complete filename where hop-run will write the Hop console log")
  protected String logFile;

  @CommandLine.Option(
      names = {"-p", "--parameters"},
      description = "A list of PARAMETER=VALUE pairs")
  protected String[] parameters = null;

  @CommandLine.Option(
      names = {"-ps", "--parameters-separator"},
      description =
          "A character to be used as separator for our list of PARAMETER=VALUE pairs (default is ,)")
  protected String parametersSeparator = null;

  @CommandLine.Option(
      names = {"-s", "--system-properties"},
      description = "A comma separated list of KEY=VALUE pairs",
      split = ",")
  protected String[] systemProperties = null;

  @CommandLine.Option(
      names = {"-r", "--runconfig"},
      description = "The name of the Run Configuration to use")
  protected String runConfigurationName = null;

  @CommandLine.Option(
      names = {"-a", "--startaction"},
      description = "The name of the action where to start a workflow")
  protected String startActionName = null;

  @CommandLine.Option(
      names = {"-m", "--metadata-export"},
      description = "A file containing exported metadata in JSON format")
  protected String metadataExportFile;

  @CommandLine.Option(
      names = {"-o", "--printoptions"},
      description = "Print the used options")
  protected boolean printingOptions = false;

  protected IVariables variables;
  protected String realRunConfigurationName;
  protected String realFilename;
  protected CommandLine cmd;
  protected ILogChannel log;
  protected MultiMetadataProvider metadataProvider;
  protected boolean finishedWithoutError;

  protected HopRunBase() {
    variables = new Variables();
  }

  @Override
  public void run() {
    validateOptions();
    FileLoggingEventListener fileLoggingEventListener = null;

    try {
      log = new LogChannel("HopRun");
      log.setLogLevel(determineLogLevel());
      log.logDetailed("Start of Hop Run");

      // Allow plugins to modify the elements loaded so far, before a pipeline or workflow is even
      // loaded
      //
      ExtensionPointHandler.callExtensionPoint(
          log, variables, HopExtensionPoint.HopRunStart.id, this);

      // Handle the options of the configuration plugins
      //
      Map<String, Object> mixins = cmd.getMixins();
      for (String key : mixins.keySet()) {
        Object mixin = mixins.get(key);
        if (mixin instanceof IConfigOptions configOptions) {
          configOptions.handleOption(log, this, variables);
        }
      }

      // Optionally we can configure metadata to come from a JSON export file.
      //
      String metadataExportFile = variables.resolve(getMetadataExportFile());
      if (StringUtils.isNotEmpty(metadataExportFile)) {
        // Load the JSON from the specified file:
        //
        try (InputStream inputStream = HopVfs.getInputStream(metadataExportFile)) {
          String json = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
          SerializableMetadataProvider exportedProvider = new SerializableMetadataProvider(json);
          metadataProvider.getProviders().add(exportedProvider);

          System.out.println("===> Metadata provider is now: " + metadataProvider.getDescription());
        }
      }

      if (!Utils.isEmpty(logFile)) {
        fileLoggingEventListener = new FileLoggingEventListener(logFile, false);
        HopLogStore.getAppender().addLoggingEventListener(fileLoggingEventListener);
      }

      if (isPipeline()) {
        runPipeline(cmd, log);
      }
      if (isWorkflow()) {
        runWorkflow(cmd, log);
      }

      ExtensionPointHandler.callExtensionPoint(
          log, variables, HopExtensionPoint.HopRunEnd.id, this);
    } catch (Exception e) {
      throw new ExecutionException(
          cmd, "There was an error during execution of file '" + filename + "'", e);
    } finally {
      if (fileLoggingEventListener != null) {
        HopLogStore.getAppender().removeLoggingEventListener(fileLoggingEventListener);
      }
    }
  }

  protected void buildVariableSpace() {
    // Also grabs the system properties from hop.config.
    //
    variables = Variables.getADefaultVariableSpace();
  }

  protected void runPipeline(CommandLine cmd, ILogChannel log) {

    try {
      calculateRealFilename();

      // Run the pipeline with the given filename
      //
      PipelineMeta pipelineMeta = new PipelineMeta(realFilename, metadataProvider, variables);

      // Configure the basic execution settings
      //
      PipelineExecutionConfiguration configuration = new PipelineExecutionConfiguration();

      // Overwrite if the user decided this
      //
      parseOptions(cmd, configuration, pipelineMeta);

      // Do we have a default run configuration?
      // That way the user doesn't have to specify the run configuration name
      //
      if (StringUtils.isEmpty(configuration.getRunConfiguration())) {
        PipelineRunConfiguration defaultRunConfiguration =
            PipelineRunConfiguration.findDefault(metadataProvider);
        if (defaultRunConfiguration != null) {
          configuration.setRunConfiguration(defaultRunConfiguration.getName());
        }
      }

      // Before running, do we print the options?
      //
      if (printingOptions) {
        printOptions(configuration);
      }

      // Now run the pipeline using the run configuration
      //
      runPipeline(cmd, log, configuration, pipelineMeta);
    } catch (Exception e) {
      throw new ExecutionException(
          cmd, "There was an error during execution of pipeline '" + filename + "'", e);
    }
  }

  /** This way we can actually use environment variables to parse the real filename */
  protected void calculateRealFilename() throws HopException {
    realFilename = variables.resolve(filename);

    ExtensionPointHandler.callExtensionPoint(
        log, variables, HopExtensionPoint.HopRunCalculateFilename.id, this);
  }

  protected void runPipeline(
      CommandLine cmd,
      ILogChannel log,
      PipelineExecutionConfiguration configuration,
      PipelineMeta pipelineMeta) {
    try {
      String pipelineRunConfigurationName = variables.resolve(configuration.getRunConfiguration());
      IPipelineEngine<PipelineMeta> pipeline =
          PipelineEngineFactory.createPipelineEngine(
              variables, pipelineRunConfigurationName, metadataProvider, pipelineMeta);
      pipeline.getPipelineMeta().setInternalHopVariables(pipeline);
      pipeline.initializeFrom(null);
      pipeline.setVariables(configuration.getVariablesMap());

      // configure the variables and parameters
      //
      pipeline.copyParametersFromDefinitions(pipelineMeta);
      configureParametersAndVariables(cmd, configuration, pipeline, pipeline);

      pipeline.setLogLevel(configuration.getLogLevel());
      pipeline.setMetadataProvider(metadataProvider);

      pipeline.activateParameters(pipeline);

      log.logMinimal("Starting pipeline: " + pipelineMeta.getFilename());

      // Run it!
      //
      pipeline.prepareExecution();
      pipeline.startThreads();
      pipeline.waitUntilFinished();
      setFinishedWithoutError(pipeline.getResult().getNrErrors() == 0L);
    } catch (Exception e) {
      throw new ExecutionException(cmd, "Error running pipeline locally", e);
    }
  }

  protected void runWorkflow(CommandLine cmd, ILogChannel log) {
    try {
      calculateRealFilename();

      // Run the workflow with the given filename
      //
      WorkflowMeta workflowMeta = new WorkflowMeta(variables, realFilename, metadataProvider);

      // Configure the basic execution settings
      //
      WorkflowExecutionConfiguration configuration = new WorkflowExecutionConfiguration();

      // Overwrite the run configuration with optional command line options
      //
      parseOptions(cmd, configuration, workflowMeta);

      // Do we have a default run configuration?
      // That way the user doesn't have to specify the run configuration name
      //
      if (StringUtils.isEmpty(configuration.getRunConfiguration())) {
        WorkflowRunConfiguration defaultRunConfiguration =
            WorkflowRunConfiguration.findDefault(metadataProvider);
        if (defaultRunConfiguration != null) {
          configuration.setRunConfiguration(defaultRunConfiguration.getName());
        }
      }

      // Start workflow at action
      //
      configuration.setStartActionName(startActionName);

      // Certain Hop plugins rely on this.  Meh.
      //
      ExtensionPointHandler.callExtensionPoint(
          log,
          variables,
          HopExtensionPoint.HopGuiWorkflowBeforeStart.id,
          new Object[] {configuration, null, workflowMeta, null});

      // Before running, do we print the options?
      //
      if (printingOptions) {
        printOptions(configuration);
      }

      runWorkflow(cmd, log, configuration, workflowMeta);

    } catch (Exception e) {
      throw new ExecutionException(
          cmd, "There was an error during execution of workflow '" + filename + "'", e);
    }
  }

  protected void runWorkflow(
      CommandLine cmd,
      ILogChannel log,
      WorkflowExecutionConfiguration configuration,
      WorkflowMeta workflowMeta) {
    try {
      String runConfigurationName = variables.resolve(configuration.getRunConfiguration());
      // Create a logging object to push down the correct loglevel to the Workflow
      //
      LoggingObject workflowLog = new LoggingObject(log);
      workflowLog.setLogLevel(configuration.getLogLevel());

      IWorkflowEngine<WorkflowMeta> workflow =
          WorkflowEngineFactory.createWorkflowEngine(
              variables, runConfigurationName, metadataProvider, workflowMeta, workflowLog);
      workflow.getWorkflowMeta().setInternalHopVariables(workflow);
      workflow.setVariables(configuration.getVariablesMap());

      // Copy the parameter definitions from the metadata, with empty values
      //
      workflow.copyParametersFromDefinitions(workflowMeta);
      configureParametersAndVariables(cmd, configuration, workflow, workflow);

      // Also copy the parameter values over to the variables...
      //
      workflow.activateParameters(workflow);

      // If there is an alternative start action, pass it to the workflow
      //
      if (!Utils.isEmpty(configuration.getStartActionName())) {
        ActionMeta startActionMeta = workflowMeta.findAction(configuration.getStartActionName());

        if (startActionMeta == null) {
          throw new ExecutionException(
              cmd, "Error running workflow, specified start action not found");
        }
        workflow.setStartActionMeta(startActionMeta);
      }

      log.logMinimal("Starting workflow: " + workflowMeta.getFilename());

      workflow.startExecution();
      setFinishedWithoutError(workflow.getResult().getResult());
    } catch (Exception e) {
      throw new ExecutionException(cmd, "Error running workflow locally", e);
    }
  }

  protected void parseOptions(
      CommandLine cmd,
      IExecutionConfiguration configuration,
      INamedParameterDefinitions namedParams)
      throws HopException {

    realRunConfigurationName = variables.resolve(runConfigurationName);
    configuration.setRunConfiguration(realRunConfigurationName);
    configuration.setLogLevel(determineLogLevel());

    // Set variables and parameters...
    //
    parseParametersAndVariables(cmd, configuration, namedParams);
  }

  protected LogLevel determineLogLevel() {
    return LogLevel.lookupCode(variables.resolve(level));
  }

  protected boolean isPipeline() {
    if (StringUtils.isEmpty(filename)) {
      return false;
    }
    return filename.toLowerCase().endsWith(".hpl");
  }

  protected boolean isWorkflow() {
    if (StringUtils.isEmpty(filename)) {
      return false;
    }
    return filename.toLowerCase().endsWith(".hwf");
  }

  /** Set the variables and parameters */
  protected void parseParametersAndVariables(
      CommandLine cmd,
      IExecutionConfiguration configuration,
      INamedParameterDefinitions namedParams) {
    try {
      String[] availableParameters = namedParams.listParameters();

      if (parameters != null) {
        parametersSeparator = parametersSeparator == null ? "," : parametersSeparator;

        for (String parameter : parameters) {
          for (String singleParameter : parameter.split(parametersSeparator)) {
            String[] split = singleParameter.split("=", 2);
            String key = split.length > 0 ? split[0] : null;
            String value = split.length > 1 ? split[1] : null;
            if (value != null && value.startsWith("\"") && value.endsWith("\"")) {
              value = value.substring(1, value.length() - 1);
            }

            if (key != null) {
              // We can work with this.
              //
              if (Const.indexOfString(key, availableParameters) < 0) {
                // A variable
                //
                configuration.getVariablesMap().put(key, value);
              } else {
                // A parameter
                //
                configuration.getParametersMap().put(key, value);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      throw new ExecutionException(
          cmd, "There was an error during execution of pipeline '" + filename + "'", e);
    }
  }

  /**
   * Configure the variables and parameters in the given configuration on the given variable
   * variables and named parameters
   */
  protected void configureParametersAndVariables(
      CommandLine cmd,
      IExecutionConfiguration configuration,
      IVariables variables,
      INamedParameters namedParams) {

    // Copy variables over to the pipeline or workflow
    //
    variables.setVariables(configuration.getVariablesMap());

    // By default, we use the value from the current variables map:
    //
    for (String key : namedParams.listParameters()) {
      String value = variables.getVariable(key);
      if (StringUtils.isNotEmpty(value)) {
        try {
          namedParams.setParameterValue(key, value);
        } catch (UnknownParamException e) {
          throw new ParameterException(cmd, "Unable to set parameter '" + key + "'", e);
        }
      }
    }

    // Possibly override with the parameter values set by the user (-p option)
    //
    for (String key : configuration.getParametersMap().keySet()) {
      String value = configuration.getParametersMap().get(key);
      try {
        namedParams.setParameterValue(key, value);
      } catch (UnknownParamException e) {
        throw new ParameterException(cmd, "Unable to set parameter '" + key + "'", e);
      }
    }
  }

  protected void validateOptions() {
    if (StringUtils.isEmpty(filename)) {
      throw new ParameterException(
          new CommandLine(this), "A filename is needed to run a workflow or pipeline");
    }
  }

  protected void printOptions(IExecutionConfiguration configuration) {
    if (StringUtils.isNotEmpty(realFilename)) {
      log.logMinimal("OPTION: filename : '" + realFilename + "'");
    }
    if (StringUtils.isNotEmpty(configuration.getRunConfiguration())) {
      log.logMinimal("OPTION: run configuration : '" + configuration.getRunConfiguration() + "'");
    }
    log.logMinimal("OPTION: Logging level : " + configuration.getLogLevel().getDescription());

    if (!configuration.getVariablesMap().isEmpty()) {
      log.logMinimal("OPTION: Variables: ");
      for (String variable : configuration.getVariablesMap().keySet()) {
        log.logMinimal("  " + variable + " : '" + configuration.getVariablesMap().get(variable));
      }
    }
    if (!configuration.getParametersMap().isEmpty()) {
      log.logMinimal("OPTION: Parameters: ");
      for (String parameter : configuration.getParametersMap().keySet()) {
        log.logMinimal(
            "OPTION:   " + parameter + " : '" + configuration.getParametersMap().get(parameter));
      }
    }
  }
}
