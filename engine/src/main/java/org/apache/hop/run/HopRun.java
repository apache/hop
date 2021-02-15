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

package org.apache.hop.run;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.IExecutionConfiguration;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.ConfigPluginType;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.parameters.INamedParameterDefinitions;
import org.apache.hop.core.parameters.INamedParameters;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.server.HopServer;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEngineFactory;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HopRun implements Runnable, IHasHopMetadataProvider {

  @Option(
      names = {"-f", "--file"},
      description = "The filename of the workflow or pipeline to run")
  private String filename;

  @Option(
      names = {"-l", "--level"},
      description = "The debug level, one of NONE, MINIMAL, BASIC, DETAILED, DEBUG, ROWLEVEL")
  private String level;

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Displays this help message and quits.")
  private boolean helpRequested;

  @Option(
      names = {"-p", "--parameters"},
      description = "A comma separated list of PARAMETER=VALUE pairs",
      split = ",")
  private String[] parameters = null;

  @Option(
      names = {"-s", "--system-properties"},
      description = "A comma separated list of KEY=VALUE pairs",
      split = ",")
  private String[] systemProperties = null;

  @Option(
      names = {"-r", "--runconfig"},
      description = "The name of the Run Configuration to use")
  private String runConfigurationName = null;

  @Option(
      names = {"-o", "--printoptions"},
      description = "Print the used options")
  private boolean printingOptions = false;

  private IVariables variables;
  private String realRunConfigurationName;
  private String realFilename;
  private CommandLine cmd;
  private ILogChannel log;
  private IHopMetadataProvider metadataProvider;
  private boolean finishedWithoutError;

  public HopRun() {
    variables = new Variables();
  }

  public void run() {
    validateOptions();

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
        if (mixin instanceof IConfigOptions) {
          IConfigOptions configOptions = (IConfigOptions) mixin;
          configOptions.handleOption(log, this, variables);
        }
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
    }
  }

  public void applySystemProperties() {
    // Set some System properties if there were any
    //
    if (systemProperties != null) {
      for (String parameter : systemProperties) {
        String[] split = parameter.split("=");
        String key = split.length > 0 ? split[0] : null;
        String value = split.length > 1 ? split[1] : null;
        if (StringUtils.isNotEmpty(key) && StringUtils.isNotEmpty(value)) {
          System.setProperty(key, value);
        }
      }
    }
  }

  private void buildVariableSpace() throws IOException {
    // Also grabs the system properties from hop.config.
    //
    variables = Variables.getADefaultVariableSpace();
  }

  private void runPipeline(CommandLine cmd, ILogChannel log) {

    try {
      calculateRealFilename();

      // Run the pipeline with the given filename
      //
      PipelineMeta pipelineMeta = new PipelineMeta(realFilename, metadataProvider, true, variables);

      // Configure the basic execution settings
      //
      PipelineExecutionConfiguration configuration = new PipelineExecutionConfiguration();

      // Overwrite if the user decided this
      //
      parseOptions(cmd, configuration, pipelineMeta);

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
  private void calculateRealFilename() throws HopException {
    realFilename = variables.resolve(filename);

    ExtensionPointHandler.callExtensionPoint(
        log, variables, HopExtensionPoint.HopRunCalculateFilename.id, this);
  }

  private void runPipeline(
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

      log.logMinimal( "Starting pipeline: "+pipelineMeta.getFilename() );

      // Run it!
      //
      pipeline.prepareExecution();
      pipeline.startThreads();
      pipeline.waitUntilFinished();
      // TODO: how to see if a pipeline fails? getresult always return true
      setFinishedWithoutError(true);
    } catch (Exception e) {
      throw new ExecutionException(cmd, "Error running pipeline locally", e);
    }
  }

  private void runWorkflow(CommandLine cmd, ILogChannel log) {
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

  private void runWorkflow(
      CommandLine cmd,
      ILogChannel log,
      WorkflowExecutionConfiguration configuration,
      WorkflowMeta workflowMeta) {
    try {
      String runConfigurationName = variables.resolve(configuration.getRunConfiguration());
      IWorkflowEngine<WorkflowMeta> workflow =
          WorkflowEngineFactory.createWorkflowEngine(
              variables, runConfigurationName, metadataProvider, workflowMeta, null);
      workflow.initializeFrom(null);
      workflow.getWorkflowMeta().setInternalHopVariables(workflow);
      workflow.setVariables(configuration.getVariablesMap());

      workflow.setLogLevel(configuration.getLogLevel());

      // Copy the parameter definitions from the metadata, with empty values
      //
      workflow.copyParametersFromDefinitions(workflowMeta);
      configureParametersAndVariables(cmd, configuration, workflow, workflow);

      // Also copy the parameter values over to the variables...
      //
      workflow.activateParameters(workflow);

      log.logMinimal( "Starting workflow: "+workflowMeta.getFilename() );

      workflow.startExecution();
      setFinishedWithoutError(workflow.getResult().getResult());
    } catch (Exception e) {
      throw new ExecutionException(cmd, "Error running workflow locally", e);
    }
  }

  private void parseOptions(
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

  private LogLevel determineLogLevel() {
    return LogLevel.getLogLevelForCode(variables.resolve(level));
  }

  private void configureHopServer(IExecutionConfiguration configuration, String name)
      throws HopException {

    IHopMetadataSerializer<HopServer> serializer = metadataProvider.getSerializer(HopServer.class);
    HopServer hopServer = serializer.load(name);
    if (hopServer == null) {
      throw new ParameterException(cmd, "Unable to find hop server '" + name + "' in the metadata");
    }
  }

  private boolean isPipeline() {
    if (StringUtils.isEmpty(filename)) {
      return false;
    }
    if (filename.toLowerCase().endsWith(".hpl")) {
      return true;
    } else {
      return false;
    }
  }

  private boolean isWorkflow() {
    if (StringUtils.isEmpty(filename)) {
      return false;
    }
    if (filename.toLowerCase().endsWith(".hwf")) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Set the variables and parameters
   *
   * @param cmd
   * @param configuration
   * @param namedParams
   */
  private void parseParametersAndVariables(
      CommandLine cmd,
      IExecutionConfiguration configuration,
      INamedParameterDefinitions namedParams) {
    try {
      String[] availableParameters = namedParams.listParameters();
      if (parameters != null) {
        for (String parameter : parameters) {
          String[] split = parameter.split("=");
          String key = split.length > 0 ? split[0] : null;
          String value = split.length > 1 ? split[1] : null;

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
    } catch (Exception e) {
      throw new ExecutionException(
          cmd, "There was an error during execution of pipeline '" + filename + "'", e);
    }
  }

  /**
   * Configure the variables and parameters in the given configuration on the given variable
   * variables and named parameters
   *
   * @param cmd
   * @param configuration
   * @param namedParams
   */
  private void configureParametersAndVariables(
      CommandLine cmd,
      IExecutionConfiguration configuration,
      IVariables variables,
      INamedParameters namedParams) {

    // Copy variables over to the pipeline or workflow
    //
    variables.setVariables(configuration.getVariablesMap());

    // By default we use the value from the current variables map:
    //
    for (String key : namedParams.listParameters()) {
      String value = variables.getVariable( key );
      if (StringUtils.isNotEmpty( value )) {
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

  private void validateOptions() {
    if (StringUtils.isEmpty(filename)) {
      throw new ParameterException(
          new CommandLine(this), "A filename is needed to run a workflow or pipeline");
    }
  }

  private void printOptions(IExecutionConfiguration configuration) {
    if (StringUtils.isNotEmpty(realFilename)) {
      log.logMinimal("OPTION: filename : '" + realFilename + "'");
    }
    if (StringUtils.isNotEmpty(realRunConfigurationName)) {
      log.logMinimal("OPTION: run configuration : '" + realRunConfigurationName + "'");
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

  /**
   * Gets log
   *
   * @return value of log
   */
  public ILogChannel getLog() {
    return log;
  }

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /**
   * Gets cmd
   *
   * @return value of cmd
   */
  public CommandLine getCmd() {
    return cmd;
  }

  /** @param cmd The cmd to set */
  public void setCmd(CommandLine cmd) {
    this.cmd = cmd;
  }

  /**
   * Gets filename
   *
   * @return value of filename
   */
  public String getFilename() {
    return filename;
  }

  /** @param filename The filename to set */
  public void setFilename(String filename) {
    this.filename = filename;
  }

  /**
   * Gets level
   *
   * @return value of level
   */
  public String getLevel() {
    return level;
  }

  /** @param level The level to set */
  public void setLevel(String level) {
    this.level = level;
  }

  /**
   * Gets helpRequested
   *
   * @return value of helpRequested
   */
  public boolean isHelpRequested() {
    return helpRequested;
  }

  /** @param helpRequested The helpRequested to set */
  public void setHelpRequested(boolean helpRequested) {
    this.helpRequested = helpRequested;
  }

  /**
   * Gets parameters
   *
   * @return value of parameters
   */
  public String[] getParameters() {
    return parameters;
  }

  /** @param parameters The parameters to set */
  public void setParameters(String[] parameters) {
    this.parameters = parameters;
  }

  /**
   * Gets runConfigurationName
   *
   * @return value of runConfigurationName
   */
  public String getRunConfigurationName() {
    return runConfigurationName;
  }

  /** @param runConfigurationName The runConfigurationName to set */
  public void setRunConfigurationName(String runConfigurationName) {
    this.runConfigurationName = runConfigurationName;
  }

  /**
   * Gets printingOptions
   *
   * @return value of printingOptions
   */
  public boolean isPrintingOptions() {
    return printingOptions;
  }

  /** @param printingOptions The printingOptions to set */
  public void setPrintingOptions(boolean printingOptions) {
    this.printingOptions = printingOptions;
  }

  /**
   * Gets systemProperties
   *
   * @return value of systemProperties
   */
  public String[] getSystemProperties() {
    return systemProperties;
  }

  /** @param systemProperties The systemProperties to set */
  public void setSystemProperties(String[] systemProperties) {
    this.systemProperties = systemProperties;
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getVariables() {
    return variables;
  }

  /** @param variables The variables to set */
  public void setVariables(IVariables variables) {
    this.variables = variables;
  }

  /**
   * Gets realRunConfigurationName
   *
   * @return value of realRunConfigurationName
   */
  public String getRealRunConfigurationName() {
    return realRunConfigurationName;
  }

  /** @param realRunConfigurationName The realRunConfigurationName to set */
  public void setRealRunConfigurationName(String realRunConfigurationName) {
    this.realRunConfigurationName = realRunConfigurationName;
  }

  /**
   * Gets realFilename
   *
   * @return value of realFilename
   */
  public String getRealFilename() {
    return realFilename;
  }

  /** @param realFilename The realFilename to set */
  public void setRealFilename(String realFilename) {
    this.realFilename = realFilename;
  }

  /** @param log The log to set */
  public void setLog(ILogChannel log) {
    this.log = log;
  }

  /** @param metadataProvider The metadataProvider to set */
  public void setMetadataProvider(IHopMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
  }

  /**
   * Gets finished status of pipeline or workflow
   *
   * @return boolean indicating no errors
   */
  public boolean isFinishedWithoutError() {
    return finishedWithoutError;
  }

  /**
   * @param finishedWithoutError Boolean indicating if pipeline or workflow finished without errors
   */
  public void setFinishedWithoutError(boolean finishedWithoutError) {
    this.finishedWithoutError = finishedWithoutError;
  }

  public static void main(String[] args) {

    HopRun hopRun = new HopRun();

    try {

      // Create the command line options...
      //
      CommandLine cmd = new CommandLine(hopRun);

      // Apply the system properties to the JVM
      //
      hopRun.applySystemProperties();

      // Initialize the Hop environment: load plugins and more
      //
      HopEnvironment.init();

      // Picks up the system settings in the variables
      //
      hopRun.buildVariableSpace();

      // Set up the metadata to use
      //
      hopRun.metadataProvider = HopMetadataUtil.getStandardHopMetadataProvider(hopRun.variables);

      // Now add run configuration plugins...
      //
      List<IPlugin> configPlugins = PluginRegistry.getInstance().getPlugins(ConfigPluginType.class);
      for (IPlugin configPlugin : configPlugins) {
        // Load only the plugins of the "run" category
        if (ConfigPlugin.CATEGORY_RUN.equals(configPlugin.getCategory())) {
          IConfigOptions configOptions =
              PluginRegistry.getInstance().loadClass(configPlugin, IConfigOptions.class);
          cmd.addMixin(configPlugin.getIds()[0], configOptions);
        }
      }
      hopRun.setCmd(cmd);

      // This will calculate the option values and put them in HopRun or the plugin classes
      //
      CommandLine.ParseResult parseResult = cmd.parseArgs(args);

      if (CommandLine.printHelpIfRequested(parseResult)) {
        System.exit(1);
      } else {
        hopRun.run();
        if (hopRun.isFinishedWithoutError()) {
          System.exit(0);
        } else {
          System.exit(1);
        }
      }
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      hopRun.cmd.usage(System.err);
      System.exit(9);
    } catch (ExecutionException e) {
      System.err.println("Error found during execution!");
      System.err.println(Const.getStackTracker(e));

      System.exit(1);
    } catch (Exception e) {
      System.err.println("General error found, something went horribly wrong!");
      System.err.println(Const.getStackTracker(e));

      System.exit(2);
    }
  }
}
