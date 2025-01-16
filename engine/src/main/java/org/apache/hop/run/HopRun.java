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

package org.apache.hop.run;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.IExecutionConfiguration;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.ConfigPluginType;
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
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.JarCache;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataInstance;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.server.HopServerMeta;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEngineFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;

@Command(versionProvider = HopVersionProvider.class)
public class HopRun implements Runnable, IHasHopMetadataProvider {

  @Option(
      names = {"-f", "--file"},
      description = "The filename of the workflow or pipeline to run")
  private String filename;

  @Option(
      names = {"-l", "--level"},
      description =
          "The debug level, one of NOTHING, ERROR, MINIMAL, BASIC, DETAILED, DEBUG, ROWLEVEL")
  private String level;

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Displays this help message and quits.")
  private boolean helpRequested;

  @Option(
      names = {"-v", "--version"},
      versionHelp = true,
      description = "Print version information and exit")
  private boolean versionRequested;

  @Option(
      names = {"-lf", "--logfile"},
      description = "The complete filename where hop-run will write the Hop console log")
  private String logFile;

  @Option(
      names = {"-p", "--parameters"},
      description = "A list of PARAMETER=VALUE pairs")
  private String[] parameters = null;

  @Option(
      names = {"-ps", "--parameters-separator"},
      description =
          "A character to be used as separator for our list of PARAMETER=VALUE pairs (default is ,)")
  private String parametersSeparator = null;

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
      names = {"-a", "--startaction"},
      description = "The name of the action where to start a workflow")
  private String startActionName = null;

  @Option(
      names = {"-m", "--metadata-export"},
      description = "A file containing exported metadata in JSON format")
  private String metadataExportFile;

  @Option(
      names = {"-o", "--printoptions"},
      description = "Print the used options")
  private boolean printingOptions = false;

  private IVariables variables;
  private String realRunConfigurationName;
  private String realFilename;
  private CommandLine cmd;
  private ILogChannel log;
  private MultiMetadataProvider metadataProvider;
  private boolean finishedWithoutError;

  public HopRun() {
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

  public void applySystemProperties() {
    // Set some System properties if there were any
    //
    if (systemProperties != null) {
      for (String parameter : systemProperties) {
        String[] split = parameter.split("=", 2);
        String key = split.length > 0 ? split[0] : null;
        String value = split.length > 1 ? split[1] : null;
        if (StringUtils.isNotEmpty(key) && StringUtils.isNotEmpty(value)) {
          System.setProperty(key, value);
        }
      }
    }
  }

  private void prepareInternalOptions(CommandLine cmd, String[] args) {
    for (String arg : args) {
      if (arg.startsWith("-h") || arg.startsWith("--help")) {
        return;
      }
    }

    String[] helpArgs = new String[args.length + 1];
    System.arraycopy(args, 0, helpArgs, 0, args.length);
    helpArgs[args.length] = "-h";

    cmd.parseArgs(helpArgs);
  }

  private void buildVariableSpace() {
    // Also grabs the system properties from hop.config.
    //
    variables = Variables.getADefaultVariableSpace();
  }

  private void runPipeline(CommandLine cmd, ILogChannel log) {

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

  private void runWorkflow(
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
    return LogLevel.lookupCode(variables.resolve(level));
  }

  private void configureHopServer(IExecutionConfiguration configuration, String name)
      throws HopException {

    IHopMetadataSerializer<HopServerMeta> serializer =
        metadataProvider.getSerializer(HopServerMeta.class);
    HopServerMeta hopServer = serializer.load(name);
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
  @Override
  public MultiMetadataProvider getMetadataProvider() {
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

  /**
   * @param cmd The cmd to set
   */
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

  /**
   * @param filename The filename to set
   */
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

  /**
   * @param level The level to set
   */
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

  /**
   * @param helpRequested The helpRequested to set
   */
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

  /**
   * @param parameters The parameters to set
   */
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

  /**
   * @param runConfigurationName The runConfigurationName to set
   */
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

  /**
   * @param printingOptions The printingOptions to set
   */
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

  /**
   * @param systemProperties The systemProperties to set
   */
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

  /**
   * @param variables The variables to set
   */
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

  /**
   * @param realRunConfigurationName The realRunConfigurationName to set
   */
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

  /**
   * @param realFilename The realFilename to set
   */
  public void setRealFilename(String realFilename) {
    this.realFilename = realFilename;
  }

  /**
   * @param log The log to set
   */
  public void setLog(ILogChannel log) {
    this.log = log;
  }

  public String getLogFile() {
    return logFile;
  }

  public void setLogFile(String logFile) {
    this.logFile = logFile;
  }

  /**
   * @param metadataProvider The metadataProvider to set
   */
  @Override
  public void setMetadataProvider(MultiMetadataProvider metadataProvider) {
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

  /**
   * Gets metadataExportFile
   *
   * @return value of metadataExportFile
   */
  public String getMetadataExportFile() {
    return metadataExportFile;
  }

  /**
   * Sets metadataExportFile
   *
   * @param metadataExportFile value of metadataExportFile
   */
  public void setMetadataExportFile(String metadataExportFile) {
    this.metadataExportFile = metadataExportFile;
  }

  public static void main(String[] args) {

    HopRun hopRun = new HopRun();

    try {

      // Create the command line options...
      //
      CommandLine cmd = new CommandLine(hopRun);

      if (args.length > 0) {
        hopRun.prepareInternalOptions(new CommandLine(hopRun), args);
      }
      // Apply the system properties to the JVM
      //
      hopRun.applySystemProperties();

      // Initialize the Hop environment: load plugins and more
      //
      HopEnvironment.init();

      // Picks up the system settings in the variables
      //
      hopRun.buildVariableSpace();

      // Initialize the logging backend
      //
      HopLogStore.init();

      // Clear the jar file cache so that we don't waste memory...
      //
      JarCache.getInstance().clear();

      // Set up the metadata to use
      //
      hopRun.metadataProvider = HopMetadataUtil.getStandardHopMetadataProvider(hopRun.variables);
      HopMetadataInstance.setMetadataProvider(hopRun.metadataProvider);

      // Now add configuration plugins with the RUN category.
      // The 'projects' plugin for example configures things like the project metadata provider.
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
        System.out.println("HopRun exit.");
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
