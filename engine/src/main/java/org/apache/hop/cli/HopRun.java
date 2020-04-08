/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.cli;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.IExecutionConfiguration;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.parameters.INamedParams;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.metastore.MetaStoreConst;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.metastore.stores.delegate.DelegatingMetaStore;
import org.apache.hop.metastore.util.HopDefaults;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.www.SlaveServerWorkflowStatus;
import org.apache.hop.www.SlaveServerPipelineStatus;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class HopRun implements Runnable {
  public static final String XP_HOP_RUN_START = "HopRunStart";
  public static final String XP_CREATE_ENVIRONMENT = "CreateEnvironment";
  public static final String XP_IMPORT_ENVIRONMENT = "ImportEnvironment";

  @Option( names = { "-f", "-z", "--file" }, description = "The filename of the workflow or pipeline to run" )
  private String filename;

  @Option( names = { "-l", "--level" }, description = "The debug level, one of NONE, MINIMAL, BASIC, DETAILED, DEBUG, ROWLEVEL" )
  private String level;

  @Option( names = { "-h", "--help" }, usageHelp = true, description = "Displays this help message and quits." )
  private boolean helpRequested;

  @Option( names = { "-p", "--parameters" }, description = "A comma separated list of PARAMTER=VALUE pairs", split = "," )
  private String[] parameters = null;

  @Option( names = { "-r", "--runconfig" }, description = "The name of the Run Configuration to use" )
  private String runConfigurationName = null;

  @Option( names = { "-t", "--pipeline" }, description = "Force execution of a pipeline" )
  private boolean runPipeline = false;

  @Option( names = { "-j", "--workflow" }, description = "Force execution of a workflow" )
  private boolean runJob = false;

  @Option( names = { "-s", "--slave" }, description = "The slave server to run on" )
  private String slaveServerName;

  @Option( names = { "-x", "--export" }, description = "Export all resources and send them to the slave server" )
  private boolean exportToSlaveServer = false;

  @Option( names = { "-q", "--querydelay" }, description = "Delay between querying of remote servers" )
  private String queryDelay;

  @Option( names = { "-d", "--dontwait" }, description = "Do not wait until the remote workflow or pipeline is done" )
  private boolean dontWait = false;

  @Option( names = { "-g", "--remotelog" }, description = "Write out the remote log of remote executions" )
  private boolean remoteLogging = false;

  @Option( names = { "-o", "--printoptions" }, description = "Print the used options" )
  private boolean printingOptions = false;

  @Option( names = { "-initialDir" }, description = "Ignored", hidden = true )
  private String intialDir = null;

  @Option( names = { "-e", "--environment" }, description = "The name of the environment to use" )
  private String environment;

  @Option( names = { "-C", "--create-environment" }, description = "Create an environment using format <Name>=<Base folder>, applies the environment created" )
  private String createEnvironmentOption;

  @Option( names = { "-I", "--import-environment" }, description = "Import an environment from a JSON file" )
  private String environmentJsonFilename;

  @Option( names = { "-V",
    "--add-variable-to-environment", }, description = "When creating an environment, add the given variable in format <Variable>=<Value>:<Description>. You can specify this option multiple times." )
  private Map<String, String> variablesToAddToEnvironment;

  private IVariables variables;
  private String realRunConfigurationName;
  private String realFilename;
  private String realSlaveServerName;
  private CommandLine cmd;
  private ILogChannel log;
  private DelegatingMetaStore metaStore;

  public void run() {
    validateOptions();

    try {
      initialize( cmd );

      log = new LogChannel( "HopRun" );
      log.logDetailed( "Start of Hop Run" );

      // Allow modification of various environment settings
      //
      ExtensionPointHandler.callExtensionPoint( log, XP_HOP_RUN_START, environment );

      buildVariableSpace();
      buildMetaStore();

      if ( StringUtils.isNotEmpty( createEnvironmentOption ) ) {
        createEnvironment();
      }
      if ( StringUtils.isNotEmpty( environmentJsonFilename ) ) {
        importEnvironment();
      }

      if ( isPipeline() ) {
        runPipeline( cmd, log );
      }
      if ( isJob() ) {
        runJob( cmd, log );
      }
    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "There was an error during execution of file '" + filename + "'", e );
    }
  }

  private void initialize( CommandLine cmd ) {
    try {
      HopEnvironment.init();
    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "There was a problem during the initialization of the Hop environment", e );
    }
  }

  private void buildVariableSpace() throws IOException {
    // Load kettle.properties before running for convenience...
    //
    variables = Variables.getADefaultVariableSpace();
    Properties kettleProperties = new Properties();
    kettleProperties.load( new FileInputStream( Const.getHopDirectory() + "/hop.properties" ) );
    for ( final String key : kettleProperties.stringPropertyNames() ) {
      variables.setVariable( key, kettleProperties.getProperty( key ) );
    }
  }

  private void runPipeline( CommandLine cmd, ILogChannel log ) {

    try {
      calculateRealFilename();

      // Run the pipeline with the given filename
      //
      PipelineMeta pipelineMeta = new PipelineMeta( realFilename, metaStore, true, variables );

      // Configure the basic execution settings
      //
      PipelineExecutionConfiguration configuration = new PipelineExecutionConfiguration();

      // Copy run config details from the metastore over to the run configuration
      // TODO
      // parseRunConfiguration( cmd, configuration, metaStore );

      // Overwrite if the user decided this
      //
      parseOptions( cmd, configuration, pipelineMeta );

      // configure the variables and parameters
      //
      configureParametersAndVariables( cmd, configuration, pipelineMeta, pipelineMeta );

      // Certain Pentaho plugins rely on this.  Meh.
      //
      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiPipelineBeforeStart.id, new Object[] {
        configuration, null, pipelineMeta, null } );

      // Before running, do we print the options?
      //
      if ( printingOptions ) {
        printOptions( configuration );
      }

      // Now run the pipeline
      //
      if ( configuration.isExecutingLocally() ) {
        runPipelineLocal( cmd, log, configuration, pipelineMeta );
      } else if ( configuration.isExecutingRemotely() ) {
        runPipelineRemote( cmd, log, pipelineMeta, configuration );
      }

    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "There was an error during execution of pipeline '" + filename + "'", e );
    }
  }

  /**
   * This way we can actually use environment variables to parse the real filename
   */
  private void calculateRealFilename() throws HopException {
    realFilename = variables.environmentSubstitute( filename );

    try {
      FileObject fileObject = HopVFS.getFileObject( realFilename );
      if ( !fileObject.exists() ) {
        // Try to prepend with ${ENVIRONMENT_HOME}
        //
        String alternativeFilename = variables.environmentSubstitute( "${ENVIRONMENT_HOME}/" + filename );
        fileObject = HopVFS.getFileObject( alternativeFilename );
        if ( fileObject.exists() ) {
          realFilename = alternativeFilename;
          log.logMinimal( "Relative path filename specified: " + realFilename );
        }
      }
    } catch ( Exception e ) {
      throw new HopException( "Error calculating filename", e );
    }
  }

  private void runPipelineLocal( CommandLine cmd, ILogChannel log, PipelineExecutionConfiguration configuration, PipelineMeta pipelineMeta ) {
    try {
      Pipeline pipeline = new Pipeline( pipelineMeta );
      pipeline.initializeVariablesFrom( null );
      pipeline.getPipelineMeta().setInternalHopVariables( pipeline );
      pipeline.injectVariables( configuration.getVariablesMap() );

      pipeline.setLogLevel( configuration.getLogLevel() );
      pipeline.setMetaStore( metaStore );

      // Also copy the parameters over...
      //
      pipeline.copyParametersFrom( pipelineMeta );
      pipelineMeta.activateParameters();
      pipeline.activateParameters();

      // Run it!
      //
      pipeline.prepareExecution();
      pipeline.startThreads();
      pipeline.waitUntilFinished();
    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "Error running pipeline locally", e );
    }
  }

  private void runPipelineRemote( CommandLine cmd, ILogChannel log, PipelineMeta pipelineMeta, PipelineExecutionConfiguration configuration ) {
    SlaveServer slaveServer = configuration.getRemoteServer();
    slaveServer.shareVariablesWith( pipelineMeta );
    try {
      runPipelineOnSlaveServer( log, pipelineMeta, slaveServer, configuration, metaStore, dontWait, getQueryDelay() );
    } catch ( Exception e ) {
      throw new ExecutionException( cmd, e.getMessage(), e );
    }
  }

  public static Result runPipelineOnSlaveServer( ILogChannel log, PipelineMeta pipelineMeta, SlaveServer slaveServer, PipelineExecutionConfiguration configuration, IMetaStore metaStore,
                                                 boolean dontWait, int queryDelay ) throws Exception {
    try {
      String carteObjectId = Pipeline.sendToSlaveServer( pipelineMeta, configuration, metaStore );
      if ( !dontWait ) {
        slaveServer.monitorRemotePipeline( log, carteObjectId, pipelineMeta.getName(), queryDelay );
        SlaveServerPipelineStatus pipelineStatus = slaveServer.getPipelineStatus( pipelineMeta.getName(), carteObjectId, 0 );
        if ( configuration.isLogRemoteExecutionLocally() ) {
          log.logBasic( pipelineStatus.getLoggingString() );
        }
        if ( pipelineStatus.getNrTransformErrors() > 0 ) {
          // Error
          throw new Exception( "Remote pipeline ended with an error" );
        }

        return pipelineStatus.getResult();
      }
      return null; // No status, we don't wait for it.
    } catch ( Exception e ) {
      throw new Exception( "Error executing pipeline remotely on server '" + slaveServer.getName() + "'", e );
    }
  }

  private void runJob( CommandLine cmd, ILogChannel log ) {
    try {
      calculateRealFilename();

      // Run the workflow with the given filename
      //
      WorkflowMeta workflowMeta = new WorkflowMeta( variables, realFilename, metaStore );

      // Configure the basic execution settings
      //
      WorkflowExecutionConfiguration configuration = new WorkflowExecutionConfiguration();

      // Copy run config details from the metastore over to the run configuration
      //
      // parseRunConfiguration( cmd, configuration, metaStore );

      // Overwrite the run configuration with optional command line options
      //
      parseOptions( cmd, configuration, workflowMeta );

      // Certain Pentaho plugins rely on this.  Meh.
      //
      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiJobBeforeStart.id, new Object[] { configuration, null, workflowMeta, null } );

      // Before running, do we print the options?
      //
      if ( printingOptions ) {
        printOptions( configuration );
      }

      // Now we can run the workflow
      //
      if ( configuration.isExecutingLocally() ) {
        runJobLocal( cmd, log, configuration, workflowMeta );
      } else if ( configuration.isExecutingRemotely() ) {
        // Simply remote execution.
        //
        runJobRemote( cmd, log, workflowMeta, configuration );
      }


    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "There was an error during execution of workflow '" + filename + "'", e );
    }
  }

  private void runJobLocal( CommandLine cmd, ILogChannel log, WorkflowExecutionConfiguration configuration, WorkflowMeta workflowMeta ) {
    try {
      Workflow workflow = new Workflow( null, workflowMeta );
      workflow.initializeVariablesFrom( null );
      workflow.getWorkflowMeta().setInternalHopVariables( workflow );
      workflow.injectVariables( configuration.getVariablesMap() );

      workflow.setLogLevel( configuration.getLogLevel() );

      // Explicitly set parameters
      for ( String parameterName : configuration.getParametersMap().keySet() ) {
        workflowMeta.setParameterValue( parameterName, configuration.getParametersMap().get( parameterName ) );
      }

      // Also copy the parameters over...
      //
      workflow.copyParametersFrom( workflowMeta );
      workflowMeta.activateParameters();
      workflow.activateParameters();

      workflow.start();
      workflow.waitUntilFinished();
    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "Error running workflow locally", e );
    }
  }

  private void runJobRemote( CommandLine cmd, ILogChannel log, WorkflowMeta workflowMeta, WorkflowExecutionConfiguration configuration ) {
    SlaveServer slaveServer = configuration.getRemoteServer();
    slaveServer.shareVariablesWith( workflowMeta );

    try {

    } catch ( Exception e ) {
      throw new ExecutionException( cmd, e.getMessage(), e );
    }
  }

  public static Result runJobOnSlaveServer( ILogChannel log, WorkflowMeta workflowMeta, SlaveServer slaveServer, WorkflowExecutionConfiguration configuration, IMetaStore metaStore, boolean dontWait,
                                            boolean remoteLogging, int queryDelay ) throws Exception {
    try {
      String carteObjectId = Workflow.sendToSlaveServer( workflowMeta, configuration, metaStore );

      // Monitor until finished...
      //
      SlaveServerWorkflowStatus jobStatus = null;
      Result oneResult = new Result();

      while ( !dontWait ) {
        try {
          jobStatus = slaveServer.getJobStatus( workflowMeta.getName(), carteObjectId, 0 );
          if ( jobStatus.getResult() != null ) {
            // The workflow is finished, get the result...
            //
            oneResult = jobStatus.getResult();
            break;
          }
        } catch ( Exception e1 ) {
          log.logError( "Unable to contact slave server [" + slaveServer + "] to verify the status of workflow [" + workflowMeta.getName() + "]", e1 );
          oneResult.setNrErrors( 1L );
          break; // Stop looking too, chances are too low the server will
          // come back on-line
        }

        // sleep for a while
        try {
          Thread.sleep( queryDelay );
        } catch ( InterruptedException e ) {
          // Ignore
        }
      }

      // Get the status
      //
      if ( !dontWait ) {
        jobStatus = slaveServer.getJobStatus( workflowMeta.getName(), carteObjectId, 0 );
        if ( remoteLogging ) {
          log.logBasic( jobStatus.getLoggingString() );
        }
        Result result = jobStatus.getResult();
        if ( result.getNrErrors() > 0 ) {
          // Error
          throw new Exception( "Remote workflow ended with an error" );
        }
        return result;
      }
      return null;
    } catch ( Exception e ) {
      throw new Exception( "Error executing workflow remotely on server '" + slaveServer.getName() + "'", e );
    }
  }

  private int getQueryDelay() {
    if ( StringUtils.isEmpty( queryDelay ) ) {
      return 5;
    }
    return Const.toInt( queryDelay, 5 );
  }

  private void parseOptions( CommandLine cmd, IExecutionConfiguration configuration, INamedParams namedParams ) throws MetaStoreException {

    if ( StringUtils.isNotEmpty( slaveServerName ) ) {
      realSlaveServerName = variables.environmentSubstitute( slaveServerName );
      configureSlaveServer( configuration, realSlaveServerName );
      configuration.setExecutingRemotely( true );
      configuration.setExecutingLocally( false );
    }
    configuration.setPassingExport( exportToSlaveServer );
    realRunConfigurationName = variables.environmentSubstitute( runConfigurationName );
    configuration.setRunConfiguration( realRunConfigurationName );
    configuration.setLogLevel( LogLevel.getLogLevelForCode( variables.environmentSubstitute( level ) ) );

    // Set variables and parameters...
    //
    parseParametersAndVariables( cmd, configuration, namedParams );
  }

  private void configureSlaveServer( IExecutionConfiguration configuration, String name ) throws MetaStoreException {
    MetaStoreFactory<SlaveServer> slaveFactory = new MetaStoreFactory<>( SlaveServer.class, metaStore, HopDefaults.NAMESPACE );
    SlaveServer slaveServer = slaveFactory.loadElement( name );
    if ( slaveServer == null ) {
      throw new ParameterException( cmd, "Unable to find slave server '" + name + "' in the metastore" );
    }
    configuration.setRemoteServer( slaveServer );
  }

  private boolean isPipeline() {
    if ( runPipeline ) {
      return true;
    }
    if ( StringUtils.isEmpty( filename ) ) {
      return false;
    }
    return filename.toLowerCase().endsWith( ".hpl" );
  }

  private boolean isJob() {
    if ( runJob ) {
      return true;
    }
    if ( StringUtils.isEmpty( filename ) ) {
      return false;
    }
    return filename.toLowerCase().endsWith( ".hwf" );
  }


  /**
   * Set the variables and parameters
   *
   * @param cmd
   * @param configuration
   * @param namedParams
   */
  private void parseParametersAndVariables( CommandLine cmd, IExecutionConfiguration configuration, INamedParams namedParams ) {
    try {
      String[] availableParameters = namedParams.listParameters();
      if ( parameters != null ) {
        for ( String parameter : parameters ) {
          String[] split = parameter.split( "=" );
          String key = split.length > 0 ? split[ 0 ] : null;
          String value = split.length > 1 ? split[ 1 ] : null;

          if ( key != null ) {
            // We can work with this.
            //
            if ( Const.indexOfString( key, availableParameters ) < 0 ) {
              // A variable
              //
              configuration.getVariablesMap().put( key, value );
            } else {
              // A parameter
              //
              configuration.getParametersMap().put( key, value );
            }
          }
        }
      }
    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "There was an error during execution of pipeline '" + filename + "'", e );
    }
  }

  private void buildMetaStore() throws MetaStoreException {
    metaStore = new DelegatingMetaStore();
    IMetaStore localMetaStore = MetaStoreConst.openLocalHopMetaStore();
    metaStore.addMetaStore( localMetaStore );
    metaStore.setActiveMetaStoreName( localMetaStore.getName() );
  }


  /**
   * Configure the variables and parameters in the given configuration on the given variable space and named parameters
   *
   * @param cmd
   * @param configuration
   * @param namedParams
   */
  private void configureParametersAndVariables( CommandLine cmd, IExecutionConfiguration configuration, IVariables variables, INamedParams namedParams ) {

    // Copy variables over to the pipeline or workflow metadata
    //
    variables.injectVariables( configuration.getVariablesMap() );

    // Set the parameter values
    //
    for ( String key : configuration.getParametersMap().keySet() ) {
      String value = configuration.getParametersMap().get( key );
      try {
        namedParams.setParameterValue( key, value );
      } catch ( UnknownParamException e ) {
        throw new ParameterException( cmd, "Unable to set parameter '" + key + "'", e );
      }
    }
  }

  public void createEnvironment() throws HopException {

    // Parse the options and call an extension point...
    // Look in the kettle-environment project for the actual code behind this.
    //
    String environmentName;
    String baseFolder;

    int equalsIndex = createEnvironmentOption.indexOf( '=' );
    if ( equalsIndex < 0 ) {
      // Default
      environmentName = createEnvironmentOption;
      baseFolder = null;
    } else {
      environmentName = createEnvironmentOption.substring( 0, equalsIndex );
      baseFolder = createEnvironmentOption.substring( equalsIndex + 1 );
    }

    // Now call the extension point.
    // The kettle-environment project knows how to handle this in the best possible way
    //
    ExtensionPointHandler.callExtensionPoint( log, XP_CREATE_ENVIRONMENT, new Object[] { environmentName, baseFolder, variablesToAddToEnvironment } );
  }

  public void importEnvironment() throws HopException {

    // Call an extension point with the file to import...
    // Look in the kettle-environment project for the actual code behind this.
    // The kettle-environment project knows how to handle this in the best possible way
    //
    ExtensionPointHandler.callExtensionPoint( log, XP_IMPORT_ENVIRONMENT, new Object[] { environmentJsonFilename } );
  }

  private void validateOptions() {
    if ( StringUtils.isNotEmpty( createEnvironmentOption ) ) {
      return;
    }
    if ( StringUtils.isNotEmpty( environmentJsonFilename ) ) {
      return;
    }

    if ( StringUtils.isEmpty( filename ) ) {
      throw new ParameterException( new CommandLine( this ), "A filename is needed to run a workflow or pipeline" );
    }
  }

  private void printOptions( IExecutionConfiguration configuration ) {
    if ( StringUtils.isNotEmpty( realFilename ) ) {
      log.logMinimal( "OPTION: filename : '" + realFilename + "'" );
    }
    if ( StringUtils.isNotEmpty( realRunConfigurationName ) ) {
      log.logMinimal( "OPTION: run configuration : '" + realRunConfigurationName + "'" );
    }
    if ( StringUtils.isNotEmpty( realSlaveServerName ) ) {
      log.logMinimal( "OPTION: slave server: '" + realSlaveServerName + "'" );
    }
    // Where are we executing? Local or Remote?
    if ( configuration.isExecutingLocally() ) {
      log.logMinimal( "OPTION: Local execution" );
    } else {
      if ( configuration.isExecutingRemotely() ) {
        log.logMinimal( "OPTION: Remote execution" );
      }
    }
    if ( configuration.isPassingExport() ) {
      log.logMinimal( "OPTION: Passing export to slave server" );
    }
    log.logMinimal( "OPTION: Logging level : " + configuration.getLogLevel().getDescription() );

    if ( !configuration.getVariablesMap().isEmpty() ) {
      log.logMinimal( "OPTION: Variables: " );
      for ( String variable : configuration.getVariablesMap().keySet() ) {
        log.logMinimal( "  " + variable + " : '" + configuration.getVariablesMap().get( variable ) );
      }
    }
    if ( !configuration.getParametersMap().isEmpty() ) {
      log.logMinimal( "OPTION: Parameters: " );
      for ( String parameter : configuration.getParametersMap().keySet() ) {
        log.logMinimal( "OPTION:   " + parameter + " : '" + configuration.getParametersMap().get( parameter ) );
      }
    }

    if ( StringUtils.isNotEmpty( queryDelay ) ) {
      log.logMinimal( "OPTION: Remote server query delay : " + getQueryDelay() );
    }
    if ( dontWait ) {
      log.logMinimal( "OPTION: Do not wait for remote workflow or pipeline to finish" );
    }
    if ( remoteLogging ) {
      log.logMinimal( "OPTION: Printing remote execution log" );
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
   * Gets metaStore
   *
   * @return value of metaStore
   */
  public IMetaStore getMetaStore() {
    return metaStore;
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
  public void setCmd( CommandLine cmd ) {
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
  public void setFilename( String filename ) {
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
  public void setLevel( String level ) {
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
  public void setHelpRequested( boolean helpRequested ) {
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
  public void setParameters( String[] parameters ) {
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
  public void setRunConfigurationName( String runConfigurationName ) {
    this.runConfigurationName = runConfigurationName;
  }

  /**
   * Gets runPipeline
   *
   * @return value of runPipeline
   */
  public boolean isRunPipeline() {
    return runPipeline;
  }

  /**
   * @param runPipeline The runPipeline to set
   */
  public void setRunPipeline( boolean runPipeline ) {
    this.runPipeline = runPipeline;
  }

  /**
   * Gets runJob
   *
   * @return value of runJob
   */
  public boolean isRunJob() {
    return runJob;
  }

  /**
   * @param runJob The runJob to set
   */
  public void setRunJob( boolean runJob ) {
    this.runJob = runJob;
  }

  /**
   * Gets slaveServerName
   *
   * @return value of slaveServerName
   */
  public String getSlaveServerName() {
    return slaveServerName;
  }

  /**
   * @param slaveServerName The slaveServerName to set
   */
  public void setSlaveServerName( String slaveServerName ) {
    this.slaveServerName = slaveServerName;
  }

  /**
   * Gets exportToSlaveServer
   *
   * @return value of exportToSlaveServer
   */
  public boolean isExportToSlaveServer() {
    return exportToSlaveServer;
  }

  /**
   * @param exportToSlaveServer The exportToSlaveServer to set
   */
  public void setExportToSlaveServer( boolean exportToSlaveServer ) {
    this.exportToSlaveServer = exportToSlaveServer;
  }

  /**
   * @param queryDelay The queryDelay to set
   */
  public void setQueryDelay( String queryDelay ) {
    this.queryDelay = queryDelay;
  }

  /**
   * Gets dontWait
   *
   * @return value of dontWait
   */
  public boolean isDontWait() {
    return dontWait;
  }

  /**
   * @param dontWait The dontWait to set
   */
  public void setDontWait( boolean dontWait ) {
    this.dontWait = dontWait;
  }

  /**
   * Gets remoteLogging
   *
   * @return value of remoteLogging
   */
  public boolean isRemoteLogging() {
    return remoteLogging;
  }

  /**
   * @param remoteLogging The remoteLogging to set
   */
  public void setRemoteLogging( boolean remoteLogging ) {
    this.remoteLogging = remoteLogging;
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
  public void setPrintingOptions( boolean printingOptions ) {
    this.printingOptions = printingOptions;
  }

  /**
   * Gets intialDir
   *
   * @return value of intialDir
   */
  public String getIntialDir() {
    return intialDir;
  }

  /**
   * @param intialDir The intialDir to set
   */
  public void setIntialDir( String intialDir ) {
    this.intialDir = intialDir;
  }

  /**
   * Gets environment
   *
   * @return value of environment
   */
  public String getEnvironment() {
    return environment;
  }

  /**
   * @param environment The environment to set
   */
  public void setEnvironment( String environment ) {
    this.environment = environment;
  }

  /**
   * Gets createEnvironmentOption
   *
   * @return value of createEnvironmentOption
   */
  public String getCreateEnvironmentOption() {
    return createEnvironmentOption;
  }

  /**
   * @param createEnvironmentOption The createEnvironmentOption to set
   */
  public void setCreateEnvironmentOption( String createEnvironmentOption ) {
    this.createEnvironmentOption = createEnvironmentOption;
  }


  public static void main( String[] args ) {

    HopRun hopRun = new HopRun();
    try {
      CommandLine cmd = new CommandLine( hopRun );
      hopRun.setCmd( cmd );
      CommandLine.ParseResult parseResult = cmd.parseArgs( args );
      if ( CommandLine.printHelpIfRequested( parseResult ) ) {
        System.exit( 1 );
      } else {
        hopRun.run();
        System.exit( 0 );
      }
    } catch ( ParameterException e ) {
      System.err.println( e.getMessage() );
      e.getCommandLine().usage( System.err );
      System.exit( 9 );
    } catch ( ExecutionException e ) {
      System.err.println( "Error found during execution!" );
      System.err.println( Const.getStackTracker( e ) );

      System.exit( 1 );
    } catch ( Exception e ) {
      System.err.println( "General error found, something went horribly wrong!" );
      System.err.println( Const.getStackTracker( e ) );

      System.exit( 2 );
    }

  }
}
