package org.apache.hop.cli;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.ExecutionConfiguration;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.parameters.NamedParams;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobExecutionConfiguration;
import org.apache.hop.job.JobMeta;
import org.apache.hop.metastore.MetaStoreConst;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.metastore.stores.delegate.DelegatingMetaStore;
import org.apache.hop.metastore.util.HopDefaults;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransExecutionConfiguration;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.www.SlaveServerJobStatus;
import org.apache.hop.www.SlaveServerTransStatus;
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

  @Option( names = { "-f", "-z", "--file" }, description = "The filename of the job or transformation to run" )
  private String filename;

  @Option( names = { "-l", "--level" }, description = "The debug level, one of NONE, MINIMAL, BASIC, DETAILED, DEBUG, ROWLEVEL" )
  private String level;

  @Option( names = { "-h", "--help" }, usageHelp = true, description = "Displays this help message and quits." )
  private boolean helpRequested;

  @Option( names = { "-p", "--parameters" }, description = "A comma separated list of PARAMTER=VALUE pairs", split = "," )
  private String[] parameters = null;

  @Option( names = { "-r", "--runconfig" }, description = "The name of the Run Configuration to use" )
  private String runConfigurationName = null;

  @Option( names = { "-t", "--transformation" }, description = "Force execution of a transformation" )
  private boolean runTransformation = false;

  @Option( names = { "-j", "--job" }, description = "Force execution of a job" )
  private boolean runJob = false;

  @Option( names = { "-a", "--safemode" }, description = "Run in safe mode" )
  private boolean safeMode = false;

  @Option( names = { "-m", "--metrics" }, description = "Gather metrics" )
  private boolean gatherMetrics = false;

  @Option( names = { "-s", "--slave" }, description = "The slave server to run on" )
  private String slaveServerName;

  @Option( names = { "-x", "--export" }, description = "Export all resources and send them to the slave server" )
  private boolean exportToSlaveServer = false;

  @Option( names = { "-q", "--querydelay" }, description = "Delay between querying of remote servers" )
  private String queryDelay;

  @Option( names = { "-d", "--dontwait" }, description = "Do not wait until the remote job or transformation is done" )
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

  private VariableSpace space;
  private String realRunConfigurationName;
  private String realFilename;
  private String realSlaveServerName;
  private CommandLine cmd;
  private LogChannelInterface log;
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

      if ( isTransformation() ) {
        runTransformation( cmd, log );
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
    space = Variables.getADefaultVariableSpace();
    Properties kettleProperties = new Properties();
    kettleProperties.load( new FileInputStream( Const.getHopDirectory() + "/hop.properties" ) );
    for ( final String key : kettleProperties.stringPropertyNames() ) {
      space.setVariable( key, kettleProperties.getProperty( key ) );
    }
  }

  private void runTransformation( CommandLine cmd, LogChannelInterface log ) {

    try {
      calculateRealFilename();

      // Run the transformation with the given filename
      //
      TransMeta transMeta = new TransMeta( realFilename, metaStore, true, space );

      // Configure the basic execution settings
      //
      TransExecutionConfiguration configuration = new TransExecutionConfiguration();

      // Copy run config details from the metastore over to the run configuration
      // TODO
      // parseRunConfiguration( cmd, configuration, metaStore );

      // Overwrite if the user decided this
      //
      parseOptions( cmd, configuration, transMeta );

      // configure the variables and parameters
      //
      configureParametersAndVariables( cmd, configuration, transMeta, transMeta );

      // Certain Pentaho plugins rely on this.  Meh.
      //
      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiTransBeforeStart.id, new Object[] {
        configuration, null, transMeta, null } );

      // Before running, do we print the options?
      //
      if ( printingOptions ) {
        printOptions( configuration );
      }

      // Now run the transformation
      //
      if ( configuration.isExecutingLocally() ) {
        runTransLocal( cmd, log, configuration, transMeta );
      } else if ( configuration.isExecutingRemotely() ) {
        runTransRemote( cmd, log, transMeta, configuration );
      }

    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "There was an error during execution of transformation '" + filename + "'", e );
    }
  }

  /**
   * This way we can actually use environment variables to parse the real filename
   */
  private void calculateRealFilename() throws HopException {
    realFilename = space.environmentSubstitute( filename );

    try {
      FileObject fileObject = HopVFS.getFileObject( realFilename );
      if ( !fileObject.exists() ) {
        // Try to prepend with ${ENVIRONMENT_HOME}
        //
        String alternativeFilename = space.environmentSubstitute( "${ENVIRONMENT_HOME}/" + filename );
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

  private void runTransLocal( CommandLine cmd, LogChannelInterface log, TransExecutionConfiguration configuration, TransMeta transMeta ) {
    try {
      Trans trans = new Trans( transMeta );
      trans.initializeVariablesFrom( null );
      trans.getTransMeta().setInternalHopVariables( trans );
      trans.injectVariables( configuration.getVariables() );

      trans.setLogLevel( configuration.getLogLevel() );
      trans.setMetaStore( metaStore );

      // Also copy the parameters over...
      //
      trans.copyParametersFrom( transMeta );
      transMeta.activateParameters();
      trans.activateParameters();

      // Run it!
      //
      trans.prepareExecution();
      trans.startThreads();
      trans.waitUntilFinished();
    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "Error running transformation locally", e );
    }
  }

  private void runTransRemote( CommandLine cmd, LogChannelInterface log, TransMeta transMeta, TransExecutionConfiguration configuration ) {
    SlaveServer slaveServer = configuration.getRemoteServer();
    slaveServer.shareVariablesWith( transMeta );
    try {
      runTransformationOnSlaveServer( log, transMeta, slaveServer, configuration, metaStore, dontWait, getQueryDelay() );
    } catch ( Exception e ) {
      throw new ExecutionException( cmd, e.getMessage(), e );
    }
  }

  public static Result runTransformationOnSlaveServer( LogChannelInterface log, TransMeta transMeta, SlaveServer slaveServer, TransExecutionConfiguration configuration, IMetaStore metaStore,
                                                       boolean dontWait, int queryDelay ) throws Exception {
    try {
      String carteObjectId = Trans.sendToSlaveServer( transMeta, configuration, metaStore );
      if ( !dontWait ) {
        slaveServer.monitorRemoteTransformation( log, carteObjectId, transMeta.getName(), queryDelay );
        SlaveServerTransStatus transStatus = slaveServer.getTransStatus( transMeta.getName(), carteObjectId, 0 );
        if ( configuration.isLogRemoteExecutionLocally() ) {
          log.logBasic( transStatus.getLoggingString() );
        }
        if ( transStatus.getNrStepErrors() > 0 ) {
          // Error
          throw new Exception( "Remote transformation ended with an error" );
        }

        return transStatus.getResult();
      }
      return null; // No status, we don't wait for it.
    } catch ( Exception e ) {
      throw new Exception( "Error executing transformation remotely on server '" + slaveServer.getName() + "'", e );
    }
  }

  private void runJob( CommandLine cmd, LogChannelInterface log ) {
    try {
      calculateRealFilename();

      // Run the job with the given filename
      //
      JobMeta jobMeta = new JobMeta( space, realFilename, metaStore );

      // Configure the basic execution settings
      //
      JobExecutionConfiguration configuration = new JobExecutionConfiguration();

      // Copy run config details from the metastore over to the run configuration
      //
      // parseRunConfiguration( cmd, configuration, metaStore );

      // Overwrite the run configuration with optional command line options
      //
      parseOptions( cmd, configuration, jobMeta );

      // Certain Pentaho plugins rely on this.  Meh.
      //
      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiJobBeforeStart.id, new Object[] { configuration, null, jobMeta, null } );

      // Before running, do we print the options?
      //
      if ( printingOptions ) {
        printOptions( configuration );
      }

      // Now we can run the job
      //
      if ( configuration.isExecutingLocally() ) {
        runJobLocal( cmd, log, configuration, jobMeta );
      } else if ( configuration.isExecutingRemotely() ) {
        // Simply remote execution.
        //
        runJobRemote( cmd, log, jobMeta, configuration );
      }


    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "There was an error during execution of job '" + filename + "'", e );
    }
  }

  private void runJobLocal( CommandLine cmd, LogChannelInterface log, JobExecutionConfiguration configuration, JobMeta jobMeta ) {
    try {
      Job job = new Job( null, jobMeta );
      job.initializeVariablesFrom( null );
      job.getJobMeta().setInternalHopVariables( job );
      job.injectVariables( configuration.getVariables() );

      job.setLogLevel( configuration.getLogLevel() );

      // Explicitly set parameters
      for ( String parameterName : configuration.getParams().keySet() ) {
        jobMeta.setParameterValue( parameterName, configuration.getParams().get( parameterName ) );
      }

      // Also copy the parameters over...
      //
      job.copyParametersFrom( jobMeta );
      jobMeta.activateParameters();
      job.activateParameters();

      job.start();
      job.waitUntilFinished();
    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "Error running job locally", e );
    }
  }

  private void runJobRemote( CommandLine cmd, LogChannelInterface log, JobMeta jobMeta, JobExecutionConfiguration configuration ) {
    SlaveServer slaveServer = configuration.getRemoteServer();
    slaveServer.shareVariablesWith( jobMeta );

    try {

    } catch ( Exception e ) {
      throw new ExecutionException( cmd, e.getMessage(), e );
    }
  }

  public static Result runJobOnSlaveServer( LogChannelInterface log, JobMeta jobMeta, SlaveServer slaveServer, JobExecutionConfiguration configuration, IMetaStore metaStore, boolean dontWait,
                                            boolean remoteLogging, int queryDelay ) throws Exception {
    try {
      String carteObjectId = Job.sendToSlaveServer( jobMeta, configuration, metaStore );

      // Monitor until finished...
      //
      SlaveServerJobStatus jobStatus = null;
      Result oneResult = new Result();

      while ( !dontWait ) {
        try {
          jobStatus = slaveServer.getJobStatus( jobMeta.getName(), carteObjectId, 0 );
          if ( jobStatus.getResult() != null ) {
            // The job is finished, get the result...
            //
            oneResult = jobStatus.getResult();
            break;
          }
        } catch ( Exception e1 ) {
          log.logError( "Unable to contact slave server [" + slaveServer + "] to verify the status of job [" + jobMeta.getName() + "]", e1 );
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
        jobStatus = slaveServer.getJobStatus( jobMeta.getName(), carteObjectId, 0 );
        if ( remoteLogging ) {
          log.logBasic( jobStatus.getLoggingString() );
        }
        Result result = jobStatus.getResult();
        if ( result.getNrErrors() > 0 ) {
          // Error
          throw new Exception( "Remote job ended with an error" );
        }
        return result;
      }
      return null;
    } catch ( Exception e ) {
      throw new Exception( "Error executing job remotely on server '" + slaveServer.getName() + "'", e );
    }
  }

  private int getQueryDelay() {
    if ( StringUtils.isEmpty( queryDelay ) ) {
      return 5;
    }
    return Const.toInt( queryDelay, 5 );
  }

  private void parseOptions( CommandLine cmd, ExecutionConfiguration configuration, NamedParams namedParams ) throws MetaStoreException {

    configuration.setSafeModeEnabled( safeMode );
    configuration.setGatheringMetrics( gatherMetrics );

    if ( StringUtils.isNotEmpty( slaveServerName ) ) {
      realSlaveServerName = space.environmentSubstitute( slaveServerName );
      configureSlaveServer( configuration, realSlaveServerName );
      configuration.setExecutingRemotely( true );
      configuration.setExecutingLocally( false );
    }
    configuration.setPassingExport( exportToSlaveServer );
    realRunConfigurationName = space.environmentSubstitute( runConfigurationName );
    configuration.setRunConfiguration( realRunConfigurationName );
    configuration.setLogLevel( LogLevel.getLogLevelForCode( space.environmentSubstitute( level ) ) );

    // Set variables and parameters...
    //
    parseParametersAndVariables( cmd, configuration, namedParams );
  }

  private void configureSlaveServer( ExecutionConfiguration configuration, String name ) throws MetaStoreException {
    MetaStoreFactory<SlaveServer> slaveFactory = new MetaStoreFactory<>( SlaveServer.class, metaStore, HopDefaults.NAMESPACE );
    SlaveServer slaveServer = slaveFactory.loadElement( name );
    if ( slaveServer == null ) {
      throw new ParameterException( cmd, "Unable to find slave server '" + name + "' in the metastore" );
    }
    configuration.setRemoteServer( slaveServer );
  }

  private boolean isTransformation() {
    if ( runTransformation ) {
      return true;
    }
    if ( StringUtils.isEmpty( filename ) ) {
      return false;
    }
    return filename.toLowerCase().endsWith( ".ktr" );
  }

  private boolean isJob() {
    if ( runJob ) {
      return true;
    }
    if ( StringUtils.isEmpty( filename ) ) {
      return false;
    }
    return filename.toLowerCase().endsWith( ".kjb" );
  }


  /**
   * Set the variables and parameters
   *
   * @param cmd
   * @param configuration
   * @param namedParams
   */
  private void parseParametersAndVariables( CommandLine cmd, ExecutionConfiguration configuration, NamedParams namedParams ) {
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
              configuration.getVariables().put( key, value );
            } else {
              // A parameter
              //
              configuration.getParams().put( key, value );
            }
          }
        }
      }
    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "There was an error during execution of transformation '" + filename + "'", e );
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
  private void configureParametersAndVariables( CommandLine cmd, ExecutionConfiguration configuration, VariableSpace space, NamedParams namedParams ) {

    // Copy variables over to the transformation or job metadata
    //
    space.injectVariables( configuration.getVariables() );

    // Set the parameter values
    //
    for ( String key : configuration.getParams().keySet() ) {
      String value = configuration.getParams().get( key );
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
      throw new ParameterException( new CommandLine( this ), "A filename is needed to run a job or transformation" );
    }
  }

  private void printOptions( ExecutionConfiguration configuration ) {
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

    if ( !configuration.getVariables().isEmpty() ) {
      log.logMinimal( "OPTION: Variables: " );
      for ( String variable : configuration.getVariables().keySet() ) {
        log.logMinimal( "  " + variable + " : '" + configuration.getVariables().get( variable ) );
      }
    }
    if ( !configuration.getParams().isEmpty() ) {
      log.logMinimal( "OPTION: Parameters: " );
      for ( String parameter : configuration.getParams().keySet() ) {
        log.logMinimal( "OPTION:   " + parameter + " : '" + configuration.getParams().get( parameter ) );
      }
    }
    if ( configuration.isSafeModeEnabled() ) {
      log.logMinimal( "OPTION: Safe mode enabled" );
    }

    if ( StringUtils.isNotEmpty( queryDelay ) ) {
      log.logMinimal( "OPTION: Remote server query delay : " + getQueryDelay() );
    }
    if ( dontWait ) {
      log.logMinimal( "OPTION: Do not wait for remote job or transformation to finish" );
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
  public LogChannelInterface getLog() {
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
   * Gets runTransformation
   *
   * @return value of runTransformation
   */
  public boolean isRunTransformation() {
    return runTransformation;
  }

  /**
   * @param runTransformation The runTransformation to set
   */
  public void setRunTransformation( boolean runTransformation ) {
    this.runTransformation = runTransformation;
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
   * Gets safeMode
   *
   * @return value of safeMode
   */
  public boolean isSafeMode() {
    return safeMode;
  }

  /**
   * @param safeMode The safeMode to set
   */
  public void setSafeMode( boolean safeMode ) {
    this.safeMode = safeMode;
  }

  /**
   * Gets gatherMetrics
   *
   * @return value of gatherMetrics
   */
  public boolean isGatherMetrics() {
    return gatherMetrics;
  }

  /**
   * @param gatherMetrics The gatherMetrics to set
   */
  public void setGatherMetrics( boolean gatherMetrics ) {
    this.gatherMetrics = gatherMetrics;
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
