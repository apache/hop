/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.job.entries.trans;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.SQLStatement;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannelFileWriter;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.parameters.NamedParams;
import org.apache.hop.core.parameters.NamedParamsDefault;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.util.CurrentDirectoryResolver;
import org.apache.hop.core.util.FileUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.DelegationListener;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.job.entry.JobEntryRunConfigurableInterface;
import org.apache.hop.job.entry.validator.AndValidator;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceNamingInterface;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.trans.StepWithMappingMeta;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransExecutionConfiguration;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.cluster.TransSplitter;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.www.SlaveServerTransStatus;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * This is the job entry that defines a transformation to be run.
 *
 * @author Matt Casters
 * @since 1-Oct-2003, rewritten on 18-June-2004
 */
public class JobEntryTrans extends JobEntryBase implements Cloneable, JobEntryInterface, JobEntryRunConfigurableInterface {
  private static Class<?> PKG = JobEntryTrans.class; // for i18n purposes, needed by Translator2!!
  public static final int IS_PENTAHO = 1;

  private String filename;

  public String[] arguments;

  public boolean argFromPrevious;

  public boolean paramsFromPrevious;

  public boolean execPerRow;

  public String[] parameters;

  public String[] parameterFieldNames;

  public String[] parameterValues;

  public boolean clearResultRows;

  public boolean clearResultFiles;

  public boolean createParentFolder;

  public boolean setLogfile;

  public boolean setAppendLogfile;

  public String logfile, logext;

  public boolean addDate, addTime;

  public LogLevel logFileLevel;

  private String directoryPath;

  private boolean clustering;

  public boolean waitingToFinish = true;

  public boolean followingAbortRemotely;

  private String remoteSlaveServerName;

  private boolean passingAllParameters = true;

  private boolean loggingRemoteWork;

  private String runConfiguration;

  private Trans trans;

  public JobEntryTrans( String name ) {
    super( name, "" );
  }

  public JobEntryTrans() {
    this( "" );
    clear();
  }

  private void allocateArgs( int nrArgs ) {
    arguments = new String[ nrArgs ];
  }

  private void allocateParams( int nrParameters ) {
    parameters = new String[ nrParameters ];
    parameterFieldNames = new String[ nrParameters ];
    parameterValues = new String[ nrParameters ];
  }

  @Override
  public Object clone() {
    JobEntryTrans je = (JobEntryTrans) super.clone();
    if ( arguments != null ) {
      int nrArgs = arguments.length;
      je.allocateArgs( nrArgs );
      System.arraycopy( arguments, 0, je.arguments, 0, nrArgs );
    }
    if ( parameters != null ) {
      int nrParameters = parameters.length;
      je.allocateParams( nrParameters );
      System.arraycopy( parameters, 0, je.parameters, 0, nrParameters );
      System.arraycopy( parameterFieldNames, 0, je.parameterFieldNames, 0, nrParameters );
      System.arraycopy( parameterValues, 0, je.parameterValues, 0, nrParameters );
    }
    return je;
  }

  public void setFileName( String n ) {
    filename = n;
  }

  /**
   * @return the filename
   * @deprecated use getFilename() instead
   */
  @Deprecated
  public String getFileName() {
    return filename;
  }

  @Override
  public String getFilename() {
    return filename;
  }

  @Override
  public String getRealFilename() {
    return environmentSubstitute( getFilename() );
  }

  public String getLogFilename() {
    String retval = "";
    if ( setLogfile ) {
      retval += logfile == null ? "" : logfile;
      Calendar cal = Calendar.getInstance();
      if ( addDate ) {
        SimpleDateFormat sdf = new SimpleDateFormat( "yyyyMMdd" );
        retval += "_" + sdf.format( cal.getTime() );
      }
      if ( addTime ) {
        SimpleDateFormat sdf = new SimpleDateFormat( "HHmmss" );
        retval += "_" + sdf.format( cal.getTime() );
      }
      if ( logext != null && logext.length() > 0 ) {
        retval += "." + logext;
      }
    }
    return retval;
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( super.getXML() );

    retval.append( "      " ).append( XMLHandler.addTagValue( "filename", filename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "arg_from_previous", argFromPrevious ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "params_from_previous", paramsFromPrevious ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "exec_per_row", execPerRow ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "clear_rows", clearResultRows ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "clear_files", clearResultFiles ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "set_logfile", setLogfile ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "logfile", logfile ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "logext", logext ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "add_date", addDate ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "add_time", addTime ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "loglevel", logFileLevel != null ? logFileLevel.getCode() : null ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "cluster", clustering ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "slave_server_name", remoteSlaveServerName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "set_append_logfile", setAppendLogfile ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "wait_until_finished", waitingToFinish ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "follow_abort_remote", followingAbortRemotely ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "create_parent_folder", createParentFolder ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "logging_remote_work", loggingRemoteWork ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "run_configuration", runConfiguration ) );

    if ( arguments != null ) {
      for ( int i = 0; i < arguments.length; i++ ) {
        // This is a very very bad way of making an XML file, don't use it (or
        // copy it). Sven Boden
        retval.append( "      " ).append( XMLHandler.addTagValue( "argument" + i, arguments[ i ] ) );
      }
    }

    if ( parameters != null ) {
      retval.append( "      " ).append( XMLHandler.openTag( "parameters" ) ).append( Const.CR );

      retval.append( "        " ).append( XMLHandler.addTagValue( "pass_all_parameters", passingAllParameters ) );

      for ( int i = 0; i < parameters.length; i++ ) {
        // This is a better way of making the XML file than the arguments.
        retval.append( "        " ).append( XMLHandler.openTag( "parameter" ) ).append( Const.CR );

        retval.append( "          " ).append( XMLHandler.addTagValue( "name", parameters[ i ] ) );
        retval.append( "          " ).append( XMLHandler.addTagValue( "stream_name", parameterFieldNames[ i ] ) );
        retval.append( "          " ).append( XMLHandler.addTagValue( "value", parameterValues[ i ] ) );

        retval.append( "        " ).append( XMLHandler.closeTag( "parameter" ) ).append( Const.CR );
      }
      retval.append( "      " ).append( XMLHandler.closeTag( "parameters" ) ).append( Const.CR );
    }

    return retval.toString();
  }

  @Override
  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );

      filename = XMLHandler.getTagValue( entrynode, "filename" );

      argFromPrevious = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "arg_from_previous" ) );
      paramsFromPrevious = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "params_from_previous" ) );
      execPerRow = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "exec_per_row" ) );
      clearResultRows = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "clear_rows" ) );
      clearResultFiles = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "clear_files" ) );
      setLogfile = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "set_logfile" ) );
      addDate = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "add_date" ) );
      addTime = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "add_time" ) );
      logfile = XMLHandler.getTagValue( entrynode, "logfile" );
      logext = XMLHandler.getTagValue( entrynode, "logext" );
      logFileLevel = LogLevel.getLogLevelForCode( XMLHandler.getTagValue( entrynode, "loglevel" ) );
      clustering = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "cluster" ) );
      createParentFolder = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "create_parent_folder" ) );
      loggingRemoteWork = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "logging_remote_work" ) );
      runConfiguration = XMLHandler.getTagValue( entrynode, "run_configuration" );

      remoteSlaveServerName = XMLHandler.getTagValue( entrynode, "slave_server_name" );

      setAppendLogfile = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "set_append_logfile" ) );
      String wait = XMLHandler.getTagValue( entrynode, "wait_until_finished" );
      if ( Utils.isEmpty( wait ) ) {
        waitingToFinish = true;
      } else {
        waitingToFinish = "Y".equalsIgnoreCase( wait );
      }

      followingAbortRemotely = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "follow_abort_remote" ) );

      // How many arguments?
      int argnr = 0;
      while ( XMLHandler.getTagValue( entrynode, "argument" + argnr ) != null ) {
        argnr++;
      }
      allocateArgs( argnr );

      // Read them all...
      for ( int a = 0; a < argnr; a++ ) {
        arguments[ a ] = XMLHandler.getTagValue( entrynode, "argument" + a );
      }

      Node parametersNode = XMLHandler.getSubNode( entrynode, "parameters" );

      String passAll = XMLHandler.getTagValue( parametersNode, "pass_all_parameters" );
      passingAllParameters = Utils.isEmpty( passAll ) || "Y".equalsIgnoreCase( passAll );

      int nrParameters = XMLHandler.countNodes( parametersNode, "parameter" );
      allocateParams( nrParameters );

      for ( int i = 0; i < nrParameters; i++ ) {
        Node knode = XMLHandler.getSubNodeByNr( parametersNode, "parameter", i );

        parameters[ i ] = XMLHandler.getTagValue( knode, "name" );
        parameterFieldNames[ i ] = XMLHandler.getTagValue( knode, "stream_name" );
        parameterValues[ i ] = XMLHandler.getTagValue( knode, "value" );
      }
    } catch ( HopException e ) {
      throw new HopXMLException( "Unable to load job entry of type 'trans' from XML node", e );
    }
  }

  @Override
  public void clear() {
    super.clear();

    filename = null;
    arguments = null;
    argFromPrevious = false;
    execPerRow = false;
    addDate = false;
    addTime = false;
    logfile = null;
    logext = null;
    setLogfile = false;
    clearResultRows = false;
    clearResultFiles = false;
    remoteSlaveServerName = null;
    setAppendLogfile = false;
    waitingToFinish = true;
    followingAbortRemotely = false; // backward compatibility reasons
    createParentFolder = false;
    logFileLevel = LogLevel.BASIC;
  }

  /**
   * Execute this job entry and return the result. In this case it means, just set the result boolean in the Result
   * class.
   *
   * @param result The result of the previous execution
   * @param nr     the job entry number
   * @return The Result of the execution.
   */
  @Override
  public Result execute( Result result, int nr ) throws HopException {
    result.setEntryNr( nr );

    LogChannelFileWriter logChannelFileWriter = null;

    LogLevel transLogLevel = parentJob.getLogLevel();

    String realLogFilename = "";
    if ( setLogfile ) {
      transLogLevel = logFileLevel;

      realLogFilename = environmentSubstitute( getLogFilename() );

      // We need to check here the log filename
      // if we do not have one, we must fail
      if ( Utils.isEmpty( realLogFilename ) ) {
        logError( BaseMessages.getString( PKG, "JobTrans.Exception.LogFilenameMissing" ) );
        result.setNrErrors( 1 );
        result.setResult( false );
        return result;
      }
      // create parent folder?
      if ( !FileUtil.createParentFolder( PKG, realLogFilename, createParentFolder, this.getLogChannel(), this ) ) {
        result.setNrErrors( 1 );
        result.setResult( false );
        return result;
      }
      try {
        logChannelFileWriter =
          new LogChannelFileWriter(
            this.getLogChannelId(), HopVFS.getFileObject( realLogFilename, this ), setAppendLogfile );
        logChannelFileWriter.startLogging();
      } catch ( HopException e ) {
        logError( BaseMessages.getString( PKG, "JobTrans.Error.UnableOpenAppender", realLogFilename, e.toString() ) );

        logError( Const.getStackTracker( e ) );
        result.setNrErrors( 1 );
        result.setResult( false );
        return result;
      }
    }

    logDetailed( BaseMessages.getString( PKG, "JobTrans.Log.OpeningTrans", environmentSubstitute( getFilename() ) ) );

    // Load the transformation only once for the complete loop!
    // Throws an exception if it was not possible to load the transformation. For example, the XML file doesn't exist or
    // the repository is down.
    // Log the stack trace and return an error condition from this
    //
    TransMeta transMeta = null;
    try {
      transMeta = getTransMeta( metaStore, this );
    } catch ( HopException e ) {
      logError( BaseMessages.getString( PKG, "JobTrans.Exception.UnableToRunJob", parentJobMeta.getName(),
        getName(), StringUtils.trim( e.getMessage() ) ), e );
      result.setNrErrors( 1 );
      result.setResult( false );
      return result;
    }

    int iteration = 0;
    String[] args1 = arguments;
    if ( args1 == null || args1.length == 0 ) { // No arguments set, look at the parent job.
      args1 = parentJob.getArguments();
    }
    // initializeVariablesFrom(parentJob);

    //
    // For the moment only do variable translation at the start of a job, not
    // for every input row (if that would be switched on). This is for safety,
    // the real argument setting is later on.
    //
    String[] args = null;
    if ( args1 != null ) {
      args = new String[ args1.length ];
      for ( int idx = 0; idx < args1.length; idx++ ) {
        args[ idx ] = environmentSubstitute( args1[ idx ] );
      }
    }

    RowMetaAndData resultRow = null;
    boolean first = true;
    List<RowMetaAndData> rows = new ArrayList<RowMetaAndData>( result.getRows() );

    while ( ( first && !execPerRow )
      || ( execPerRow && rows != null && iteration < rows.size() && result.getNrErrors() == 0 )
      && !parentJob.isStopped() ) {
      // Clear the result rows of the result
      // Otherwise we double the amount of rows every iteration in the simple cases.
      //
      if ( execPerRow ) {
        result.getRows().clear();
      }
      if ( rows != null && execPerRow ) {
        resultRow = rows.get( iteration );
      } else {
        resultRow = null;
      }

      NamedParams namedParam = new NamedParamsDefault();
      if ( parameters != null ) {
        for ( int idx = 0; idx < parameters.length; idx++ ) {
          if ( !Utils.isEmpty( parameters[ idx ] ) ) {
            // We have a parameter
            //
            namedParam.addParameterDefinition( parameters[ idx ], "", "Job entry runtime" );
            if ( Utils.isEmpty( Const.trim( parameterFieldNames[ idx ] ) ) ) {
              // There is no field name specified.
              //
              String value = Const.NVL( environmentSubstitute( parameterValues[ idx ] ), "" );
              namedParam.setParameterValue( parameters[ idx ], value );
            } else {
              // something filled in, in the field column...
              //
              String value = "";
              if ( resultRow != null ) {
                value = resultRow.getString( parameterFieldNames[ idx ], "" );
              }
              namedParam.setParameterValue( parameters[ idx ], value );
            }
          }
        }
      }

      first = false;

      Result previousResult = result;

      try {
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString(
            PKG, "JobTrans.StartingTrans", getFilename(), getName(), getDescription() ) );
        }

        if ( clearResultRows ) {
          previousResult.setRows( new ArrayList<RowMetaAndData>() );
        }

        if ( clearResultFiles ) {
          previousResult.getResultFiles().clear();
        }

        /*
         * Set one or more "result" rows on the transformation...
         */
        if ( execPerRow ) {
          // Execute for each input row

          if ( argFromPrevious ) {
            // Copy the input row to the (command line) arguments

            args = null;
            if ( resultRow != null ) {
              args = new String[ resultRow.size() ];
              for ( int i = 0; i < resultRow.size(); i++ ) {
                args[ i ] = resultRow.getString( i, null );
              }
            }
          } else {
            // Just pass a single row
            List<RowMetaAndData> newList = new ArrayList<RowMetaAndData>();
            newList.add( resultRow );

            // This previous result rows list can be either empty or not.
            // Depending on the checkbox "clear result rows"
            // In this case, it would execute the transformation with one extra row each time
            // Can't figure out a real use-case for it, but hey, who am I to decide that, right?
            // :-)
            //
            previousResult.getRows().addAll( newList );
          }

          if ( paramsFromPrevious ) { // Copy the input the parameters

            if ( parameters != null ) {
              for ( int idx = 0; idx < parameters.length; idx++ ) {
                if ( !Utils.isEmpty( parameters[ idx ] ) ) {
                  // We have a parameter
                  if ( Utils.isEmpty( Const.trim( parameterFieldNames[ idx ] ) ) ) {
                    namedParam.setParameterValue( parameters[ idx ], Const.NVL(
                      environmentSubstitute( parameterValues[ idx ] ), "" ) );
                  } else {
                    String fieldValue = "";

                    if ( resultRow != null ) {
                      fieldValue = resultRow.getString( parameterFieldNames[ idx ], "" );
                    }
                    // Get the value from the input stream
                    namedParam.setParameterValue( parameters[ idx ], Const.NVL( fieldValue, "" ) );
                  }
                }
              }
            }
          }
        } else {
          if ( argFromPrevious ) {
            // Only put the first Row on the arguments
            args = null;
            if ( resultRow != null ) {
              args = new String[ resultRow.size() ];
              for ( int i = 0; i < resultRow.size(); i++ ) {
                args[ i ] = resultRow.getString( i, null );
              }
            }
          }

          if ( paramsFromPrevious ) {
            // Copy the input the parameters
            if ( parameters != null ) {
              for ( int idx = 0; idx < parameters.length; idx++ ) {
                if ( !Utils.isEmpty( parameters[ idx ] ) ) {
                  // We have a parameter
                  if ( Utils.isEmpty( Const.trim( parameterFieldNames[ idx ] ) ) ) {
                    namedParam.setParameterValue( parameters[ idx ], Const.NVL(
                      environmentSubstitute( parameterValues[ idx ] ), "" ) );
                  } else {
                    String fieldValue = "";

                    if ( resultRow != null ) {
                      fieldValue = resultRow.getString( parameterFieldNames[ idx ], "" );
                    }
                    // Get the value from the input stream
                    namedParam.setParameterValue( parameters[ idx ], Const.NVL( fieldValue, "" ) );
                  }
                }
              }
            }
          }
        }

        // Handle the parameters...
        //
        transMeta.clearParameters();
        String[] parameterNames = transMeta.listParameters();

        prepareFieldNamesParameters( parameters, parameterFieldNames, parameterValues, namedParam, this );

        StepWithMappingMeta.activateParams( transMeta, transMeta, this, parameterNames,
          parameters, parameterValues, isPassingAllParameters() );
        boolean doFallback = true;
        SlaveServer remoteSlaveServer = null;
        TransExecutionConfiguration executionConfiguration = new TransExecutionConfiguration();
        if ( !Utils.isEmpty( runConfiguration ) ) {
          runConfiguration = environmentSubstitute( runConfiguration );
          log.logBasic( BaseMessages.getString( PKG, "JobTrans.RunConfig.Message" ), runConfiguration );
          executionConfiguration.setRunConfiguration( runConfiguration );
          try {
            ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiTransBeforeStart.id, new Object[] {
              executionConfiguration, parentJob.getJobMeta(), transMeta } );
            List<Object> items = Arrays.asList( runConfiguration, false );
            try {
              ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint
                .RunConfigurationSelection.id, items );
              if ( waitingToFinish && (Boolean) items.get( IS_PENTAHO ) ) {
                String jobName = parentJob.getJobMeta().getName();
                String name = transMeta.getName();
                logBasic( BaseMessages.getString( PKG, "JobTrans.Log.InvalidRunConfigurationCombination", jobName,
                  name, jobName ) );
              }
            } catch ( Exception ignored ) {
              // Ignored
            }
            if ( !executionConfiguration.isExecutingLocally() && !executionConfiguration.isExecutingRemotely() && !executionConfiguration.isExecutingClustered() ) {
              result.setResult( true );
              return result;
            }
            clustering = executionConfiguration.isExecutingClustered();
            remoteSlaveServer = executionConfiguration.getRemoteServer();
            doFallback = false;
          } catch ( HopException e ) {
            log.logError( e.getMessage(), getName() );
            result.setNrErrors( 1 );
            result.setResult( false );
            return result;
          }
        }

        if ( doFallback ) {
          // Figure out the remote slave server...
          //
          if ( !Utils.isEmpty( remoteSlaveServerName ) ) {
            String realRemoteSlaveServerName = environmentSubstitute( remoteSlaveServerName );
            remoteSlaveServer = parentJob.getJobMeta().findSlaveServer( realRemoteSlaveServerName );
            if ( remoteSlaveServer == null ) {
              throw new HopException( BaseMessages.getString(
                PKG, "JobTrans.Exception.UnableToFindRemoteSlaveServer", realRemoteSlaveServerName ) );
            }
          }
        }


        // Execute this transformation across a cluster of servers
        //
        if ( clustering ) {
          executionConfiguration.setClusterPosting( true );
          executionConfiguration.setClusterPreparing( true );
          executionConfiguration.setClusterStarting( true );
          executionConfiguration.setClusterShowingTransformation( false );
          executionConfiguration.setSafeModeEnabled( false );
          executionConfiguration.setLogLevel( transLogLevel );
          executionConfiguration.setPreviousResult( previousResult );

          // Also pass the variables from the transformation into the execution configuration
          // That way it can go over the HTTP connection to the slave server.
          //
          executionConfiguration.setVariables( transMeta );

          // Also set the arguments...
          //
          executionConfiguration.setArgumentStrings( args );

          if ( parentJob.getJobMeta().isBatchIdPassed() ) {
            executionConfiguration.setPassedBatchId( parentJob.getPassedBatchId() );
          }

          TransSplitter transSplitter = null;
          long errors = 0;
          try {
            transSplitter = Trans.executeClustered( transMeta, executionConfiguration );

            // Monitor the running transformations, wait until they are done.
            // Also kill them all if anything goes bad
            // Also clean up afterwards...
            //
            errors += Trans.monitorClusteredTransformation( log, transSplitter, parentJob );

          } catch ( Exception e ) {
            logError( "Error during clustered execution. Cleaning up clustered execution.", e );
            // In case something goes wrong, make sure to clean up afterwards!
            //
            errors++;
            if ( transSplitter != null ) {
              Trans.cleanupCluster( log, transSplitter );
            } else {
              // Try to clean anyway...
              //
              SlaveServer master = null;
              for ( StepMeta stepMeta : transMeta.getSteps() ) {
                if ( stepMeta.isClustered() ) {
                  for ( SlaveServer slaveServer : stepMeta.getClusterSchema().getSlaveServers() ) {
                    if ( slaveServer.isMaster() ) {
                      master = slaveServer;
                      break;
                    }
                  }
                }
              }
              if ( master != null ) {
                master.deAllocateServerSockets( transMeta.getName(), null );
              }
            }
          }

          result.clear();

          if ( transSplitter != null ) {
            Result clusterResult = Trans.getClusteredTransformationResult( log, transSplitter, parentJob,
              executionConfiguration.isLogRemoteExecutionLocally() );
            result.add( clusterResult );
          }

          result.setNrErrors( result.getNrErrors() + errors );

        } else if ( remoteSlaveServer != null ) {
          // Execute this transformation remotely
          //

          // Make sure we can parameterize the slave server connection
          //
          remoteSlaveServer.shareVariablesWith( this );

          // Remote execution...
          //
          executionConfiguration.setPreviousResult( previousResult.clone() );
          executionConfiguration.setArgumentStrings( args );
          executionConfiguration.setVariables( this );
          executionConfiguration.setRemoteServer( remoteSlaveServer );
          executionConfiguration.setLogLevel( transLogLevel );
          executionConfiguration.setLogFileName( realLogFilename );
          executionConfiguration.setSetAppendLogfile( setAppendLogfile );
          executionConfiguration.setSetLogfile( setLogfile );

          Map<String, String> params = executionConfiguration.getParams();
          for ( String param : transMeta.listParameters() ) {
            String value =
              Const.NVL( transMeta.getParameterValue( param ), Const.NVL(
                transMeta.getParameterDefault( param ), transMeta.getVariable( param ) ) );
            params.put( param, value );
          }

          if ( parentJob.getJobMeta().isBatchIdPassed() ) {
            executionConfiguration.setPassedBatchId( parentJob.getPassedBatchId() );
          }

          // Send the XML over to the slave server
          // Also start the transformation over there...
          //
          String carteObjectId = Trans.sendToSlaveServer( transMeta, executionConfiguration, metaStore );

          // Now start the monitoring...
          //
          SlaveServerTransStatus transStatus = null;
          while ( !parentJob.isStopped() && waitingToFinish ) {
            try {
              transStatus = remoteSlaveServer.getTransStatus( transMeta.getName(), carteObjectId, 0 );
              if ( !transStatus.isRunning() ) {
                // The transformation is finished, get the result...
                //
                //get the status with the result ( we don't do it above because of changing PDI-15781)
                transStatus = remoteSlaveServer.getTransStatus( transMeta.getName(), carteObjectId, 0, true );
                Result remoteResult = transStatus.getResult();
                result.clear();
                result.add( remoteResult );

                // In case you manually stop the remote trans (browser etc), make sure it's marked as an error
                //
                if ( remoteResult.isStopped() ) {
                  result.setNrErrors( result.getNrErrors() + 1 ); //
                }

                // Make sure to clean up : write a log record etc, close any left-over sockets etc.
                //
                remoteSlaveServer.cleanupTransformation( transMeta.getName(), carteObjectId );

                break;
              }
            } catch ( Exception e1 ) {

              logError( BaseMessages.getString( PKG, "JobTrans.Error.UnableContactSlaveServer", ""
                + remoteSlaveServer, transMeta.getName() ), e1 );
              result.setNrErrors( result.getNrErrors() + 1L );
              break; // Stop looking too, chances are too low the server will come back on-line
            }

            // sleep for 2 seconds
            try {
              Thread.sleep( 2000 );
            } catch ( InterruptedException e ) {
              // Ignore
            }
          }

          if ( parentJob.isStopped() ) {
            // See if we have a status and if we need to stop the remote execution here...
            //
            if ( transStatus == null || transStatus.isRunning() ) {
              // Try a remote abort ...
              //
              remoteSlaveServer.stopTransformation( transMeta.getName(), transStatus.getId() );

              // And a cleanup...
              //
              remoteSlaveServer.cleanupTransformation( transMeta.getName(), transStatus.getId() );

              // Set an error state!
              //
              result.setNrErrors( result.getNrErrors() + 1L );
            }
          }

        } else {

          // Execute this transformation on the local machine
          //

          // Create the transformation from meta-data
          //
          final TransMeta meta = transMeta;
          trans = new Trans( meta, this );
          trans.setParent( this );

          // Pass the socket repository as early as possible...
          //
          trans.setSocketRepository( parentJob.getSocketRepository() );

          if ( parentJob.getJobMeta().isBatchIdPassed() ) {
            trans.setPassedBatchId( parentJob.getPassedBatchId() );
          }

          // set the parent job on the transformation, variables are taken from here...
          //
          trans.setParentJob( parentJob );
          trans.setParentVariableSpace( parentJob );
          trans.setLogLevel( transLogLevel );
          trans.setPreviousResult( previousResult );
          trans.setArguments( arguments );

          // inject the metaStore
          trans.setMetaStore( metaStore );

          // First get the root job
          //
          Job rootJob = parentJob;
          while ( rootJob.getParentJob() != null ) {
            rootJob = rootJob.getParentJob();
          }

          // Get the start and end-date from the root job...
          //
          trans.setJobStartDate( rootJob.getStartDate() );
          trans.setJobEndDate( rootJob.getEndDate() );

          // Inform the parent job we started something here...
          //
          for ( DelegationListener delegationListener : parentJob.getDelegationListeners() ) {
            // TODO: copy some settings in the job execution configuration, not strictly needed
            // but the execution configuration information is useful in case of a job re-start
            //
            delegationListener.transformationDelegationStarted( trans, new TransExecutionConfiguration() );
          }

          try {
            // Start execution...
            //
            trans.execute( args );

            // Wait until we're done with it...
            //TODO is it possible to implement Observer pattern to avoid Thread.sleep here?
            while ( !trans.isFinished() && trans.getErrors() == 0 ) {
              if ( parentJob.isStopped() ) {
                trans.stopAll();
                break;
              } else {
                try {
                  Thread.sleep( 0, 500 );
                } catch ( InterruptedException e ) {
                  // Ignore errors
                }
              }
            }
            trans.waitUntilFinished();

            if ( parentJob.isStopped() || trans.getErrors() != 0 ) {
              trans.stopAll();
              result.setNrErrors( 1 );
            }
            updateResult( result );
            if ( setLogfile ) {
              ResultFile resultFile =
                new ResultFile(
                  ResultFile.FILE_TYPE_LOG, HopVFS.getFileObject( realLogFilename, this ), parentJob
                  .getJobname(), toString()
                );
              result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
            }
          } catch ( HopException e ) {

            logError( BaseMessages.getString( PKG, "JobTrans.Error.UnablePrepareExec" ), e );
            result.setNrErrors( 1 );
          }
        }
      } catch ( Exception e ) {

        logError( BaseMessages.getString( PKG, "JobTrans.ErrorUnableOpenTrans", e.getMessage() ) );
        logError( Const.getStackTracker( e ) );
        result.setNrErrors( 1 );
      }
      iteration++;
    }

    if ( setLogfile ) {
      if ( logChannelFileWriter != null ) {
        logChannelFileWriter.stopLogging();

        ResultFile resultFile =
          new ResultFile(
            ResultFile.FILE_TYPE_LOG, logChannelFileWriter.getLogFile(), parentJob.getJobname(), getName() );
        result.getResultFiles().put( resultFile.getFile().toString(), resultFile );

        // See if anything went wrong during file writing...
        //
        if ( logChannelFileWriter.getException() != null ) {
          logError( "Unable to open log file [" + getLogFilename() + "] : " );
          logError( Const.getStackTracker( logChannelFileWriter.getException() ) );
          result.setNrErrors( 1 );
          result.setResult( false );
          return result;
        }
      }
    }

    if ( result.getNrErrors() == 0 ) {
      result.setResult( true );
    } else {
      result.setResult( false );
    }

    return result;
  }

  protected void updateResult( Result result ) {
    Result newResult = trans.getResult();
    result.clear(); // clear only the numbers, NOT the files or rows.
    result.add( newResult );
    result.setRows( newResult.getRows() );
  }

  public TransMeta getTransMeta( IMetaStore metaStore, VariableSpace space ) throws HopException {
    try {
      TransMeta transMeta = null;
      CurrentDirectoryResolver r = new CurrentDirectoryResolver();
      VariableSpace tmpSpace = r.resolveCurrentDirectory( space, parentJob, getFilename() );

      String realFilename = tmpSpace.environmentSubstitute( getFilename() );

      transMeta = new TransMeta( realFilename, metaStore, true, this );

      if ( transMeta != null ) {
        // set Internal.Entry.Current.Directory again because it was changed
        transMeta.setInternalHopVariables();
        //  When the child parameter does exist in the parent parameters, overwrite the child parameter by the
        // parent parameter.

        StepWithMappingMeta.replaceVariableValues( transMeta, space, "Trans" );
        if ( isPassingAllParameters() ) {
          // All other parent parameters need to get copied into the child parameters  (when the 'Inherit all
          // variables from the transformation?' option is checked)
          StepWithMappingMeta.addMissingVariables( transMeta, space );
        }
        // Pass repository and metastore references
        //
        transMeta.setMetaStore( metaStore );
      }

      return transMeta;
    } catch ( final HopException ke ) {
      // if we get a HopException, simply re-throw it
      throw ke;
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "JobTrans.Exception.MetaDataLoad" ), e );
    }
  }

  @Override
  public boolean evaluates() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return true;
  }

  @Override
  public List<SQLStatement> getSQLStatements( IMetaStore metaStore, VariableSpace space ) throws HopException {
    this.copyVariablesFrom( space );
    TransMeta transMeta = getTransMeta( metaStore, this );

    return transMeta.getSQLStatements();
  }

  /**
   * @return Returns the directoryPath.
   */
  public String getDirectoryPath() {
    return directoryPath;
  }

  /**
   * @param directoryPath The directoryPath to set.
   */
  public void setDirectoryPath( String directoryPath ) {
    this.directoryPath = directoryPath;
  }

  /**
   * @return the clustering
   */
  public boolean isClustering() {
    return clustering;
  }

  /**
   * @param clustering the clustering to set
   */
  public void setClustering( boolean clustering ) {
    this.clustering = clustering;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space,
                     IMetaStore metaStore ) {
    if ( setLogfile ) {
      JobEntryValidatorUtils.andValidator().validate( this, "logfile", remarks,
        AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );
    }
    if ( !Utils.isEmpty( filename ) ) {
      JobEntryValidatorUtils.andValidator().validate( this, "filename", remarks,
        AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );
    } else {
      JobEntryValidatorUtils.andValidator().validate( this, "transname", remarks,
        AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );
      JobEntryValidatorUtils.andValidator().validate( this, "directory", remarks,
        AndValidator.putValidators( JobEntryValidatorUtils.notNullValidator() ) );
    }
  }

  @Override
  public List<ResourceReference> getResourceDependencies( JobMeta jobMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( jobMeta );
    if ( !Utils.isEmpty( filename ) ) {
      // During this phase, the variable space hasn't been initialized yet - it seems
      // to happen during the execute. As such, we need to use the job meta's resolution
      // of the variables.
      String realFileName = jobMeta.environmentSubstitute( filename );
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( realFileName, ResourceType.ACTIONFILE ) );
      references.add( reference );
    }
    return references;
  }

  /**
   * We're going to load the transformation meta data referenced here. Then we're going to give it a new filename,
   * modify that filename in this entries. The parent caller will have made a copy of it, so it should be OK to do so.
   * <p/>
   * Exports the object to a flat-file system, adding content with filename keys to a set of definitions. The supplied
   * resource naming interface allows the object to name appropriately without worrying about those parts of the
   * implementation specific details.
   *
   * @param space           The variable space to resolve (environment) variables with.
   * @param definitions     The map containing the filenames and content
   * @param namingInterface The resource naming interface allows the object to be named appropriately
   * @param metaStore       the metaStore to load external metadata from
   * @return The filename for this object. (also contained in the definitions map)
   * @throws HopException in case something goes wrong during the export
   */
  @Override
  public String exportResources( VariableSpace space, Map<String, ResourceDefinition> definitions,
                                 ResourceNamingInterface namingInterface, IMetaStore metaStore ) throws HopException {
    // Try to load the transformation from repository or file.
    // Modify this recursively too...
    //
    // AGAIN: there is no need to clone this job entry because the caller is responsible for this.
    //
    // First load the transformation metadata...
    //
    copyVariablesFrom( space );
    TransMeta transMeta = getTransMeta( metaStore, space );

    // Also go down into the transformation and export the files there. (mapping recursively down)
    //
    String proposedNewFilename =
      transMeta.exportResources( transMeta, definitions, namingInterface, metaStore );

    // To get a relative path to it, we inject ${Internal.Entry.Current.Directory}
    //
    String newFilename = "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY + "}/" + proposedNewFilename;

    // Set the correct filename inside the XML.
    //
    transMeta.setFilename( newFilename );

    // change it in the job entry
    //
    filename = newFilename;

    return proposedNewFilename;
  }

  protected String getLogfile() {
    return logfile;
  }

  /**
   * @return the remote slave server name
   */
  public String getRemoteSlaveServerName() {
    return remoteSlaveServerName;
  }

  /**
   * @param remoteSlaveServerName the remote slave server name to set
   */
  public void setRemoteSlaveServerName( String remoteSlaveServerName ) {
    this.remoteSlaveServerName = remoteSlaveServerName;
  }

  /**
   * @return the waitingToFinish
   */
  public boolean isWaitingToFinish() {
    return waitingToFinish;
  }

  /**
   * @param waitingToFinish the waitingToFinish to set
   */
  public void setWaitingToFinish( boolean waitingToFinish ) {
    this.waitingToFinish = waitingToFinish;
  }

  /**
   * @return the followingAbortRemotely
   */
  public boolean isFollowingAbortRemotely() {
    return followingAbortRemotely;
  }

  /**
   * @param followingAbortRemotely the followingAbortRemotely to set
   */
  public void setFollowingAbortRemotely( boolean followingAbortRemotely ) {
    this.followingAbortRemotely = followingAbortRemotely;
  }

  public boolean isLoggingRemoteWork() {
    return loggingRemoteWork;
  }

  public void setLoggingRemoteWork( boolean loggingRemoteWork ) {
    this.loggingRemoteWork = loggingRemoteWork;
  }

  /**
   * @return the passingAllParameters
   */
  public boolean isPassingAllParameters() {
    return passingAllParameters;
  }

  /**
   * @param passingAllParameters the passingAllParameters to set
   */
  public void setPassingAllParameters( boolean passingAllParameters ) {
    this.passingAllParameters = passingAllParameters;
  }

  public String getRunConfiguration() {
    return runConfiguration;
  }

  public void setRunConfiguration( String runConfiguration ) {
    this.runConfiguration = runConfiguration;
  }

  public Trans getTrans() {
    return trans;
  }

  /**
   * @return The objects referenced in the step, like a a transformation, a job, a mapper, a reducer, a combiner, ...
   */
  @Override
  public String[] getReferencedObjectDescriptions() {
    return new String[] { BaseMessages.getString( PKG, "JobEntryTrans.ReferencedObject.Description" ), };
  }

  private boolean isTransformationDefined() {
    return StringUtils.isNotEmpty( filename );
  }

  @Override
  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] { isTransformationDefined(), };
  }

  /**
   * Load the referenced object
   *
   * @param index     the referenced object index to load (in case there are multiple references)
   * @param metaStore metaStore
   * @param space     the variable space to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  @Override
  public Object loadReferencedObject( int index, IMetaStore metaStore, VariableSpace space ) throws HopException {
    return getTransMeta( metaStore, space );
  }

  @Override
  public void setParentJobMeta( JobMeta parentJobMeta ) {
    JobMeta previous = getParentJobMeta();
    super.setParentJobMeta( parentJobMeta );
    if ( parentJobMeta != null ) {
      variables.setParentVariableSpace( parentJobMeta );
    }
  }

  public void prepareFieldNamesParameters( String[] parameters, String[] parameterFieldNames, String[] parameterValues,
                                           NamedParams namedParam, JobEntryTrans jobEntryTrans )
    throws UnknownParamException {
    for ( int idx = 0; idx < parameters.length; idx++ ) {
      // Grab the parameter value set in the Trans job entry
      // Set fieldNameParameter only if exists and if it is not declared any staticValue( parameterValues array )
      //
      String thisValue = namedParam.getParameterValue( parameters[ idx ] );
      // Set value only if is not empty at namedParam and exists in parameterFieldNames
      if ( !Utils.isEmpty( thisValue ) && idx < parameterFieldNames.length ) {
        // If exists then ask if is not empty
        if ( !Utils.isEmpty( Const.trim( parameterFieldNames[ idx ] ) ) ) {
          // If is not empty then we have to ask if it exists too in parameterValues array, since the values in
          // parameterValues prevail over parameterFieldNames
          if ( idx < parameterValues.length ) {
            // If is empty at parameterValues array, then we can finally add that variable with that value
            if ( Utils.isEmpty( Const.trim( parameterValues[ idx ] ) ) ) {
              jobEntryTrans.setVariable( parameters[ idx ], thisValue );
            }
          } else {
            // Or if not in parameterValues then we can add that variable with that value too
            jobEntryTrans.setVariable( parameters[ idx ], thisValue );
          }
        }
      }
    }
  }

}
