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

package org.apache.hop.job.entries.pipeline;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.SQLStatement;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.logging.LogChannelFileWriter;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.parameters.INamedParams;
import org.apache.hop.core.parameters.NamedParamsDefault;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.util.CurrentDirectoryResolver;
import org.apache.hop.core.util.FileUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.IDelegationListener;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.IJobEntry;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.IJobEntryRunConfigurable;
import org.apache.hop.job.entry.validator.AndValidator;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.www.SlaveServerPipelineStatus;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * This is the job entry that defines a pipeline to be run.
 *
 * @author Matt Casters
 * @since 1-Oct-2003, rewritten on 18-June-2004
 */
public class JobEntryPipeline extends JobEntryBase implements Cloneable, IJobEntry, IJobEntryRunConfigurable {
  private static Class<?> PKG = JobEntryPipeline.class; // for i18n purposes, needed by Translator!!
  public static final int IS_PENTAHO = 1;

  private String filename;

  public String[] arguments;

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

  public boolean waitingToFinish = true;

  public boolean followingAbortRemotely;

  private String remoteSlaveServerName;

  private boolean passingAllParameters = true;

  private boolean loggingRemoteWork;

  private String runConfiguration;

  private Pipeline pipeline;

  public JobEntryPipeline( String name ) {
    super( name, "" );
  }

  public JobEntryPipeline() {
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
    JobEntryPipeline je = (JobEntryPipeline) super.clone();
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
      throw new HopXMLException( "Unable to load job entry of type 'pipeline' from XML node", e );
    }
  }

  @Override
  public void clear() {
    super.clear();

    filename = null;
    arguments = null;
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

    LogLevel pipelineLogLevel = parentJob.getLogLevel();

    String realLogFilename = "";
    if ( setLogfile ) {
      pipelineLogLevel = logFileLevel;

      realLogFilename = environmentSubstitute( getLogFilename() );

      // We need to check here the log filename
      // if we do not have one, we must fail
      if ( Utils.isEmpty( realLogFilename ) ) {
        logError( BaseMessages.getString( PKG, "JobPipeline.Exception.LogFilenameMissing" ) );
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
        logError( BaseMessages.getString( PKG, "JobPipeline.Error.UnableOpenAppender", realLogFilename, e.toString() ) );

        logError( Const.getStackTracker( e ) );
        result.setNrErrors( 1 );
        result.setResult( false );
        return result;
      }
    }

    logDetailed( BaseMessages.getString( PKG, "JobPipeline.Log.OpeningPipeline", environmentSubstitute( getFilename() ) ) );

    // Load the pipeline only once for the complete loop!
    // Throws an exception if it was not possible to load the pipeline, for example if the XML file doesn't exist.
    // Log the stack trace and return an error condition from this
    //
    PipelineMeta pipelineMeta = null;
    try {
      pipelineMeta = getPipelineMeta( metaStore, this );
    } catch ( HopException e ) {
      logError( BaseMessages.getString( PKG, "JobPipeline.Exception.UnableToRunJob", parentJobMeta.getName(),
        getName(), StringUtils.trim( e.getMessage() ) ), e );
      result.setNrErrors( 1 );
      result.setResult( false );
      return result;
    }

    int iteration = 0;

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

      INamedParams namedParam = new NamedParamsDefault();
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
            PKG, "JobPipeline.StartingPipeline", getFilename(), getName(), getDescription() ) );
        }

        if ( clearResultRows ) {
          previousResult.setRows( new ArrayList<RowMetaAndData>() );
        }

        if ( clearResultFiles ) {
          previousResult.getResultFiles().clear();
        }

        /*
         * Set one or more "result" rows on the pipeline...
         */
        if ( execPerRow ) {
          // Execute for each input row

          // Just pass a single row
          List<RowMetaAndData> newList = new ArrayList<RowMetaAndData>();
          newList.add( resultRow );

          // This previous result rows list can be either empty or not.
          // Depending on the checkbox "clear result rows"
          // In this case, it would execute the pipeline with one extra row each time
          // Can't figure out a real use-case for it, but hey, who am I to decide that, right?
          // :-)
          //
          previousResult.getRows().addAll( newList );


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
        pipelineMeta.clearParameters();
        String[] parameterNames = pipelineMeta.listParameters();

        prepareFieldNamesParameters( parameters, parameterFieldNames, parameterValues, namedParam, this );

        TransformWithMappingMeta.activateParams( pipelineMeta, pipelineMeta, this, parameterNames,
          parameters, parameterValues, isPassingAllParameters() );
        boolean doFallback = true;
        SlaveServer remoteSlaveServer = null;
        PipelineExecutionConfiguration executionConfiguration = new PipelineExecutionConfiguration();
        if ( !Utils.isEmpty( runConfiguration ) ) {
          runConfiguration = environmentSubstitute( runConfiguration );
          log.logBasic( BaseMessages.getString( PKG, "JobPipeline.RunConfig.Message" ), runConfiguration );
          executionConfiguration.setRunConfiguration( runConfiguration );
          try {
            ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiPipelineBeforeStart.id, new Object[] {
              executionConfiguration, parentJob.getJobMeta(), pipelineMeta } );
            List<Object> items = Arrays.asList( runConfiguration, false );
            try {
              ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint
                .RunConfigurationSelection.id, items );
              if ( waitingToFinish && (Boolean) items.get( IS_PENTAHO ) ) {
                String jobName = parentJob.getJobMeta().getName();
                String name = pipelineMeta.getName();
                logBasic( BaseMessages.getString( PKG, "JobPipeline.Log.InvalidRunConfigurationCombination", jobName,
                  name, jobName ) );
              }
            } catch ( Exception ignored ) {
              // Ignored
            }
            if ( !executionConfiguration.isExecutingLocally() && !executionConfiguration.isExecutingRemotely() ) {
              result.setResult( true );
              return result;
            }
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
                PKG, "JobPipeline.Exception.UnableToFindRemoteSlaveServer", realRemoteSlaveServerName ) );
            }
          }
        }

        if ( remoteSlaveServer != null ) {
          // Execute this pipeline remotely
          //

          // Make sure we can parameterize the slave server connection
          //
          remoteSlaveServer.shareVariablesWith( this );

          // Remote execution...
          //
          executionConfiguration.setPreviousResult( previousResult.clone() );
          executionConfiguration.setVariablesMap( this );
          executionConfiguration.setRemoteServer( remoteSlaveServer );
          executionConfiguration.setLogLevel( pipelineLogLevel );
          executionConfiguration.setLogFileName( realLogFilename );
          executionConfiguration.setSetAppendLogfile( setAppendLogfile );
          executionConfiguration.setSetLogfile( setLogfile );

          Map<String, String> params = executionConfiguration.getParametersMap();
          for ( String param : pipelineMeta.listParameters() ) {
            String value =
              Const.NVL( pipelineMeta.getParameterValue( param ), Const.NVL(
                pipelineMeta.getParameterDefault( param ), pipelineMeta.getVariable( param ) ) );
            params.put( param, value );
          }

          if ( parentJob.getJobMeta().isBatchIdPassed() ) {
            executionConfiguration.setPassedBatchId( parentJob.getPassedBatchId() );
          }

          // Send the XML over to the slave server
          // Also start the pipeline over there...
          //
          String carteObjectId = Pipeline.sendToSlaveServer( pipelineMeta, executionConfiguration, metaStore );

          // Now start the monitoring...
          //
          SlaveServerPipelineStatus pipelineStatus = null;
          while ( !parentJob.isStopped() && waitingToFinish ) {
            try {
              pipelineStatus = remoteSlaveServer.getPipelineStatus( pipelineMeta.getName(), carteObjectId, 0 );
              if ( !pipelineStatus.isRunning() ) {
                // The pipeline is finished, get the result...
                //
                //get the status with the result ( we don't do it above because of changing PDI-15781)
                pipelineStatus = remoteSlaveServer.getPipelineStatus( pipelineMeta.getName(), carteObjectId, 0, true );
                Result remoteResult = pipelineStatus.getResult();
                result.clear();
                result.add( remoteResult );

                // In case you manually stop the remote pipeline (browser etc), make sure it's marked as an error
                //
                if ( remoteResult.isStopped() ) {
                  result.setNrErrors( result.getNrErrors() + 1 ); //
                }

                // Make sure to clean up : write a log record etc, close any left-over sockets etc.
                //
                remoteSlaveServer.cleanupPipeline( pipelineMeta.getName(), carteObjectId );

                break;
              }
            } catch ( Exception e1 ) {

              logError( BaseMessages.getString( PKG, "JobPipeline.Error.UnableContactSlaveServer", ""
                + remoteSlaveServer, pipelineMeta.getName() ), e1 );
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
            if ( pipelineStatus == null || pipelineStatus.isRunning() ) {
              // Try a remote abort ...
              //
              remoteSlaveServer.stopPipeline( pipelineMeta.getName(), pipelineStatus.getId() );

              // And a cleanup...
              //
              remoteSlaveServer.cleanupPipeline( pipelineMeta.getName(), pipelineStatus.getId() );

              // Set an error state!
              //
              result.setNrErrors( result.getNrErrors() + 1L );
            }
          }

        } else {

          // Execute this pipeline on the local machine
          //

          // Create the pipeline from meta-data
          //
          final PipelineMeta meta = pipelineMeta;
          pipeline = new Pipeline( meta, this );
          pipeline.setParent( this );

          // Pass the socket repository as early as possible...
          //
          pipeline.setSocketRepository( parentJob.getSocketRepository() );

          if ( parentJob.getJobMeta().isBatchIdPassed() ) {
            pipeline.setPassedBatchId( parentJob.getPassedBatchId() );
          }

          // set the parent job on the pipeline, variables are taken from here...
          //
          pipeline.setParentJob( parentJob );
          pipeline.setParentVariableSpace( parentJob );
          pipeline.setLogLevel( pipelineLogLevel );
          pipeline.setPreviousResult( previousResult );

          // inject the metaStore
          pipeline.setMetaStore( metaStore );

          // First get the root job
          //
          Job rootJob = parentJob;
          while ( rootJob.getParentJob() != null ) {
            rootJob = rootJob.getParentJob();
          }

          // Get the start and end-date from the root job...
          //
          pipeline.setJobStartDate( rootJob.getStartDate() );
          pipeline.setJobEndDate( rootJob.getEndDate() );

          // Inform the parent job we started something here...
          //
          for ( IDelegationListener delegationListener : parentJob.getDelegationListeners() ) {
            // TODO: copy some settings in the job execution configuration, not strictly needed
            // but the execution configuration information is useful in case of a job re-start
            //
            delegationListener.pipelineDelegationStarted( pipeline, new PipelineExecutionConfiguration() );
          }

          try {
            // Start execution...
            //
            pipeline.execute();

            // Wait until we're done with it...
            //TODO is it possible to implement Observer pattern to avoid Thread.sleep here?
            while ( !pipeline.isFinished() && pipeline.getErrors() == 0 ) {
              if ( parentJob.isStopped() ) {
                pipeline.stopAll();
                break;
              } else {
                try {
                  Thread.sleep( 0, 500 );
                } catch ( InterruptedException e ) {
                  // Ignore errors
                }
              }
            }
            pipeline.waitUntilFinished();

            if ( parentJob.isStopped() || pipeline.getErrors() != 0 ) {
              pipeline.stopAll();
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

            logError( BaseMessages.getString( PKG, "JobPipeline.Error.UnablePrepareExec" ), e );
            result.setNrErrors( 1 );
          }
        }
      } catch ( Exception e ) {

        logError( BaseMessages.getString( PKG, "JobPipeline.ErrorUnableOpenPipeline", e.getMessage() ) );
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
    Result newResult = pipeline.getResult();
    result.clear(); // clear only the numbers, NOT the files or rows.
    result.add( newResult );
    result.setRows( newResult.getRows() );
  }

  public PipelineMeta getPipelineMeta( IMetaStore metaStore, IVariables variables ) throws HopException {
    try {
      PipelineMeta pipelineMeta = null;
      CurrentDirectoryResolver r = new CurrentDirectoryResolver();
      IVariables tmpSpace = r.resolveCurrentDirectory( variables, parentJob, getFilename() );

      String realFilename = tmpSpace.environmentSubstitute( getFilename() );

      pipelineMeta = new PipelineMeta( realFilename, metaStore, true, this );

      if ( pipelineMeta != null ) {
        // set Internal.Entry.Current.Directory again because it was changed
        pipelineMeta.setInternalHopVariables();
        //  When the child parameter does exist in the parent parameters, overwrite the child parameter by the
        // parent parameter.

        TransformWithMappingMeta.replaceVariableValues( pipelineMeta, variables, "Pipeline" );
        if ( isPassingAllParameters() ) {
          // All other parent parameters need to get copied into the child parameters  (when the 'Inherit all
          // variables from the pipeline?' option is checked)
          TransformWithMappingMeta.addMissingVariables( pipelineMeta, variables );
        }
        // Pass the IMetaStore references
        //
        pipelineMeta.setMetaStore( metaStore );
      }

      return pipelineMeta;
    } catch ( final HopException ke ) {
      // if we get a HopException, simply re-throw it
      throw ke;
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "JobPipeline.Exception.MetaDataLoad" ), e );
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
  public List<SQLStatement> getSQLStatements( IMetaStore metaStore, IVariables variables ) throws HopException {
    this.copyVariablesFrom( variables );
    PipelineMeta pipelineMeta = getPipelineMeta( metaStore, this );

    return pipelineMeta.getSQLStatements();
  }

  @Override
  public void check( List<ICheckResult> remarks, JobMeta jobMeta, IVariables variables,
                     IMetaStore metaStore ) {
    if ( setLogfile ) {
      JobEntryValidatorUtils.andValidator().validate( this, "logfile", remarks,
        AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );
    }
    if ( !Utils.isEmpty( filename ) ) {
      JobEntryValidatorUtils.andValidator().validate( this, "filename", remarks,
        AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );
    } else {
      JobEntryValidatorUtils.andValidator().validate( this, "pipeline-name", remarks,
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
   * We're going to load the pipeline meta data referenced here. Then we're going to give it a new filename,
   * modify that filename in this entries. The parent caller will have made a copy of it, so it should be OK to do so.
   * <p/>
   * Exports the object to a flat-file system, adding content with filename keys to a set of definitions. The supplied
   * resource naming interface allows the object to name appropriately without worrying about those parts of the
   * implementation specific details.
   *
   * @param variables           The variable space to resolve (environment) variables with.
   * @param definitions     The map containing the filenames and content
   * @param namingInterface The resource naming interface allows the object to be named appropriately
   * @param metaStore       the metaStore to load external metadata from
   * @return The filename for this object. (also contained in the definitions map)
   * @throws HopException in case something goes wrong during the export
   */
  @Override
  public String exportResources( IVariables variables, Map<String, ResourceDefinition> definitions,
                                 IResourceNaming namingInterface, IMetaStore metaStore ) throws HopException {
    // Try to load the pipeline from a file.
    // Modify this recursively too...
    //
    // AGAIN: there is no need to clone this job entry because the caller is responsible for this.
    //
    // First load the pipeline metadata...
    //
    copyVariablesFrom( variables );
    PipelineMeta pipelineMeta = getPipelineMeta( metaStore, variables );

    // Also go down into the pipeline and export the files there. (mapping recursively down)
    //
    String proposedNewFilename =
      pipelineMeta.exportResources( pipelineMeta, definitions, namingInterface, metaStore );

    // To get a relative path to it, we inject ${Internal.Entry.Current.Directory}
    //
    String newFilename = "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY + "}/" + proposedNewFilename;

    // Set the correct filename inside the XML.
    //
    pipelineMeta.setFilename( newFilename );

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

  public Pipeline getPipeline() {
    return pipeline;
  }

  /**
   * @return The objects referenced in the transform, like a a pipeline, a job, a mapper, a reducer, a combiner, ...
   */
  @Override
  public String[] getReferencedObjectDescriptions() {
    return new String[] { BaseMessages.getString( PKG, "JobEntryPipeline.ReferencedObject.Description" ), };
  }

  private boolean isPipelineDefined() {
    return StringUtils.isNotEmpty( filename );
  }

  @Override
  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] { isPipelineDefined(), };
  }

  /**
   * Load the referenced object
   *
   * @param index     the referenced object index to load (in case there are multiple references)
   * @param metaStore metaStore
   * @param variables     the variable space to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  @Override
  public IHasFilename loadReferencedObject( int index, IMetaStore metaStore, IVariables variables ) throws HopException {
    return getPipelineMeta( metaStore, variables );
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
                                           INamedParams namedParam, JobEntryPipeline jobEntryPipeline )
    throws UnknownParamException {
    for ( int idx = 0; idx < parameters.length; idx++ ) {
      // Grab the parameter value set in the Pipeline job entry
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
              jobEntryPipeline.setVariable( parameters[ idx ], thisValue );
            }
          } else {
            // Or if not in parameterValues then we can add that variable with that value too
            jobEntryPipeline.setVariable( parameters[ idx ], thisValue );
          }
        }
      }
    }
  }

}
