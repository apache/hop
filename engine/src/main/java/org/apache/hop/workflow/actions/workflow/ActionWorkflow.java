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

package org.apache.hop.workflow.actions.workflow;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.listeners.ICurrentDirectoryChangedListener;
import org.apache.hop.core.listeners.impl.EntryCurrentDirectoryChangedListener;
import org.apache.hop.core.logging.LogChannelFileWriter;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.parameters.DuplicateParamException;
import org.apache.hop.core.parameters.INamedParams;
import org.apache.hop.core.parameters.NamedParamsDefault;
import org.apache.hop.core.util.CurrentDirectoryResolver;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.IDelegationListener;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionRunConfigurable;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.www.SlaveServerWorkflowStatus;
import org.w3c.dom.Node;

import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Recursive definition of a Workflow. This transform means that an entire Workflow has to be executed. It can be the same Workflow, but
 * just make sure that you don't get an endless loop. Provide an escape routine using JobEval.
 *
 * @author Matt
 * @since 01-10-2003, Rewritten on 18-06-2004
 */
public class ActionWorkflow extends ActionBase implements Cloneable, IAction, IActionRunConfigurable {
  private static Class<?> PKG = ActionWorkflow.class; // for i18n purposes, needed by Translator!!
  public static final int IS_PENTAHO = 1;

  private String filename;
  private String workflowName;
  private String directory;

  public boolean paramsFromPrevious;
  public boolean execPerRow;

  public String[] parameters;
  public String[] parameterFieldNames;
  public String[] parameterValues;

  public boolean setLogfile;
  public String logfile, logext;
  public boolean addDate, addTime;
  public LogLevel logFileLevel;

  public boolean parallel;
  public boolean setAppendLogfile;
  public boolean createParentFolder;

  public boolean waitingToFinish = true;
  public boolean followingAbortRemotely;

  public boolean expandingRemoteJob;

  private String remoteSlaveServerName;
  public boolean passingAllParameters = true;

  private boolean passingExport;

  private String runConfiguration;

  public static final LogLevel DEFAULT_LOG_LEVEL = LogLevel.NOTHING;

  private Workflow workflow;

  private ICurrentDirectoryChangedListener dirListener = new EntryCurrentDirectoryChangedListener(
    this::getDirectory,
    this::setDirectory );

  public ActionWorkflow( String name ) {
    super( name, "" );
  }

  public ActionWorkflow() {
    this( "" );
    clear();
  }

  private void allocateArgs( int nrArgs ) {
  }

  private void allocateParams( int nrParameters ) {
    parameters = new String[ nrParameters ];
    parameterFieldNames = new String[ nrParameters ];
    parameterValues = new String[ nrParameters ];
  }

  @Override
  public Object clone() {
    ActionWorkflow je = (ActionWorkflow) super.clone();
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
   * @deprecated use getFilename() instead.
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

  public void setWorkflowName( String workflowName ) {
    this.workflowName = workflowName;
  }

  public String getWorkflowName() {
    return workflowName;
  }

  public String getDirectory() {
    return directory;
  }

  public void setDirectory( String directory ) {
    this.directory = directory;
  }

  public void setDirectories( String[] directories ) {
    this.directory = directories[ 0 ];
  }

  public boolean isPassingExport() {
    return passingExport;
  }

  public void setPassingExport( boolean passingExport ) {
    this.passingExport = passingExport;
  }

  public String getRunConfiguration() {
    return runConfiguration;
  }

  public void setRunConfiguration( String runConfiguration ) {
    this.runConfiguration = runConfiguration;
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
  public String getXml() {
    StringBuilder retval = new StringBuilder( 400 );

    retval.append( super.getXml() );

    retval.append( "      " ).append( XmlHandler.addTagValue( "filename", filename ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "workflowname", workflowName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "directory", directory ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "params_from_previous", paramsFromPrevious ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "exec_per_row", execPerRow ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "set_logfile", setLogfile ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "logfile", logfile ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "logext", logext ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_date", addDate ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_time", addTime ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "loglevel", logFileLevel != null ? logFileLevel.getCode() : DEFAULT_LOG_LEVEL
        .getCode() ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "slave_server_name", remoteSlaveServerName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "wait_until_finished", waitingToFinish ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "follow_abort_remote", followingAbortRemotely ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "expand_remote_job", expandingRemoteJob ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "create_parent_folder", createParentFolder ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "pass_export", passingExport ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "run_configuration", runConfiguration ) );

    if ( parameters != null ) {
      retval.append( "      " ).append( XmlHandler.openTag( "parameters" ) );

      retval.append( "        " ).append( XmlHandler.addTagValue( "pass_all_parameters", passingAllParameters ) );

      for ( int i = 0; i < parameters.length; i++ ) {
        // This is a better way of making the XML file than the arguments.
        retval.append( "            " ).append( XmlHandler.openTag( "parameter" ) );

        retval.append( "            " ).append( XmlHandler.addTagValue( "name", parameters[ i ] ) );
        retval.append( "            " ).append( XmlHandler.addTagValue( "stream_name", parameterFieldNames[ i ] ) );
        retval.append( "            " ).append( XmlHandler.addTagValue( "value", parameterValues[ i ] ) );

        retval.append( "            " ).append( XmlHandler.closeTag( "parameter" ) );
      }
      retval.append( "      " ).append( XmlHandler.closeTag( "parameters" ) );
    }
    retval.append( "      " ).append( XmlHandler.addTagValue( "set_append_logfile", setAppendLogfile ) );

    return retval.toString();
  }

  @Override
  public void loadXml( Node entrynode,
                       IMetaStore metaStore ) throws HopXmlException {
    try {
      super.loadXml( entrynode );

      filename = XmlHandler.getTagValue( entrynode, "filename" );
      workflowName = XmlHandler.getTagValue( entrynode, "workflowname" );
      directory = XmlHandler.getTagValue( entrynode, "directory" );

      paramsFromPrevious = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "params_from_previous" ) );
      execPerRow = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "exec_per_row" ) );
      setLogfile = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "set_logfile" ) );
      addDate = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_date" ) );
      addTime = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_time" ) );
      logfile = XmlHandler.getTagValue( entrynode, "logfile" );
      logext = XmlHandler.getTagValue( entrynode, "logext" );
      logFileLevel = LogLevel.getLogLevelForCode( XmlHandler.getTagValue( entrynode, "loglevel" ) );
      setAppendLogfile = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "set_append_logfile" ) );
      remoteSlaveServerName = XmlHandler.getTagValue( entrynode, "slave_server_name" );
      passingExport = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "pass_export" ) );
      createParentFolder = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "create_parent_folder" ) );
      runConfiguration = XmlHandler.getTagValue( entrynode, "run_configuration" );

      String wait = XmlHandler.getTagValue( entrynode, "wait_until_finished" );
      if ( Utils.isEmpty( wait ) ) {
        waitingToFinish = true;
      } else {
        waitingToFinish = "Y".equalsIgnoreCase( wait );
      }

      followingAbortRemotely = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "follow_abort_remote" ) );
      expandingRemoteJob = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "expand_remote_job" ) );

      // How many arguments?
      int argnr = 0;
      while ( XmlHandler.getTagValue( entrynode, "argument" + argnr ) != null ) {
        argnr++;
      }
      allocateArgs( argnr );

      Node parametersNode = XmlHandler.getSubNode( entrynode, "parameters" );

      String passAll = XmlHandler.getTagValue( parametersNode, "pass_all_parameters" );
      passingAllParameters = Utils.isEmpty( passAll ) || "Y".equalsIgnoreCase( passAll );

      int nrParameters = XmlHandler.countNodes( parametersNode, "parameter" );
      allocateParams( nrParameters );

      for ( int i = 0; i < nrParameters; i++ ) {
        Node knode = XmlHandler.getSubNodeByNr( parametersNode, "parameter", i );

        parameters[ i ] = XmlHandler.getTagValue( knode, "name" );
        parameterFieldNames[ i ] = XmlHandler.getTagValue( knode, "stream_name" );
        parameterValues[ i ] = XmlHandler.getTagValue( knode, "value" );
      }
    } catch ( HopXmlException xe ) {
      throw new HopXmlException( "Unable to load 'workflow' action from XML node", xe );
    }
  }

  @Override
  public Result execute( Result result, int nr ) throws HopException {
    result.setEntryNr( nr );

    LogChannelFileWriter logChannelFileWriter = null;
    LogLevel jobLogLevel = parentWorkflow.getLogLevel();

    if ( setLogfile ) {
      String realLogFilename = environmentSubstitute( getLogFilename() );
      // We need to check here the log filename
      // if we do not have one, we must fail
      if ( Utils.isEmpty( realLogFilename ) ) {
        logError( BaseMessages.getString( PKG, "JobJob.Exception.LogFilenameMissing" ) );
        result.setNrErrors( 1 );
        result.setResult( false );
        return result;
      }

      // create parent folder?
      if ( !createParentFolder( realLogFilename ) ) {
        result.setNrErrors( 1 );
        result.setResult( false );
        return result;
      }
      try {
        logChannelFileWriter =
          new LogChannelFileWriter(
            this.getLogChannelId(), HopVfs.getFileObject( realLogFilename ), setAppendLogfile );
        logChannelFileWriter.startLogging();
      } catch ( HopException e ) {
        logError( "Unable to open file appender for file [" + getLogFilename() + "] : " + e.toString() );
        logError( Const.getStackTracker( e ) );
        result.setNrErrors( 1 );
        result.setResult( false );
        return result;
      }
      jobLogLevel = logFileLevel;
    }

    try {
      // First load the workflow, outside of the loop...
      if ( parentWorkflow.getWorkflowMeta() != null ) {
        // reset the internal variables again.
        // Maybe we should split up the variables even more like in UNIX shells.
        // The internal variables need to be reset to be able use them properly
        // in 2 sequential sub workflows.
        parentWorkflow.getWorkflowMeta().setInternalHopVariables();
      }

      // Explain what we are loading...
      //
      logDetailed( "Loading workflow from XML file : [" + environmentSubstitute( filename ) + "]" );

      WorkflowMeta workflowMeta = getWorkflowMeta( metaStore, this );

      // Verify that we loaded something, complain if we did not...
      //
      if ( workflowMeta == null ) {
        throw new HopException( "Unable to load the workflow: please specify a filename" );
      }

      verifyRecursiveExecution( parentWorkflow, workflowMeta );

      int iteration = 0;

      copyVariablesFrom( parentWorkflow );
      setParentVariableSpace( parentWorkflow );

      RowMetaAndData resultRow = null;
      boolean first = true;
      List<RowMetaAndData> rows = new ArrayList<RowMetaAndData>( result.getRows() );

      while ( ( first && !execPerRow )
        || ( execPerRow && rows != null && iteration < rows.size() && result.getNrErrors() == 0 ) ) {
        first = false;

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

        // First (optionally) copy all the parameter values from the parent workflow
        //
        if ( paramsFromPrevious ) {
          String[] parentParameters = parentWorkflow.listParameters();
          for ( int idx = 0; idx < parentParameters.length; idx++ ) {
            String par = parentParameters[ idx ];
            String def = parentWorkflow.getParameterDefault( par );
            String val = parentWorkflow.getParameterValue( par );
            String des = parentWorkflow.getParameterDescription( par );

            namedParam.addParameterDefinition( par, def, des );
            namedParam.setParameterValue( par, val );
          }
        }

        // Now add those parameter values specified by the user in the action
        //
        if ( parameters != null ) {
          for ( int idx = 0; idx < parameters.length; idx++ ) {
            if ( !Utils.isEmpty( parameters[ idx ] ) ) {

              // If it's not yet present in the parent workflow, add it...
              //
              if ( Const.indexOfString( parameters[ idx ], namedParam.listParameters() ) < 0 ) {
                // We have a parameter
                try {
                  namedParam.addParameterDefinition( parameters[ idx ], "", "Action runtime" );
                } catch ( DuplicateParamException e ) {
                  // Should never happen
                  //
                  logError( "Duplicate parameter definition for " + parameters[ idx ] );
                }
              }

              if ( Utils.isEmpty( Const.trim( parameterFieldNames[ idx ] ) ) ) {
                namedParam.setParameterValue( parameters[ idx ], Const.NVL(
                  environmentSubstitute( parameterValues[ idx ] ), "" ) );
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

        Result oneResult = new Result();

        List<RowMetaAndData> sourceRows = null;

        if ( execPerRow ) {
          // Execute for each input row
          // Just pass a single row
          //
          List<RowMetaAndData> newList = new ArrayList<RowMetaAndData>();
          newList.add( resultRow );
          sourceRows = newList;

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

          // Keep it as it was...
          //
          sourceRows = result.getRows();

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
        }

        boolean doFallback = true;
        SlaveServer remoteSlaveServer = null;
        WorkflowExecutionConfiguration executionConfiguration = new WorkflowExecutionConfiguration();
        if ( !Utils.isEmpty( runConfiguration ) ) {
          runConfiguration = environmentSubstitute( runConfiguration );
          log.logBasic( BaseMessages.getString( PKG, "JobJob.RunConfig.Message" ), runConfiguration );
          executionConfiguration.setRunConfiguration( runConfiguration );
          try {
            ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiPipelineBeforeStart.id, new Object[] {
              executionConfiguration, parentWorkflow.getWorkflowMeta(), workflowMeta
            } );
            List<Object> items = Arrays.asList( runConfiguration, false );
            try {
              ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint
                .RunConfigurationSelection.id, items );
              if ( waitingToFinish && (Boolean) items.get( IS_PENTAHO ) ) {
                String workflowName = parentWorkflow.getWorkflowMeta().getName();
                String name = workflowMeta.getName();
                logBasic( BaseMessages.getString( PKG, "JobJob.Log.InvalidRunConfigurationCombination", workflowName,
                  name, workflowName ) );
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
            remoteSlaveServer = parentWorkflow.getWorkflowMeta().findSlaveServer( realRemoteSlaveServerName );
            if ( remoteSlaveServer == null ) {
              throw new HopException( BaseMessages.getString(
                PKG, "JobPipeline.Exception.UnableToFindRemoteSlaveServer", realRemoteSlaveServerName ) );
            }
          }
        }

        if ( remoteSlaveServer == null ) {
          // Local execution...
          //

          // Create a new workflow
          //
          workflow = new Workflow( workflowMeta, this );
          workflow.setParentWorkflow( parentWorkflow );
          workflow.setLogLevel( jobLogLevel );
          workflow.shareVariablesWith( this );
          workflow.setInternalHopVariables( this );
          workflow.copyParametersFrom( workflowMeta );
          workflow.setInteractive( parentWorkflow.isInteractive() );
          if ( workflow.isInteractive() ) {
            workflow.getJobEntryListeners().addAll( parentWorkflow.getJobEntryListeners() );
          }
          
          // Set the parameters calculated above on this instance.
          //
          workflow.clearParameters();
          String[] parameterNames = workflow.listParameters();
          for ( int idx = 0; idx < parameterNames.length; idx++ ) {
            // Grab the parameter value set in the action
            //
            String thisValue = namedParam.getParameterValue( parameterNames[ idx ] );
            if ( !Utils.isEmpty( thisValue ) ) {
              // Set the value as specified by the user in the action
              //
              workflow.setParameterValue( parameterNames[ idx ], thisValue );
            } else {
              // See if the parameter had a value set in the parent workflow...
              // This value should pass down to the sub-workflow if that's what we
              // opted to do.
              //
              if ( isPassingAllParameters() ) {
                String parentValue = parentWorkflow.getParameterValue( parameterNames[ idx ] );
                if ( !Utils.isEmpty( parentValue ) ) {
                  workflow.setParameterValue( parameterNames[ idx ], parentValue );
                }
              }
            }
          }
          workflow.activateParameters();

          // Set the source rows we calculated above...
          //
          workflow.setSourceRows( sourceRows );

          // Don't forget the logging...
          workflow.beginProcessing();

          // Link the workflow with the sub-workflow
          parentWorkflow.getWorkflowTracker().addWorkflowTracker( workflow.getWorkflowTracker() );

          // Link both ways!
          workflow.getWorkflowTracker().setParentWorkflowTracker( parentWorkflow.getWorkflowTracker() );


          // Inform the parent workflow we started something here...
          //
          for ( IDelegationListener delegationListener : parentWorkflow.getDelegationListeners() ) {
            // TODO: copy some settings in the workflow execution configuration, not strictly needed
            // but the execution configuration information is useful in case of a workflow re-start
            //
            delegationListener.jobDelegationStarted( workflow, new WorkflowExecutionConfiguration() );
          }

          ActionWorkflowRunner runner = new ActionWorkflowRunner( workflow, result, nr, log );
          Thread jobRunnerThread = new Thread( runner );
          // PDI-6518
          // added UUID to thread name, otherwise threads do share names if workflows entries are executed in parallel in a
          // parent workflow
          // if that happens, contained pipelines start closing each other's connections
          jobRunnerThread.setName( Const.NVL( workflow.getWorkflowMeta().getName(), workflow.getWorkflowMeta().getFilename() )
            + " UUID: " + UUID.randomUUID().toString() );
          jobRunnerThread.start();

          // Keep running until we're done.
          //
          while ( !runner.isFinished() && !parentWorkflow.isStopped() ) {
            try {
              Thread.sleep( 0, 1 );
            } catch ( InterruptedException e ) {
              // Ignore
            }
          }

          // if the parent-workflow was stopped, stop the sub-workflow too...
          if ( parentWorkflow.isStopped() ) {
            workflow.stopAll();
            runner.waitUntilFinished(); // Wait until finished!
          }

          oneResult = runner.getResult();

        } else {

          // Make sure we can parameterize the slave server connection
          //
          remoteSlaveServer.shareVariablesWith( this );

          // Remote execution...
          //
          WorkflowExecutionConfiguration workflowExecutionConfiguration = new WorkflowExecutionConfiguration();
          workflowExecutionConfiguration.setPreviousResult( result.lightClone() ); // lightClone() because rows are
          // overwritten in next line.
          workflowExecutionConfiguration.getPreviousResult().setRows( sourceRows );
          workflowExecutionConfiguration.setVariablesMap( this );
          workflowExecutionConfiguration.setRemoteServer( remoteSlaveServer );
          workflowExecutionConfiguration.setLogLevel( jobLogLevel );
          workflowExecutionConfiguration.setPassingExport( passingExport );
          for ( String param : namedParam.listParameters() ) {
            String defValue = namedParam.getParameterDefault( param );
            String value = namedParam.getParameterValue( param );
            workflowExecutionConfiguration.getParametersMap().put( param, Const.NVL( value, defValue ) );
          }

          // Send the XML over to the slave server
          // Also start the workflow over there...
          //
          String carteObjectId = null;
          try {
            carteObjectId = Workflow.sendToSlaveServer( workflowMeta, workflowExecutionConfiguration, metaStore );
          } catch ( HopException e ) {
            // Perhaps the workflow exists on the remote server, carte is down, etc.
            // This is an abort situation, stop the parent workflow...
            // We want this in case we are running in parallel. The other workflow
            // entries can stop running now.
            //
            parentWorkflow.stopAll();

            // Pass the exception along
            //
            throw e;
          }

          // Now start the monitoring...
          //
          SlaveServerWorkflowStatus jobStatus = null;
          while ( !parentWorkflow.isStopped() && waitingToFinish ) {
            try {
              jobStatus = remoteSlaveServer.getWorkflowStatus( workflowMeta.getName(), carteObjectId, 0 );
              if ( jobStatus.getResult() != null ) {
                // The workflow is finished, get the result...
                //
                oneResult = jobStatus.getResult();
                break;
              }
            } catch ( Exception e1 ) {
              logError( "Unable to contact slave server ["
                + remoteSlaveServer + "] to verify the status of workflow [" + workflowMeta.getName() + "]", e1 );
              oneResult.setNrErrors( 1L );
              break; // Stop looking too, chances are too low the server will
              // come back on-line
            }

            // sleep for 1 second
            try {
              Thread.sleep( 1000 );
            } catch ( InterruptedException e ) {
              // Ignore
            }
          }

          // PDI-14781
          // Write log from carte to file
          if ( setLogfile && jobStatus != null ) {
            String logFromCarte = jobStatus.getLoggingString();
            if ( !Utils.isEmpty( logFromCarte ) ) {
              FileObject logfile = logChannelFileWriter.getLogFile();
              OutputStream logFileOutputStream = null;
              try {
                logFileOutputStream = HopVfs.getOutputStream( logfile, setAppendLogfile );
                logFileOutputStream.write( logFromCarte.getBytes() );
                logFileOutputStream.flush();
              } catch ( Exception e ) {
                logError( "There was an error logging to file '" + logfile + "'", e );
              } finally {
                try {
                  if ( logFileOutputStream != null ) {
                    logFileOutputStream.close();
                    logFileOutputStream = null;
                  }
                } catch ( Exception e ) {
                  logError( "There was an error closing log file file '" + logfile + "'", e );
                }
              }
            }
          }

          if ( !waitingToFinish ) {
            // Since the workflow was posted successfully, the result is true...
            //
            oneResult = new Result();
            oneResult.setResult( true );
          }

          if ( parentWorkflow.isStopped() ) {
            try {
              // See if we have a status and if we need to stop the remote
              // execution here...
              //
              if ( jobStatus == null || jobStatus.isRunning() ) {
                // Try a remote abort ...
                //
                remoteSlaveServer.stopWorkflow( workflowMeta.getName(), carteObjectId );
              }
            } catch ( Exception e1 ) {
              logError( "Unable to contact slave server ["
                + remoteSlaveServer + "] to stop workflow [" + workflowMeta.getName() + "]", e1 );
              oneResult.setNrErrors( 1L );
              break; // Stop looking too, chances are too low the server will
              // come back on-line
            }
          }

        }

        result.clear(); // clear only the numbers, NOT the files or rows.
        result.add( oneResult );

        // Set the result rows too, if any ...
        if ( !Utils.isEmpty( oneResult.getRows() ) ) {
          result.setRows( new ArrayList<RowMetaAndData>( oneResult.getRows() ) );
        }

        // if one of them fails (in the loop), increase the number of errors
        //
        if ( oneResult.getResult() == false ) {
          result.setNrErrors( result.getNrErrors() + 1 );
        }

        iteration++;
      }

    } catch ( HopException ke ) {
      logError( "Error running action 'workflow' : ", ke );

      result.setResult( false );
      result.setNrErrors( 1L );
    }

    if ( setLogfile ) {
      if ( logChannelFileWriter != null ) {
        logChannelFileWriter.stopLogging();

        ResultFile resultFile =
          new ResultFile(
            ResultFile.FILE_TYPE_LOG, logChannelFileWriter.getLogFile(), parentWorkflow.getJobname(), getName() );
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

    if ( result.getNrErrors() > 0 ) {
      result.setResult( false );
    } else {
      result.setResult( true );
    }

    return result;
  }

  private boolean createParentFolder( String filename ) {
    // Check for parent folder
    FileObject parentfolder = null;
    boolean resultat = true;
    try {
      // Get parent folder
      parentfolder = HopVfs.getFileObject( filename, this ).getParent();
      if ( !parentfolder.exists() ) {
        if ( createParentFolder ) {
          if ( log.isDebug() ) {
            log.logDebug( BaseMessages.getString( PKG, "JobJob.Log.ParentLogFolderNotExist", parentfolder
              .getName().toString() ) );
          }
          parentfolder.createFolder();
          if ( log.isDebug() ) {
            log.logDebug( BaseMessages.getString( PKG, "JobJob.Log.ParentLogFolderCreated", parentfolder
              .getName().toString() ) );
          }
        } else {
          log.logError( BaseMessages.getString( PKG, "JobJob.Log.ParentLogFolderNotExist", parentfolder
            .getName().toString() ) );
          resultat = false;
        }
      } else {
        if ( log.isDebug() ) {
          log.logDebug( BaseMessages.getString( PKG, "JobJob.Log.ParentLogFolderExists", parentfolder
            .getName().toString() ) );
        }
      }
    } catch ( Exception e ) {
      resultat = false;
      log.logError( BaseMessages.getString( PKG, "JobJob.Error.ChekingParentLogFolderTitle" ), BaseMessages
        .getString( PKG, "JobJob.Error.ChekingParentLogFolder", parentfolder.getName().toString() ), e );
    } finally {
      if ( parentfolder != null ) {
        try {
          parentfolder.close();
          parentfolder = null;
        } catch ( Exception ex ) {
          // Ignore
        }
      }
    }

    return resultat;
  }

  /**
   * Make sure that we are not loading workflows recursively...
   *
   * @param parentWorkflow the parent workflow
   * @param workflowMeta   the workflow metadata
   * @throws HopException in case both workflows are loaded from the same source
   */
  private void verifyRecursiveExecution( Workflow parentWorkflow, WorkflowMeta workflowMeta ) throws HopException {

    if ( parentWorkflow == null ) {
      return; // OK!
    }

    WorkflowMeta parentWorkflowMeta = parentWorkflow.getWorkflowMeta();

    if ( parentWorkflowMeta.getName() == null && workflowMeta.getName() != null ) {
      return; // OK
    }
    if ( parentWorkflowMeta.getName() != null && workflowMeta.getName() == null ) {
      return; // OK as well.
    }

    // Verify the filename for recursive execution
    //
    if ( workflowMeta.getFilename() != null && workflowMeta.getFilename().equals( parentWorkflowMeta.getFilename() ) ) {
      throw new HopException( BaseMessages.getString( PKG, "JobJobError.Recursive", workflowMeta.getFilename() ) );
    }

    // Also compare with the grand-parent (if there is any)
    verifyRecursiveExecution( parentWorkflow.getParentWorkflow(), workflowMeta );
  }

  @Override
  public void clear() {
    super.clear();

    workflowName = null;
    filename = null;
    directory = null;
    addDate = false;
    addTime = false;
    logfile = null;
    logext = null;
    setLogfile = false;
    setAppendLogfile = false;
    runConfiguration = null;
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
  public List<SqlStatement> getSqlStatements( IMetaStore metaStore, IVariables variables ) throws HopException {
    this.copyVariablesFrom( variables );
    WorkflowMeta workflowMeta = getWorkflowMeta( metaStore, variables );
    return workflowMeta.getSqlStatements( null );
  }

  public WorkflowMeta getWorkflowMeta( IMetaStore metaStore, IVariables variables ) throws HopException {
    WorkflowMeta workflowMeta = null;
    try {
      CurrentDirectoryResolver r = new CurrentDirectoryResolver();
      IVariables tmpSpace = r.resolveCurrentDirectory( variables, parentWorkflow, getFilename() );

      String realFilename = tmpSpace.environmentSubstitute( getFilename() );
      workflowMeta = new WorkflowMeta( tmpSpace, realFilename, metaStore );
      if ( workflowMeta != null ) {
        workflowMeta.setMetaStore( metaStore );
      }
      return workflowMeta;
    } catch ( Exception e ) {
      throw new HopException( "Unexpected error during workflow metadata load", e );
    }

  }

  /**
   * @return Returns the runEveryResultRow.
   */
  public boolean isExecPerRow() {
    return execPerRow;
  }

  /**
   * @param runEveryResultRow The runEveryResultRow to set.
   */
  public void setExecPerRow( boolean runEveryResultRow ) {
    this.execPerRow = runEveryResultRow;
  }

  @Override
  public List<ResourceReference> getResourceDependencies( WorkflowMeta workflowMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( workflowMeta );
    if ( !Utils.isEmpty( filename ) ) {
      String realFileName = workflowMeta.environmentSubstitute( filename );
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( realFileName, ResourceType.ACTIONFILE ) );
      references.add( reference );
    }
    return references;
  }

  /**
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
    // Try to load the pipeline from file.
    // Modify this recursively too...
    //
    // AGAIN: there is no need to clone this action because the caller is
    // responsible for this.
    //
    // First load the workflow meta data...
    //
    copyVariablesFrom( variables ); // To make sure variables are available.
    WorkflowMeta workflowMeta = getWorkflowMeta( metaStore, variables );

    // Also go down into the workflow and export the files there. (going down
    // recursively)
    //
    String proposedNewFilename =
      workflowMeta.exportResources( workflowMeta, definitions, namingInterface, metaStore );

    // To get a relative path to it, we inject
    // ${Internal.Entry.Current.Directory}
    //
    String newFilename = "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY + "}/" + proposedNewFilename;

    // Set the filename in the workflow
    //
    workflowMeta.setFilename( newFilename );

    // change it in the action
    //
    filename = newFilename;

    return proposedNewFilename;
  }

  @Override
  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IMetaStore metaStore ) {
    if ( setLogfile ) {
      ActionValidatorUtils.andValidator().validate( this, "logfile", remarks,
        AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
    }

    if ( null != directory ) {
      // if from repo
      ActionValidatorUtils.andValidator().validate( this, "directory", remarks,
        AndValidator.putValidators( ActionValidatorUtils.notNullValidator() ) );
      ActionValidatorUtils.andValidator().validate( this, "workflowName", remarks,
        AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
    } else {
      // else from xml file
      ActionValidatorUtils.andValidator().validate( this, "filename", remarks,
        AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
    }
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
   * @param remoteSlaveServerName the remoteSlaveServer to set
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

  public void setLoggingRemoteWork( boolean loggingRemoteWork ) {
    // do nothing. for compatibility with IActionRunConfigurable
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

  public Workflow getWorkflow() {
    return workflow;
  }


  private boolean isJobDefined() {
    return !Utils.isEmpty( filename ) || ( !Utils.isEmpty( this.directory ) && !Utils.isEmpty( workflowName ) );
  }

  @Override
  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] { isJobDefined(), };
  }

  /**
   * @return The objects referenced in the transform, like a a pipeline, a workflow, a mapper, a reducer, a combiner, ...
   */
  @Override
  public String[] getReferencedObjectDescriptions() {
    return new String[] { BaseMessages.getString( PKG, "ActionJob.ReferencedObject.Description" ), };
  }

  /**
   * Load the referenced object
   *
   * @param index     the referenced object index to load (in case there are multiple references)
   * @param metaStore the metaStore
   * @param variables     the variable space to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  @Override
  public IHasFilename loadReferencedObject( int index, IMetaStore metaStore, IVariables variables ) throws HopException {
    return getWorkflowMeta( metaStore, variables );
  }

  public boolean isExpandingRemoteJob() {
    return expandingRemoteJob;
  }

  public void setExpandingRemoteJob( boolean expandingRemoteJob ) {
    this.expandingRemoteJob = expandingRemoteJob;
  }

  @Override
  public void setParentWorkflowMeta( WorkflowMeta parentWorkflowMeta ) {
    WorkflowMeta previous = getParentWorkflowMeta();
    super.setParentWorkflowMeta( parentWorkflowMeta );
    if ( previous != null ) {
      previous.removeCurrentDirectoryChangedListener( this.dirListener );
    }
    if ( parentWorkflowMeta != null ) {
      parentWorkflowMeta.addCurrentDirectoryChangedListener( this.dirListener );
    }
  }

}
