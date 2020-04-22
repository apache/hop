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
package org.apache.hop.www;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannelFileWriter;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.util.FileUtil;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionCopy;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEngineFactory;

import java.util.Map;
import java.util.UUID;

public abstract class BaseWorkflowServlet extends BodyHttpServlet {

  private static final long serialVersionUID = 8523062215275251356L;

  protected IWorkflowEngine<WorkflowMeta> createJob( WorkflowConfiguration workflowConfiguration ) throws HopException {
    WorkflowExecutionConfiguration workflowExecutionConfiguration = workflowConfiguration.getWorkflowExecutionConfiguration();

    WorkflowMeta workflowMeta = workflowConfiguration.getWorkflowMeta();
    workflowMeta.setLogLevel( workflowExecutionConfiguration.getLogLevel() );
    workflowMeta.injectVariables( workflowExecutionConfiguration.getVariablesMap() );

    String carteObjectId = UUID.randomUUID().toString();

    SimpleLoggingObject servletLoggingObject =
      getServletLogging( carteObjectId, workflowExecutionConfiguration.getLogLevel() );

    // Create the workflow and store in the list...
    //
    String runConfigurationName = workflowExecutionConfiguration.getRunConfiguration();
    IMetaStore metaStore = HopServerSingleton.getInstance().getWorkflowMap().getSlaveServerConfig().getMetaStore();
    final IWorkflowEngine<WorkflowMeta> workflow = WorkflowEngineFactory.createWorkflowEngine( runConfigurationName, metaStore, workflowMeta );

    // Setting variables
    workflow.initializeVariablesFrom( null );
    workflow.getWorkflowMeta().setMetaStore( workflowMap.getSlaveServerConfig().getMetaStore() );
    workflow.getWorkflowMeta().setInternalHopVariables( workflow );
    workflow.injectVariables( workflowConfiguration.getWorkflowExecutionConfiguration().getVariablesMap() );

    copyJobParameters( workflow, workflowExecutionConfiguration.getParametersMap() );

    // Check if there is a starting point specified.
    String startCopyName = workflowExecutionConfiguration.getStartCopyName();
    if ( startCopyName != null && !startCopyName.isEmpty() ) {
      int startCopyNr = workflowExecutionConfiguration.getStartCopyNr();
      ActionCopy startActionCopy = workflowMeta.findAction( startCopyName, startCopyNr );
      workflow.setStartActionCopy( startActionCopy );
    }

    getWorkflowMap().addWorkflow( workflow.getWorkflowName(), carteObjectId, workflow, workflowConfiguration );

    return workflow;
  }

  protected IPipelineEngine<PipelineMeta> createPipeline( PipelineConfiguration pipelineConfiguration ) throws HopException {
    PipelineMeta pipelineMeta = pipelineConfiguration.getPipelineMeta();
    PipelineExecutionConfiguration pipelineExecutionConfiguration = pipelineConfiguration.getPipelineExecutionConfiguration();
    pipelineMeta.setLogLevel( pipelineExecutionConfiguration.getLogLevel() );
    pipelineMeta.injectVariables( pipelineExecutionConfiguration.getVariablesMap() );

    // Also copy the parameters over...
    copyParameters( pipelineMeta, pipelineExecutionConfiguration.getParametersMap() );

    String carteObjectId = UUID.randomUUID().toString();
    SimpleLoggingObject servletLoggingObject = getServletLogging( carteObjectId, pipelineExecutionConfiguration.getLogLevel() );

    // Get the metastore from the server
    //
    IMetaStore metaStore = getPipelineMap().getSlaveServerConfig().getMetaStore();

    // Create the pipeline and store in the list...
    //
    String runConfigurationName = pipelineConfiguration.getPipelineExecutionConfiguration().getRunConfiguration();
    if ( StringUtils.isEmpty(runConfigurationName)) {
      throw new HopException( "We need to know which pipeline run configuration to use to execute the pipeline");
    }
    PipelineRunConfiguration runConfiguration;
    try {
      runConfiguration = PipelineRunConfiguration.createFactory( metaStore ).loadElement( runConfigurationName );
    } catch(Exception e) {
      throw new HopException( "Error loading pipeline run configuration '"+runConfigurationName+"'", e );
    }
    if (runConfiguration==null) {
      throw new HopException( "Pipeline run configuration '"+runConfigurationName+"' could not be found" );
    }

    final IPipelineEngine<PipelineMeta> pipeline = PipelineEngineFactory.createPipelineEngine( runConfiguration, pipelineMeta );
    pipeline.setParent( servletLoggingObject );
    pipeline.setMetaStore( pipelineMap.getSlaveServerConfig().getMetaStore() );

    if ( pipelineExecutionConfiguration.isSetLogfile() ) {
      String realLogFilename = pipelineExecutionConfiguration.getLogFileName();
      try {
        FileUtil.createParentFolder( AddPipelineServlet.class, realLogFilename, pipelineExecutionConfiguration
          .isCreateParentFolder(), pipeline.getLogChannel(), pipeline );
        final LogChannelFileWriter logChannelFileWriter =
          new LogChannelFileWriter( servletLoggingObject.getLogChannelId(),
            HopVfs.getFileObject( realLogFilename ), pipelineExecutionConfiguration.isSetAppendLogfile() );
        logChannelFileWriter.startLogging();

        pipeline.addExecutionFinishedListener( pipelineEngine -> {
            if ( logChannelFileWriter != null ) {
              logChannelFileWriter.stopLogging();
            }
          });
      } catch ( HopException e ) {
        logError( Const.getStackTracker( e ) );
      }

    }

    pipeline.setContainerId( carteObjectId );
    getPipelineMap().addPipeline( pipelineMeta.getName(), carteObjectId, pipeline, pipelineConfiguration );

    return pipeline;
  }

  private void copyParameters( final AbstractMeta meta, final Map<String, String> params ) throws UnknownParamException {
    for ( String parameterName : params.keySet() ) {
      String thisValue = params.get( parameterName );
      if ( !StringUtils.isBlank( thisValue ) ) {
        meta.setParameterValue( parameterName, thisValue );
      }
    }
  }

  private void copyJobParameters( IWorkflowEngine<WorkflowMeta> workflow, Map<String, String> params ) throws UnknownParamException {
    WorkflowMeta workflowMeta = workflow.getWorkflowMeta();
    // Also copy the parameters over...
    workflow.copyParametersFrom( workflowMeta );
    workflow.clearParameters();
    String[] parameterNames = workflow.listParameters();
    for ( String parameterName : parameterNames ) {
      // Grab the parameter value set in the action
      String thisValue = params.get( parameterName );
      if ( !StringUtils.isBlank( thisValue ) ) {
        // Set the value as specified by the user in the action
        workflowMeta.setParameterValue( parameterName, thisValue );
      }
    }
    workflowMeta.activateParameters();
  }

  private SimpleLoggingObject getServletLogging( final String carteObjectId, final LogLevel level ) {
    SimpleLoggingObject servletLoggingObject =
      new SimpleLoggingObject( getContextPath(), LoggingObjectType.HOP_SERVER, null );
    servletLoggingObject.setContainerObjectId( carteObjectId );
    servletLoggingObject.setLogLevel( level );
    return servletLoggingObject;
  }

}
