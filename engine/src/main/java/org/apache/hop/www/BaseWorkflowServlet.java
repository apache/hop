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
import org.apache.hop.pipeline.IExecutionFinishedListener;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionCopy;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineConfiguration;

import java.util.Map;
import java.util.UUID;

public abstract class BaseWorkflowServlet extends BodyHttpServlet {

  private static final long serialVersionUID = 8523062215275251356L;

  protected Workflow createJob( WorkflowConfiguration workflowConfiguration ) throws UnknownParamException {
    WorkflowExecutionConfiguration workflowExecutionConfiguration = workflowConfiguration.getWorkflowExecutionConfiguration();

    WorkflowMeta workflowMeta = workflowConfiguration.getWorkflowMeta();
    workflowMeta.setLogLevel( workflowExecutionConfiguration.getLogLevel() );
    workflowMeta.injectVariables( workflowExecutionConfiguration.getVariablesMap() );

    String carteObjectId = UUID.randomUUID().toString();

    SimpleLoggingObject servletLoggingObject =
      getServletLogging( carteObjectId, workflowExecutionConfiguration.getLogLevel() );

    // Create the pipeline and store in the list...
    final Workflow workflow = new Workflow( workflowMeta, servletLoggingObject );
    // Setting variables
    workflow.initializeVariablesFrom( null );
    workflow.getWorkflowMeta().setMetaStore( workflowMap.getSlaveServerConfig().getMetaStore() );
    workflow.getWorkflowMeta().setInternalHopVariables( workflow );
    workflow.injectVariables( workflowConfiguration.getWorkflowExecutionConfiguration().getVariablesMap() );
    workflow.setSocketRepository( getSocketRepository() );

    copyJobParameters( workflow, workflowExecutionConfiguration.getParametersMap() );

    // Check if there is a starting point specified.
    String startCopyName = workflowExecutionConfiguration.getStartCopyName();
    if ( startCopyName != null && !startCopyName.isEmpty() ) {
      int startCopyNr = workflowExecutionConfiguration.getStartCopyNr();
      ActionCopy startActionCopy = workflowMeta.findAction( startCopyName, startCopyNr );
      workflow.setStartActionCopy( startActionCopy );
    }

    // Do we need to expand the workflow when it's running?
    // Note: the plugin (Workflow and Pipeline) actions need to call the delegation listeners in the parent workflow.
    if ( workflowExecutionConfiguration.isExpandingRemoteJob() ) {
      workflow.addDelegationListener( new HopServerDelegationHandler( getPipelineMap(), getWorkflowMap() ) );
    }

    getWorkflowMap().addWorkflow( workflow.getJobname(), carteObjectId, workflow, workflowConfiguration );

    final Long passedBatchId = workflowExecutionConfiguration.getPassedBatchId();
    if ( passedBatchId != null ) {
      workflow.setPassedBatchId( passedBatchId );
    }

    return workflow;
  }

  protected Pipeline createPipeline( PipelineConfiguration pipelineConfiguration ) throws UnknownParamException {
    PipelineMeta pipelineMeta = pipelineConfiguration.getPipelineMeta();
    PipelineExecutionConfiguration pipelineExecutionConfiguration = pipelineConfiguration.getPipelineExecutionConfiguration();
    pipelineMeta.setLogLevel( pipelineExecutionConfiguration.getLogLevel() );
    pipelineMeta.injectVariables( pipelineExecutionConfiguration.getVariablesMap() );

    // Also copy the parameters over...
    copyParameters( pipelineMeta, pipelineExecutionConfiguration.getParametersMap() );

    String carteObjectId = UUID.randomUUID().toString();
    SimpleLoggingObject servletLoggingObject =
      getServletLogging( carteObjectId, pipelineExecutionConfiguration.getLogLevel() );

    // Create the pipeline and store in the list...
    final Pipeline pipeline = new Pipeline( pipelineMeta, servletLoggingObject );
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

        pipeline.addExecutionFinishedListener((IExecutionFinishedListener<PipelineMeta>) pipelineEngine -> {
            if ( logChannelFileWriter != null ) {
              logChannelFileWriter.stopLogging();
            }
          });
      } catch ( HopException e ) {
        logError( Const.getStackTracker( e ) );
      }

    }

    pipeline.setSocketRepository( getSocketRepository() );

    pipeline.setContainerObjectId( carteObjectId );
    getPipelineMap().addPipeline( pipelineMeta.getName(), carteObjectId, pipeline, pipelineConfiguration );

    final Long passedBatchId = pipelineExecutionConfiguration.getPassedBatchId();
    if ( passedBatchId != null ) {
      pipeline.setPassedBatchId( passedBatchId );
    }

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

  private void copyJobParameters( Workflow workflow, Map<String, String> params ) throws UnknownParamException {
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
      new SimpleLoggingObject( getContextPath(), LoggingObjectType.CARTE, null );
    servletLoggingObject.setContainerObjectId( carteObjectId );
    servletLoggingObject.setLogLevel( level );
    return servletLoggingObject;
  }

}
