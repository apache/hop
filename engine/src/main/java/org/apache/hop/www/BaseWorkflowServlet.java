/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEngineFactory;
import org.json.simple.parser.ParseException;

import java.util.Map;
import java.util.UUID;

public abstract class BaseWorkflowServlet extends BodyHttpServlet {

  private static final long serialVersionUID = 8523062215275251356L;

  protected IWorkflowEngine<WorkflowMeta> createWorkflow( WorkflowConfiguration workflowConfiguration ) throws HopException, HopException, ParseException {
    WorkflowExecutionConfiguration workflowExecutionConfiguration = workflowConfiguration.getWorkflowExecutionConfiguration();

    IHopMetadataProvider metadataProvider = workflowConfiguration.getMetadataProvider();

    WorkflowMeta workflowMeta = workflowConfiguration.getWorkflowMeta();
    workflowMeta.setLogLevel( workflowExecutionConfiguration.getLogLevel() );
    workflowMeta.injectVariables( workflowExecutionConfiguration.getVariablesMap() );

    String serverObjectId = UUID.randomUUID().toString();

    SimpleLoggingObject servletLoggingObject = getServletLogging( serverObjectId, workflowExecutionConfiguration.getLogLevel() );

    // Create the workflow and store in the list...
    //
    String runConfigurationName = workflowExecutionConfiguration.getRunConfiguration();
    final IWorkflowEngine<WorkflowMeta> workflow = WorkflowEngineFactory.createWorkflowEngine( runConfigurationName, metadataProvider, workflowMeta );

    // Setting variables
    workflow.initializeVariablesFrom( null );
    workflow.getWorkflowMeta().setMetadataProvider( metadataProvider );
    workflow.getWorkflowMeta().setInternalHopVariables( workflow );
    workflow.injectVariables( workflowConfiguration.getWorkflowExecutionConfiguration().getVariablesMap() );

    copyWorkflowParameters( workflow, workflowExecutionConfiguration.getParametersMap() );

    // Check if there is a starting point specified.
    String startActionName = workflowExecutionConfiguration.getStartActionName();
    if ( startActionName != null && !startActionName.isEmpty() ) {
      int startCopyNr = workflowExecutionConfiguration.getStartActionNr();
      ActionMeta startActionMeta = workflowMeta.findAction( startActionName, startCopyNr );
      workflow.setStartActionMeta( startActionMeta );
    }

    getWorkflowMap().addWorkflow( workflow.getWorkflowName(), serverObjectId, workflow, workflowConfiguration );

    return workflow;
  }

  protected IPipelineEngine<PipelineMeta> createPipeline( PipelineConfiguration pipelineConfiguration ) throws HopException, HopException, ParseException {
    PipelineMeta pipelineMeta = pipelineConfiguration.getPipelineMeta();
    PipelineExecutionConfiguration pipelineExecutionConfiguration = pipelineConfiguration.getPipelineExecutionConfiguration();
    pipelineMeta.setLogLevel( pipelineExecutionConfiguration.getLogLevel() );
    pipelineMeta.injectVariables( pipelineExecutionConfiguration.getVariablesMap() );

    IHopMetadataProvider metadataProvider = pipelineConfiguration.getMetadataProvider();

    // Also copy the parameters over...
    copyParameters( pipelineMeta, pipelineExecutionConfiguration.getParametersMap() );

    String serverObjectId = UUID.randomUUID().toString();
    SimpleLoggingObject servletLoggingObject = getServletLogging( serverObjectId, pipelineExecutionConfiguration.getLogLevel() );

    // Create the pipeline and store in the list...
    //
    String runConfigurationName = pipelineConfiguration.getPipelineExecutionConfiguration().getRunConfiguration();
    final IPipelineEngine<PipelineMeta> pipeline = PipelineEngineFactory.createPipelineEngine( runConfigurationName, metadataProvider, pipelineMeta );
    pipeline.setParent( servletLoggingObject );
    pipeline.setMetadataProvider( metadataProvider );

    if ( pipelineExecutionConfiguration.isSetLogfile() ) {
      String realLogFilename = pipelineExecutionConfiguration.getLogFileName();
      try {
        FileUtil.createParentFolder( AddPipelineServlet.class, realLogFilename, pipelineExecutionConfiguration
          .isCreateParentFolder(), pipeline.getLogChannel() );
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

    pipeline.setContainerId( serverObjectId );
    getPipelineMap().addPipeline( pipelineMeta.getName(), serverObjectId, pipeline, pipelineConfiguration );

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

  private void copyWorkflowParameters( IWorkflowEngine<WorkflowMeta> workflow, Map<String, String> params ) throws UnknownParamException {
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

  private SimpleLoggingObject getServletLogging( final String serverObjectId, final LogLevel level ) {
    SimpleLoggingObject servletLoggingObject =
      new SimpleLoggingObject( getContextPath(), LoggingObjectType.HOP_SERVER, null );
    servletLoggingObject.setContainerObjectId( serverObjectId );
    servletLoggingObject.setLogLevel( level );
    return servletLoggingObject;
  }

}
