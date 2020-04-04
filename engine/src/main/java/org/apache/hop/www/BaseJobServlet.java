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
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobConfiguration;
import org.apache.hop.job.JobExecutionConfiguration;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.pipeline.ExecutionAdapter;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;

import java.util.Map;
import java.util.UUID;

public abstract class BaseJobServlet extends BodyHttpServlet {

  private static final long serialVersionUID = 8523062215275251356L;

  protected Job createJob( JobConfiguration jobConfiguration ) throws UnknownParamException {
    JobExecutionConfiguration jobExecutionConfiguration = jobConfiguration.getJobExecutionConfiguration();

    JobMeta jobMeta = jobConfiguration.getJobMeta();
    jobMeta.setLogLevel( jobExecutionConfiguration.getLogLevel() );
    jobMeta.injectVariables( jobExecutionConfiguration.getVariablesMap() );

    String carteObjectId = UUID.randomUUID().toString();

    SimpleLoggingObject servletLoggingObject =
      getServletLogging( carteObjectId, jobExecutionConfiguration.getLogLevel() );

    // Create the pipeline and store in the list...
    final Job job = new Job( jobMeta, servletLoggingObject );
    // Setting variables
    job.initializeVariablesFrom( null );
    job.getJobMeta().setMetaStore( jobMap.getSlaveServerConfig().getMetaStore() );
    job.getJobMeta().setInternalHopVariables( job );
    job.injectVariables( jobConfiguration.getJobExecutionConfiguration().getVariablesMap() );
    job.setSocketRepository( getSocketRepository() );

    copyJobParameters( job, jobExecutionConfiguration.getParametersMap() );

    // Check if there is a starting point specified.
    String startCopyName = jobExecutionConfiguration.getStartCopyName();
    if ( startCopyName != null && !startCopyName.isEmpty() ) {
      int startCopyNr = jobExecutionConfiguration.getStartCopyNr();
      JobEntryCopy startJobEntryCopy = jobMeta.findJobEntry( startCopyName, startCopyNr );
      job.setStartJobEntryCopy( startJobEntryCopy );
    }

    // Do we need to expand the job when it's running?
    // Note: the plugin (Job and Pipeline) job entries need to call the delegation listeners in the parent job.
    if ( jobExecutionConfiguration.isExpandingRemoteJob() ) {
      job.addDelegationListener( new HopServerDelegationHandler( getPipelineMap(), getJobMap() ) );
    }

    getJobMap().addJob( job.getJobname(), carteObjectId, job, jobConfiguration );

    final Long passedBatchId = jobExecutionConfiguration.getPassedBatchId();
    if ( passedBatchId != null ) {
      job.setPassedBatchId( passedBatchId );
    }

    return job;
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
            HopVFS.getFileObject( realLogFilename ), pipelineExecutionConfiguration.isSetAppendLogfile() );
        logChannelFileWriter.startLogging();

        pipeline.addPipelineListener( new ExecutionAdapter<PipelineMeta>() {
          @Override
          public void finished( IPipelineEngine<PipelineMeta> pipeline ) throws HopException {
            if ( logChannelFileWriter != null ) {
              logChannelFileWriter.stopLogging();
            }
          }
        } );
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

  private void copyJobParameters( Job job, Map<String, String> params ) throws UnknownParamException {
    JobMeta jobMeta = job.getJobMeta();
    // Also copy the parameters over...
    job.copyParametersFrom( jobMeta );
    job.clearParameters();
    String[] parameterNames = job.listParameters();
    for ( String parameterName : parameterNames ) {
      // Grab the parameter value set in the job entry
      String thisValue = params.get( parameterName );
      if ( !StringUtils.isBlank( thisValue ) ) {
        // Set the value as specified by the user in the job entry
        jobMeta.setParameterValue( parameterName, thisValue );
      }
    }
    jobMeta.activateParameters();
  }

  private SimpleLoggingObject getServletLogging( final String carteObjectId, final LogLevel level ) {
    SimpleLoggingObject servletLoggingObject =
      new SimpleLoggingObject( getContextPath(), LoggingObjectType.CARTE, null );
    servletLoggingObject.setContainerObjectId( carteObjectId );
    servletLoggingObject.setLogLevel( level );
    return servletLoggingObject;
  }

}
