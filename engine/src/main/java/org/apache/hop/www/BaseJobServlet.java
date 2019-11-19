/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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
import org.apache.hop.job.JobAdapter;
import org.apache.hop.job.JobConfiguration;
import org.apache.hop.job.JobExecutionConfiguration;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.repository.Repository;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransAdapter;
import org.apache.hop.trans.TransConfiguration;
import org.apache.hop.trans.TransExecutionConfiguration;
import org.apache.hop.trans.TransMeta;

import java.util.Map;
import java.util.UUID;

public abstract class BaseJobServlet extends BodyHttpServlet {

  private static final long serialVersionUID = 8523062215275251356L;

  protected Job createJob( JobConfiguration jobConfiguration ) throws UnknownParamException {
    JobExecutionConfiguration jobExecutionConfiguration = jobConfiguration.getJobExecutionConfiguration();

    JobMeta jobMeta = jobConfiguration.getJobMeta();
    jobMeta.setLogLevel( jobExecutionConfiguration.getLogLevel() );
    jobMeta.injectVariables( jobExecutionConfiguration.getVariables() );

    // If there was a repository, we know about it at this point in time.
    final Repository repository = jobConfiguration.getJobExecutionConfiguration().getRepository();

    String carteObjectId = UUID.randomUUID().toString();

    SimpleLoggingObject servletLoggingObject =
        getServletLogging( carteObjectId, jobExecutionConfiguration.getLogLevel() );

    // Create the transformation and store in the list...
    final Job job = new Job( repository, jobMeta, servletLoggingObject );
    // Setting variables
    job.initializeVariablesFrom( null );
    job.getJobMeta().setMetaStore( jobMap.getSlaveServerConfig().getMetaStore() );
    job.getJobMeta().setInternalHopVariables( job );
    job.injectVariables( jobConfiguration.getJobExecutionConfiguration().getVariables() );
    job.setArguments( jobExecutionConfiguration.getArgumentStrings() );
    job.setSocketRepository( getSocketRepository() );

    copyJobParameters( job, jobExecutionConfiguration.getParams() );

    // Check if there is a starting point specified.
    String startCopyName = jobExecutionConfiguration.getStartCopyName();
    if ( startCopyName != null && !startCopyName.isEmpty() ) {
      int startCopyNr = jobExecutionConfiguration.getStartCopyNr();
      JobEntryCopy startJobEntryCopy = jobMeta.findJobEntry( startCopyName, startCopyNr, false );
      job.setStartJobEntryCopy( startJobEntryCopy );
    }

    // Do we need to expand the job when it's running?
    // Note: the plugin (Job and Trans) job entries need to call the delegation listeners in the parent job.
    if ( jobExecutionConfiguration.isExpandingRemoteJob() ) {
      job.addDelegationListener( new HopServerDelegationHandler( getTransformationMap(), getJobMap() ) );
    }

    // Make sure to disconnect from the repository when the job finishes.
    if ( repository != null ) {
      job.addJobListener( new JobAdapter() {
        public void jobFinished( Job job ) {
          repository.disconnect();
        }
      } );
    }

    getJobMap().addJob( job.getJobname(), carteObjectId, job, jobConfiguration );

    final Long passedBatchId = jobExecutionConfiguration.getPassedBatchId();
    if ( passedBatchId != null ) {
      job.setPassedBatchId( passedBatchId );
    }

    return job;
  }

  protected Trans createTrans( TransConfiguration transConfiguration ) throws UnknownParamException {
    TransMeta transMeta = transConfiguration.getTransMeta();
    TransExecutionConfiguration transExecutionConfiguration = transConfiguration.getTransExecutionConfiguration();
    transMeta.setLogLevel( transExecutionConfiguration.getLogLevel() );
    transMeta.injectVariables( transExecutionConfiguration.getVariables() );

    // Also copy the parameters over...
    copyParameters( transMeta, transExecutionConfiguration.getParams() );

    String carteObjectId = UUID.randomUUID().toString();
    SimpleLoggingObject servletLoggingObject =
        getServletLogging( carteObjectId, transExecutionConfiguration.getLogLevel() );

    // Create the transformation and store in the list...
    final Trans trans = new Trans( transMeta, servletLoggingObject );
    trans.setMetaStore( transformationMap.getSlaveServerConfig().getMetaStore() );

    if ( transExecutionConfiguration.isSetLogfile() ) {
      String realLogFilename = transExecutionConfiguration.getLogFileName();
      try {
        FileUtil.createParentFolder( AddTransServlet.class, realLogFilename, transExecutionConfiguration
            .isCreateParentFolder(), trans.getLogChannel(), trans );
        final LogChannelFileWriter logChannelFileWriter =
            new LogChannelFileWriter( servletLoggingObject.getLogChannelId(),
                HopVFS.getFileObject( realLogFilename ), transExecutionConfiguration.isSetAppendLogfile() );
        logChannelFileWriter.startLogging();

        trans.addTransListener( new TransAdapter() {
          @Override
          public void transFinished( Trans trans ) throws HopException {
            if ( logChannelFileWriter != null ) {
              logChannelFileWriter.stopLogging();
            }
          }
        } );
      } catch ( HopException e ) {
        logError( Const.getStackTracker( e ) );
      }

    }

    // If there was a repository, we know about it at this point in time.
    final Repository repository = transExecutionConfiguration.getRepository();

    trans.setRepository( repository );
    trans.setSocketRepository( getSocketRepository() );

    trans.setContainerObjectId( carteObjectId );
    getTransformationMap().addTransformation( transMeta.getName(), carteObjectId, trans, transConfiguration );

    if ( repository != null ) {
      // The repository connection is open: make sure we disconnect from the repository once we
      // are done with this transformation.
      trans.addTransListener( new TransAdapter() {
        @Override public void transFinished( Trans trans ) {
          repository.disconnect();
        }
      } );
    }
    final Long passedBatchId = transExecutionConfiguration.getPassedBatchId();
    if ( passedBatchId != null ) {
      trans.setPassedBatchId( passedBatchId );
    }

    return trans;
  }

  private void copyParameters( final AbstractMeta meta, final Map<String, String> params  ) throws UnknownParamException {
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
