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

package org.apache.hop.www;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.RepositoryPluginType;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobAdapter;
import org.apache.hop.job.JobConfiguration;
import org.apache.hop.job.JobExecutionConfiguration;
import org.apache.hop.job.JobMeta;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.RepositoriesMeta;
import org.apache.hop.repository.Repository;
import org.apache.hop.repository.RepositoryDirectory;
import org.apache.hop.repository.RepositoryDirectoryInterface;
import org.apache.hop.repository.RepositoryMeta;

public class ExecuteJobServlet extends BaseHttpServlet implements HopServerPluginInterface {

  private static Class<?> PKG = ExecuteJobServlet.class; // i18n

  private static final long serialVersionUID = -5879219287669847357L;

  public static final String CONTEXT_PATH = "/hop/executeJob";

  public ExecuteJobServlet() {
  }

  public ExecuteJobServlet( JobMap jobMap ) {
    super( jobMap );
  }


  /**
 <div id="mindtouch">
    <h1>/hop/executeJob</h1>
    <a name="GET"></a>
    <h2>GET</h2>
    <p>Executes job from the specified repository.
  Connects to the repository provided as a parameter, loads the job from it and executes it.
  Empty response is returned or response contains output of an error happened during the job execution.
  Response contains <code>ERROR</code> result if error happened during job execution.</p>

    <p><b>Example Request:</b><br />
    <pre function="syntax.xml">
    GET /hop/executeJob/?rep=my_repository&user=my_user&pass=my_password&job=my_job&level=INFO
    </pre>

    </p>
    <h3>Parameters</h3>
    <table class="pentaho-table">
    <tbody>
    <tr>
      <th>name</th>
      <th>description</th>
      <th>type</th>
    </tr>
    <tr>
    <td>rep</td>
    <td>Repository id to connect to.</td>
    <td>query</td>
    </tr>
    <tr>
    <td>user</td>
    <td>User name to be used to connect to repository.</td>
    <td>query</td>
    </tr>
    <tr>
    <td>pass</td>
    <td>User password to be used to connect to repository.</td>
    <td>query</td>
    </tr>
    <tr>
    <td>job</td>
    <td>Job name to be loaded and executed.</td>
    <td>query</td>
    </tr>
    <tr>
    <td>level</td>
    <td>Logging level to be used for job execution (i.e. Debug).</td>
    <td>query</td>
    </tr>
    <tr>
    <td>*any name*</td>
    <td>All the other parameters will be sent to the job for using as variables.
  When necessary you can add custom parameters to the request.
  They will be used to set the job variables values.</td>
    <td>query</td>
    </tr>
    </tbody>
    </table>

  <h3>Response Body</h3>

  <table class="pentaho-table">
    <tbody>
      <tr>
        <td align="right">element:</td>
        <td>(custom)</td>
      </tr>
      <tr>
        <td align="right">media types:</td>
        <td>application/xml</td>
      </tr>
    </tbody>
  </table>
    <p>Response contains error output of the job executed or HopServer object Id
  if the execution was successful.</p>

    <p><b>Example Error Response:</b></p>
    <pre function="syntax.xml">
  <webresult>
    <result>OK</result>
    <message>Job started</message>
    <id>74d96aa6-f29a-4bac-a26a-06a8c8f107e5</id>
  </webresult>
    </pre>

    <h3>Status Codes</h3>
    <table class="pentaho-table">
  <tbody>
    <tr>
      <th>code</th>
      <th>description</th>
    </tr>
    <tr>
      <td>200</td>
      <td>Request was processed.</td>
    </tr>
    <tr>
      <td>500</td>
      <td>Internal server error occurs during request processing.</td>
    </tr>
  </tbody>
</table>
</div>
  */
  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException,
    IOException {
    if ( isJettyMode() && !request.getContextPath().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "ExecuteJobServlet.Log.ExecuteJobRequested" ) );
    }

    // Options taken from PAN
    //
    String[] knownOptions = new String[] { "rep", "user", "pass", "job", "level", };

    String repOption = request.getParameter( "rep" );
    String userOption = request.getParameter( "user" );
    String passOption = Encr.decryptPasswordOptionallyEncrypted( request.getParameter( "pass" ) );
    String jobOption = request.getParameter( "job" );
    String levelOption = request.getParameter( "level" );

    response.setStatus( HttpServletResponse.SC_OK );

    String encoding = System.getProperty( "HOP_DEFAULT_SERVLET_ENCODING", null );
    if ( encoding != null && !Utils.isEmpty( encoding.trim() ) ) {
      response.setCharacterEncoding( encoding );
      response.setContentType( "text/html; charset=" + encoding );
    }

    PrintWriter out = response.getWriter();

    try {

      final Repository repository = openRepository( repOption, userOption, passOption );
      final JobMeta jobMeta = loadJob( repository, jobOption );

      // Set the servlet parameters as variables in the job
      //
      String[] parameters = jobMeta.listParameters();
      Enumeration<?> parameterNames = request.getParameterNames();
      while ( parameterNames.hasMoreElements() ) {
        String parameter = (String) parameterNames.nextElement();
        String[] values = request.getParameterValues( parameter );

        // Ignore the known options. set the rest as variables
        //
        if ( Const.indexOfString( parameter, knownOptions ) < 0 ) {
          // If it's a job parameter, set it, otherwise simply set the variable
          //
          if ( Const.indexOfString( parameter, parameters ) < 0 ) {
            jobMeta.setVariable( parameter, values[0] );
          } else {
            jobMeta.setParameterValue( parameter, values[0] );
          }
        }
      }

      JobExecutionConfiguration jobExecutionConfiguration = new JobExecutionConfiguration();
      LogLevel logLevel = LogLevel.getLogLevelForCode( levelOption );
      jobExecutionConfiguration.setLogLevel( logLevel );
      JobConfiguration jobConfiguration = new JobConfiguration( jobMeta, jobExecutionConfiguration );

      String carteObjectId = UUID.randomUUID().toString();
      SimpleLoggingObject servletLoggingObject =
        new SimpleLoggingObject( CONTEXT_PATH, LoggingObjectType.CARTE, null );
      servletLoggingObject.setContainerObjectId( carteObjectId );
      servletLoggingObject.setLogLevel( logLevel );

      // Create the job and store in the list...
      //
      final Job job = new Job( repository, jobMeta, servletLoggingObject );

      job.setRepository( repository );
      job.setSocketRepository( getSocketRepository() );

      getJobMap().addJob( jobMeta.getName(), carteObjectId, job, jobConfiguration );
      job.setContainerObjectId( carteObjectId );

      if ( repository != null ) {
        // The repository connection is open: make sure we disconnect from the repository once we
        // are done with this job.
        //
        job.addJobListener( new JobAdapter() {
          @Override public void jobFinished( Job job ) {
            repository.disconnect();
          }
        } );
      }

      try {
        runJob( job );
        WebResult webResult = new WebResult( WebResult.STRING_OK, "Job started", carteObjectId );
        out.println( webResult.getXML() );
        out.flush();

      } catch ( Exception executionException ) {
        String logging = HopLogStore.getAppender().getBuffer( job.getLogChannelId(), false ).toString();
        throw new HopException( "Error executing job: " + logging, executionException );
      }
    } catch ( Exception ex ) {

      out.println( new WebResult( WebResult.STRING_ERROR, BaseMessages.getString(
        PKG, "ExecuteJobServlet.Error.UnexpectedError", Const.CR + Const.getStackTracker( ex ) ) ) );
    }
  }

  private JobMeta loadJob( Repository repository, String job ) throws HopException {

    if ( repository == null ) {

      // Without a repository it's a filename --> file:///foo/bar/job.kjb
      //
      JobMeta jobMeta = new JobMeta( job, repository );
      return jobMeta;

    } else {

      // With a repository we need to load it from /foo/bar/Job
      // We need to extract the folder name from the path in front of the name...
      //
      String directoryPath;
      String name;
      int lastSlash = job.lastIndexOf( RepositoryDirectory.DIRECTORY_SEPARATOR );
      if ( lastSlash < 0 ) {
        directoryPath = "/";
        name = job;
      } else {
        directoryPath = job.substring( 0, lastSlash );
        name = job.substring( lastSlash + 1 );
      }
      RepositoryDirectoryInterface directory =
        repository.loadRepositoryDirectoryTree().findDirectory( directoryPath );
      if ( directory == null ) {
        throw new HopException( "Unable to find directory path '" + directoryPath + "' in the repository" );
      }

      ObjectId jobID = repository.getJobId( name, directory );
      if ( jobID == null ) {
        throw new HopException( "Unable to find job '" + name + "' in directory :" + directory );
      }
      JobMeta jobMeta = repository.loadJob( jobID, null );
      return jobMeta;
    }
  }

  private Repository openRepository( String repositoryName, String user, String pass ) throws HopException {

    if ( Utils.isEmpty( repositoryName ) ) {
      return null;
    }

    RepositoriesMeta repositoriesMeta = new RepositoriesMeta();
    repositoriesMeta.readData();
    RepositoryMeta repositoryMeta = repositoriesMeta.findRepository( repositoryName );
    if ( repositoryMeta == null ) {
      throw new HopException( "Unable to find repository: " + repositoryName );
    }
    PluginRegistry registry = PluginRegistry.getInstance();
    Repository repository = registry.loadClass( RepositoryPluginType.class, repositoryMeta, Repository.class );
    repository.init( repositoryMeta );
    repository.connect( user, pass );
    return repository;
  }

  public String toString() {
    return "Start job";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  protected void runJob( Job job ) {
    // Execute the job...
    //
    job.start();
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

}
