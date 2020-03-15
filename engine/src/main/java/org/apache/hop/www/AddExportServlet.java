/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobConfiguration;
import org.apache.hop.job.JobExecutionConfiguration;
import org.apache.hop.job.JobMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransConfiguration;
import org.apache.hop.trans.TransExecutionConfiguration;
import org.apache.hop.trans.TransMeta;
import org.w3c.dom.Document;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Map;
import java.util.UUID;

/**
 * This servlet allows you to transport an exported job or transformation over to the carte server as a zip file. It
 * ends up in a temporary file.
 * <p>
 * The servlet returns the name of the file stored.
 *
 * @author matt
 */
// has been replaced by RegisterPackageServlet
@Deprecated
public class AddExportServlet extends BaseHttpServlet implements HopServerPluginInterface {
  public static final String PARAMETER_LOAD = "load";
  public static final String PARAMETER_TYPE = "type";

  public static final String TYPE_JOB = "job";
  public static final String TYPE_TRANS = "trans";

  private static final long serialVersionUID = -6850701762586992604L;
  public static final String CONTEXT_PATH = "/hop/addExport";

  public AddExportServlet() {
  }

  public AddExportServlet( JobMap jobMap, TransformationMap transformationMap ) {
    super( transformationMap, jobMap );
  }

  /**
   * <div id="mindtouch">
   * <h1>/hop/addExport</h1>
   * <a name="POST"></a>
   * <h2>POST</h2>
   * <p>Returns the list of users in the platform. This list is in an xml format as shown in the example response.
   * Uploads and executes previously exported job or transformation.
   * Uploads zip file containing job or transformation to be executed and executes it.
   * Method relies on the input parameters to find the entity to be executed. The archive is
   * transferred within request body.
   *
   * <code>File url of the executed entity </code> will be returned in the Response object
   * or <code>message</code> describing error occurred. To determine if the call is successful
   * rely on <code>result</code> parameter in response.</p>
   *
   * <p><b>Example Request:</b><br />
   * <pre function="syntax.xml">
   * POST /hop/addExport/?type=job&load=dummy_job.kjb
   * </pre>
   * Request body should contain zip file prepared for HopServer execution.
   * </p>
   * <h3>Parameters</h3>
   * <table class="pentaho-table">
   * <tbody>
   * <tr>
   * <th>name</th>
   * <th>description</th>
   * <th>type</th>
   * </tr>
   * <tr>
   * <td>type</td>
   * <td>The type of the entity to be executed either <code>job</code> or <code>trans</code>.</td>
   * <td>query</td>
   * </tr>
   * <tr>
   * <td>load</td>
   * <td>The name of the entity within archive to be executed.</td>
   * <td>query</td>
   * </tr>
   * </tbody>
   * </table>
   *
   * <h3>Response Body</h3>
   *
   * <table class="pentaho-table">
   * <tbody>
   * <tr>
   * <td align="right">element:</td>
   * <td>(custom)</td>
   * </tr>
   * <tr>
   * <td align="right">media types:</td>
   * <td>application/xml</td>
   * </tr>
   * </tbody>
   * </table>
   * <p>Response wraps file url of the entity that was executed or error stack trace if an error occurred.
   * Response has <code>result</code> OK if there were no errors. Otherwise it returns ERROR.</p>
   *
   * <p><b>Example Response:</b></p>
   * <pre function="syntax.xml">
   * <?xml version="1.0" encoding="UTF-8"?>
   * <webresult>
   * <result>OK</result>
   * <message>zip&#x3a;file&#x3a;&#x2f;&#x2f;&#x2f;temp&#x2f;export_ee2a67de-6a72-11e4-82c0-4701a2bac6a5.zip&#x21;dummy_job.kjb</message>
   * <id>74cf4219-c881-4633-a71a-2ed16b7db7b8</id>
   * </webresult>
   * </pre>
   *
   * <h3>Status Codes</h3>
   * <table class="pentaho-table">
   * <tbody>
   * <tr>
   * <th>code</th>
   * <th>description</th>
   * </tr>
   * <tr>
   * <td>200</td>
   * <td>Request was processed and XML response is returned.</td>
   * </tr>
   * <tr>
   * <td>500</td>
   * <td>Internal server error occurs during request processing.</td>
   * </tr>
   * </tbody>
   * </table>
   * </div>
   */

  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException,
    IOException {
    if ( isJettyMode() && !request.getRequestURI().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    if ( log.isDebug() ) {
      logDebug( "Addition of export requested" );
    }

    PrintWriter out = response.getWriter();
    InputStream in = request.getInputStream(); // read from the client
    if ( log.isDetailed() ) {
      logDetailed( "Encoding: " + request.getCharacterEncoding() );
    }

    boolean isJob = TYPE_JOB.equalsIgnoreCase( request.getParameter( PARAMETER_TYPE ) );
    String load = request.getParameter( PARAMETER_LOAD ); // the resource to load

    response.setContentType( "text/xml" );
    out.print( XMLHandler.getXMLHeader() );

    response.setStatus( HttpServletResponse.SC_OK );

    OutputStream outputStream = null;

    try {
      FileObject tempFile = HopVFS.createTempFile( "export", ".zip", System.getProperty( "java.io.tmpdir" ) );
      outputStream = HopVFS.getOutputStream( tempFile, false );

      // Pass the input directly to a temporary file
      //
      // int size = 0;
      int c;
      while ( ( c = in.read() ) != -1 ) {
        outputStream.write( c );
        // size++;
      }

      outputStream.flush();
      outputStream.close();
      outputStream = null; // don't close it twice

      String archiveUrl = tempFile.getName().toString();
      String fileUrl = null;

      String carteObjectId = null;
      SimpleLoggingObject servletLoggingObject =
        new SimpleLoggingObject( CONTEXT_PATH, LoggingObjectType.CARTE, null );

      // Now open the top level resource...
      //
      if ( !Utils.isEmpty( load ) ) {

        fileUrl = "zip:" + archiveUrl + "!" + load;

        if ( isJob ) {
          // Open the job from inside the ZIP archive
          //
          HopVFS.getFileObject( fileUrl );

          JobMeta jobMeta = new JobMeta( fileUrl );

          // Also read the execution configuration information
          //
          String configUrl = "zip:" + archiveUrl + "!" + Job.CONFIGURATION_IN_EXPORT_FILENAME;
          Document configDoc = XMLHandler.loadXMLFile( configUrl );
          JobExecutionConfiguration jobExecutionConfiguration =
            new JobExecutionConfiguration( XMLHandler.getSubNode( configDoc, JobExecutionConfiguration.XML_TAG ) );

          carteObjectId = UUID.randomUUID().toString();
          servletLoggingObject.setContainerObjectId( carteObjectId );
          servletLoggingObject.setLogLevel( jobExecutionConfiguration.getLogLevel() );

          Job job = new Job( jobMeta, servletLoggingObject );

          // Do we need to expand the job when it's running?
          // Note: the plugin (Job and Trans) job entries need to call the delegation listeners in the parent job.
          //
          if ( jobExecutionConfiguration.isExpandingRemoteJob() ) {
            job.addDelegationListener( new HopServerDelegationHandler( getTransformationMap(), getJobMap() ) );
          }

          // store it all in the map...
          //
          getJobMap().addJob(
            job.getJobname(), carteObjectId, job, new JobConfiguration( jobMeta, jobExecutionConfiguration ) );

          // Apply the execution configuration...
          //
          log.setLogLevel( jobExecutionConfiguration.getLogLevel() );
          jobMeta.injectVariables( jobExecutionConfiguration.getVariables() );

          // Also copy the parameters over...
          //
          Map<String, String> params = jobExecutionConfiguration.getParams();
          for ( String param : params.keySet() ) {
            String value = params.get( param );
            jobMeta.setParameterValue( param, value );
          }

        } else {
          // Open the transformation from inside the ZIP archive
          //
          IMetaStore metaStore = transformationMap.getSlaveServerConfig().getMetaStore();
          TransMeta transMeta = new TransMeta( fileUrl, metaStore, true, Variables.getADefaultVariableSpace() );

          // Also read the execution configuration information
          //
          String configUrl = "zip:" + archiveUrl + "!" + Trans.CONFIGURATION_IN_EXPORT_FILENAME;
          Document configDoc = XMLHandler.loadXMLFile( configUrl );
          TransExecutionConfiguration executionConfiguration =
            new TransExecutionConfiguration( XMLHandler.getSubNode(
              configDoc, TransExecutionConfiguration.XML_TAG ) );

          carteObjectId = UUID.randomUUID().toString();
          servletLoggingObject.setContainerObjectId( carteObjectId );
          servletLoggingObject.setLogLevel( executionConfiguration.getLogLevel() );

          Trans trans = new Trans( transMeta, servletLoggingObject );

          // store it all in the map...
          //
          getTransformationMap().addTransformation(
            trans.getName(), carteObjectId, trans, new TransConfiguration( transMeta, executionConfiguration ) );
        }
      } else {
        fileUrl = archiveUrl;
      }

      out.println( new WebResult( WebResult.STRING_OK, fileUrl, carteObjectId ) );
    } catch ( Exception ex ) {
      out.println( new WebResult( WebResult.STRING_ERROR, Const.getStackTracker( ex ) ) );
    } finally {
      if ( outputStream != null ) {
        outputStream.close();
      }
    }
  }

  public String toString() {
    return "Add export";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }
}
