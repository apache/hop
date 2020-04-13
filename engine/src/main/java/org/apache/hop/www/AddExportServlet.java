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

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
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
 * This servlet allows you to transport an exported workflow or transformation over to the carte server as a zip file. It
 * ends up in a temporary file.
 * <p>
 * The servlet returns the name of the file stored.
 *
 * @author matt
 */
public class AddExportServlet extends BaseHttpServlet implements IHopServerPlugin {
  public static final String PARAMETER_LOAD = "load";
  public static final String PARAMETER_TYPE = "type";

  public static final String TYPE_JOB = "workflow";
  public static final String TYPE_PIPELINE = "pipeline";

  private static final long serialVersionUID = -6850701762586992604L;
  public static final String CONTEXT_PATH = "/hop/addExport";

  public AddExportServlet() {
  }

  public AddExportServlet( WorkflowMap workflowMap, PipelineMap transformationMap ) {
    super( transformationMap, workflowMap );
  }

  /**
   * <div id="mindtouch">
   * <h1>/hop/addExport</h1>
   * <a name="POST"></a>
   * <h2>POST</h2>
   * <p>Returns the list of users in the platform. This list is in an xml format as shown in the example response.
   * Uploads and executes previously exported workflow or transformation.
   * Uploads zip file containing workflow or transformation to be executed and executes it.
   * Method relies on the input parameters to find the entity to be executed. The archive is
   * transferred within request body.
   *
   * <code>File url of the executed entity </code> will be returned in the Response object
   * or <code>message</code> describing error occurred. To determine if the call is successful
   * rely on <code>result</code> parameter in response.</p>
   *
   * <p><b>Example Request:</b><br />
   * <pre function="syntax.xml">
   * POST /hop/addExport/?type=workflow&load=dummy_job.hwf
   * </pre>
   * Request body should contain zip file prepared for HopServer execution.
   * </p>
   * <h3>Parameters</h3>
   * <table class="hop-table">
   * <tbody>
   * <tr>
   * <th>name</th>
   * <th>description</th>
   * <th>type</th>
   * </tr>
   * <tr>
   * <td>type</td>
   * <td>The type of the entity to be executed either <code>workflow</code> or <code>pipeline</code>.</td>
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
   * <table class="hop-table">
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
   * <message>zip&#x3a;file&#x3a;&#x2f;&#x2f;&#x2f;temp&#x2f;export_ee2a67de-6a72-11e4-82c0-4701a2bac6a5.zip&#x21;dummy_job.hwf</message>
   * <id>74cf4219-c881-4633-a71a-2ed16b7db7b8</id>
   * </webresult>
   * </pre>
   *
   * <h3>Status Codes</h3>
   * <table class="hop-table">
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
    out.print( XmlHandler.getXMLHeader() );

    response.setStatus( HttpServletResponse.SC_OK );

    OutputStream outputStream = null;

    try {
      FileObject tempFile = HopVfs.createTempFile( "export", ".zip", System.getProperty( "java.io.tmpdir" ) );
      outputStream = HopVfs.getOutputStream( tempFile, false );

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
          // Open the workflow from inside the ZIP archive
          //
          HopVfs.getFileObject( fileUrl );

          WorkflowMeta workflowMeta = new WorkflowMeta( fileUrl );

          // Also read the execution configuration information
          //
          String configUrl = "zip:" + archiveUrl + "!" + Workflow.CONFIGURATION_IN_EXPORT_FILENAME;
          Document configDoc = XmlHandler.loadXmlFile( configUrl );
          WorkflowExecutionConfiguration workflowExecutionConfiguration =
            new WorkflowExecutionConfiguration( XmlHandler.getSubNode( configDoc, WorkflowExecutionConfiguration.XML_TAG ) );

          carteObjectId = UUID.randomUUID().toString();
          servletLoggingObject.setContainerObjectId( carteObjectId );
          servletLoggingObject.setLogLevel( workflowExecutionConfiguration.getLogLevel() );

          Workflow workflow = new Workflow( workflowMeta, servletLoggingObject );

          // Do we need to expand the workflow when it's running?
          // Note: the plugin (Workflow and Pipeline) actions need to call the delegation listeners in the parent workflow.
          //
          if ( workflowExecutionConfiguration.isExpandingRemoteJob() ) {
            workflow.addDelegationListener( new HopServerDelegationHandler( getPipelineMap(), getWorkflowMap() ) );
          }

          // store it all in the map...
          //
          getWorkflowMap().addWorkflow(
            workflow.getJobname(), carteObjectId, workflow, new WorkflowConfiguration( workflowMeta, workflowExecutionConfiguration ) );

          // Apply the execution configuration...
          //
          log.setLogLevel( workflowExecutionConfiguration.getLogLevel() );
          workflowMeta.injectVariables( workflowExecutionConfiguration.getVariablesMap() );

          // Also copy the parameters over...
          //
          Map<String, String> params = workflowExecutionConfiguration.getParametersMap();
          for ( String param : params.keySet() ) {
            String value = params.get( param );
            workflowMeta.setParameterValue( param, value );
          }

        } else {
          // Open the transformation from inside the ZIP archive
          //
          IMetaStore metaStore = pipelineMap.getSlaveServerConfig().getMetaStore();
          PipelineMeta pipelineMeta = new PipelineMeta( fileUrl, metaStore, true, Variables.getADefaultVariableSpace() );

          // Also read the execution configuration information
          //
          String configUrl = "zip:" + archiveUrl + "!" + Pipeline.CONFIGURATION_IN_EXPORT_FILENAME;
          Document configDoc = XmlHandler.loadXmlFile( configUrl );
          PipelineExecutionConfiguration executionConfiguration =
            new PipelineExecutionConfiguration( XmlHandler.getSubNode(
              configDoc, PipelineExecutionConfiguration.XML_TAG ) );

          carteObjectId = UUID.randomUUID().toString();
          servletLoggingObject.setContainerObjectId( carteObjectId );
          servletLoggingObject.setLogLevel( executionConfiguration.getLogLevel() );

          Pipeline pipeline = new Pipeline( pipelineMeta, servletLoggingObject );

          // store it all in the map...
          //
          getPipelineMap().addPipeline( pipeline.getName(), carteObjectId, pipeline, new PipelineConfiguration( pipelineMeta, executionConfiguration ) );
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
