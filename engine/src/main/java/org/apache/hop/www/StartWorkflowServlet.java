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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.www.cache.HopServerStatusCache;
import org.owasp.encoder.Encode;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.UUID;


public class StartWorkflowServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static Class<?> PKG = StartWorkflowServlet.class; // for i18n purposes,
  // needed by
  // Translator!!

  private static final long serialVersionUID = -8487225953910464032L;

  public static final String CONTEXT_PATH = "/hop/startWorkflow";

  @VisibleForTesting
  HopServerStatusCache cache = HopServerStatusCache.getInstance();

  public StartWorkflowServlet() {
  }

  public StartWorkflowServlet( WorkflowMap workflowMap ) {
    super( workflowMap );
  }

  /**
   * <div id="mindtouch">
   * <h1>/hop/startWorkflow</h1>
   * <a name="GET"></a>
   * <h2>GET</h2>
   * <p>Starts the workflow. If the workflow cannot be started, an error is returned.</p>
   *
   * <p><b>Example Request:</b><br />
   * <pre function="syntax.xml">
   * GET /hop/startWorkflow/?name=dummy_job&xml=Y
   * </pre>
   *
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
   * <td>name</td>
   * <td>Name of the workflow to be executed.</td>
   * <td>query</td>
   * </tr>
   * <tr>
   * <td>xml</td>
   * <td>Boolean flag which sets the output format required. Use <code>Y</code> to receive XML response.</td>
   * <td>boolean, optional</td>
   * </tr>
   * <tr>
   * <td>id</td>
   * <td>HopServer workflow ID of the workflow to be executed. This parameter is optional when xml=Y is used.</td>
   * <td>query, optional</td>
   * </tr>
   * </tbody>
   * </table>
   *
   * <h3>Response Body</h3>
   *
   * <table class="hop-table">
   * <tbody>
   * <tr>
   * <td align="right">text:</td>
   * <td>HTML</td>
   * </tr>
   * <tr>
   * <td align="right">media types:</td>
   * <td>text/xml, text/html</td>
   * </tr>
   * </tbody>
   * </table>
   * <p>Response XML or HTML containing operation result. When using xml=Y <code>result</code> field indicates whether
   * operation was successful (<code>OK</code>) or not (<code>ERROR</code>).</p>
   *
   * <p><b>Example Response:</b></p>
   * <pre function="syntax.xml">
   * <?xml version="1.0" encoding="UTF-8"?>
   * <webresult>
   * <result>OK</result>
   * <message>Workflow &#x5b;dummy_job&#x5d; was started.</message>
   * <id>abd61143-8174-4f27-9037-6b22fbd3e229</id>
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
   * <td>Request was processed.</td>
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
    if ( isJettyMode() && !request.getContextPath().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "StartWorkflowServlet.Log.StartJobRequested" ) );
    }

    String workflowName = request.getParameter( "name" );
    String id = request.getParameter( "id" );
    boolean useXML = "Y".equalsIgnoreCase( request.getParameter( "xml" ) );

    response.setStatus( HttpServletResponse.SC_OK );

    PrintWriter out = response.getWriter();
    if ( useXML ) {
      response.setContentType( "text/xml" );
      response.setCharacterEncoding( Const.XML_ENCODING );
      out.print( XmlHandler.getXMLHeader( Const.XML_ENCODING ) );
    } else {
      response.setContentType( "text/html;charset=UTF-8" );
      out.println( "<HTML>" );
      out.println( "<HEAD>" );
      out.println( "<TITLE>Start workflow</TITLE>" );
      out.println( "<META http-equiv=\"Refresh\" content=\"2;url="
        + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + "?name=" + URLEncoder.encode( workflowName, "UTF-8" )
        + "\">" );
      out.println( "<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">" );
      out.println( "</HEAD>" );
      out.println( "<BODY>" );
    }

    try {
      // ID is optional...
      //
      Workflow workflow;
      HopServerObjectEntry entry;
      if ( Utils.isEmpty( id ) ) {
        // get the first workflow that matches...
        //
        entry = getWorkflowMap().getFirstCarteObjectEntry( workflowName );
        if ( entry == null ) {
          workflow = null;
        } else {
          id = entry.getId();
          workflow = getWorkflowMap().getWorkflow( entry );
        }
      } else {
        // Take the ID into account!
        //
        entry = new HopServerObjectEntry( workflowName, id );
        workflow = getWorkflowMap().getWorkflow( entry );
      }

      if ( workflow != null ) {
        // First see if this workflow already ran to completion.
        // If so, we get an exception is we try to start() the workflow thread
        //
        if ( workflow.isInitialized() && !workflow.isActive() ) {
          // Re-create the workflow from the workflowMeta
          //

          cache.remove( workflow.getLogChannelId() );

          // Create a new workflow object to start from a sane state. Then replace
          // the new workflow in the workflow map
          //
          synchronized ( this ) {
            WorkflowConfiguration workflowConfiguration = getWorkflowMap().getConfiguration( workflowName );

            String carteObjectId = UUID.randomUUID().toString();
            SimpleLoggingObject servletLoggingObject =
              new SimpleLoggingObject( CONTEXT_PATH, LoggingObjectType.CARTE, null );
            servletLoggingObject.setContainerObjectId( carteObjectId );

            Workflow newWorkflow = new Workflow( workflow.getWorkflowMeta(), servletLoggingObject );
            newWorkflow.setLogLevel( workflow.getLogLevel() );

            // Discard old log lines from the old workflow
            //
            HopLogStore.discardLines( workflow.getLogChannelId(), true );

            getWorkflowMap().replaceWorkflow( entry, newWorkflow, workflowConfiguration );
            workflow = newWorkflow;
          }
        }

        runJob( workflow );

        String message = BaseMessages.getString( PKG, "StartWorkflowServlet.Log.JobStarted", workflowName );
        if ( useXML ) {
          out.println( new WebResult( WebResult.STRING_OK, message, id ).getXml() );
        } else {

          out.println( "<H1>" + Encode.forHtml( message ) + "</H1>" );
          out.println( "<a href=\""
            + convertContextPath( GetWorkflowStatusServlet.CONTEXT_PATH ) + "?name="
            + URLEncoder.encode( workflowName, "UTF-8" ) + "&id=" + URLEncoder.encode( id, "UTF-8" ) + "\">"
            + BaseMessages.getString( PKG, "JobStatusServlet.BackToJobStatusPage" ) + "</a><p>" );
        }
      } else {
        String message = BaseMessages.getString( PKG, "StartWorkflowServlet.Log.SpecifiedJobNotFound", workflowName );
        if ( useXML ) {
          out.println( new WebResult( WebResult.STRING_ERROR, message ) );
        } else {
          out.println( "<H1>" + Encode.forHtml( message ) + "</H1>" );
          out.println( "<a href=\""
            + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + "\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.BackToStatusPage" ) + "</a><p>" );
          response.setStatus( HttpServletResponse.SC_BAD_REQUEST );
        }
      }
    } catch ( Exception ex ) {
      if ( useXML ) {
        out.println( new WebResult( WebResult.STRING_ERROR, BaseMessages.getString(
          PKG, "StartWorkflowServlet.Error.UnexpectedError", Const.CR + Const.getStackTracker( ex ) ) ) );
      } else {
        out.println( "<p>" );
        out.println( "<pre>" );
        out.println( Encode.forHtml( Const.getStackTracker( ex ) ) );
        out.println( "</pre>" );
        response.setStatus( HttpServletResponse.SC_BAD_REQUEST );
      }
    }

    if ( !useXML ) {
      out.println( "<p>" );
      out.println( "</BODY>" );
      out.println( "</HTML>" );
    }
  }

  public String toString() {
    return "Start workflow";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  protected void runJob( Workflow workflow ) throws HopException {
    workflow.start(); // runs the thread in the background...
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

}
