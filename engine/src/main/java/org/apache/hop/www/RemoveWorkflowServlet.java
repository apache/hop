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
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.www.cache.HopServerStatusCache;
import org.owasp.encoder.Encode;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;


public class RemoveWorkflowServlet extends BaseHttpServlet implements IHopServerPlugin {

  private static Class<?> PKG = RemoveWorkflowServlet.class; // for i18n purposes, needed by Translator!!
  private static final long serialVersionUID = -2051906998698124039L;

  public static final String CONTEXT_PATH = "/hop/removeWorkflow";

  @VisibleForTesting
  private HopServerStatusCache cache = HopServerStatusCache.getInstance();

  public RemoveWorkflowServlet() {
  }

  public RemoveWorkflowServlet( WorkflowMap workflowMap ) {
    super( workflowMap );
  }

  /**
   * <div id="mindtouch">
   * <h1>/hop/removeWorkflow</h1>
   * <a name="GET"></a>
   * <h2>GET</h2>
   * <p>Remove specified workflow from HopServer server.</p>
   *
   * <p><b>Example Request:</b><br />
   * <pre function="syntax.xml">
   * GET /hop/removeWorkflow/?name=dummy_job&xml=Y
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
   * <td>Name of the workflow to be removed.</td>
   * <td>query</td>
   * </tr>
   * <tr>
   * <td>xml</td>
   * <td>Boolean flag which sets the output format required. Use <code>Y</code> to receive XML response.</td>
   * <td>boolean, optional</td>
   * </tr>
   * <tr>
   * <td>id</td>
   * <td>HopServer workflow ID of the workflow to be removed. This parameter is optional when xml=Y is used.</td>
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
   * <message/>
   * <id/>
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
      logDebug( BaseMessages.getString( PKG, "RemoveWorkflowServlet.Log.RemoveJobRequested" ) );
    }

    String workflowName = request.getParameter( "name" );
    String id = request.getParameter( "id" );
    boolean useXML = "Y".equalsIgnoreCase( request.getParameter( "xml" ) );

    response.setStatus( HttpServletResponse.SC_OK );

    if ( useXML ) {
      response.setContentType( "text/xml" );
      response.setCharacterEncoding( Const.XML_ENCODING );
    } else {
      response.setContentType( "text/html;charset=UTF-8" );
    }

    PrintWriter out = response.getWriter();

    // ID is optional...
    //
    IWorkflowEngine<WorkflowMeta> workflow;
    HopServerObjectEntry entry;
    if ( Utils.isEmpty( id ) ) {
      // get the first pipeline that matches...
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

      cache.remove( workflow.getLogChannelId() );
      HopLogStore.discardLines( workflow.getLogChannelId(), true );
      getWorkflowMap().removeJob( entry );

      if ( useXML ) {
        response.setContentType( "text/xml" );
        response.setCharacterEncoding( Const.XML_ENCODING );
        out.print( XmlHandler.getXMLHeader( Const.XML_ENCODING ) );
        out.print( WebResult.OK.getXml() );
      } else {
        response.setContentType( "text/html;charset=UTF-8" );

        out.println( "<HTML>" );
        out.println( "<HEAD>" );
        out.println( "<TITLE>" + BaseMessages.getString( PKG, "RemoveWorkflowServlet.JobRemoved" ) + "</TITLE>" );
        out.println( "<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">" );
        out.println( "</HEAD>" );
        out.println( "<BODY>" );
        out.println( "<H3>"
          + Encode.forHtml( BaseMessages
          .getString( PKG, "RemoveWorkflowServlet.TheJobWasRemoved", workflowName, id ) ) + "</H3>" );
        out.print( "<a href=\""
          + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + "\">"
          + BaseMessages.getString( PKG, "PipelineStatusServlet.BackToStatusPage" ) + "</a><br>" );
        out.println( "<p>" );
        out.println( "</BODY>" );
        out.println( "</HTML>" );
      }
    } else {
      if ( useXML ) {
        out.println( new WebResult( WebResult.STRING_ERROR, BaseMessages.getString(
          PKG, "RemoveWorkflowServlet.Log.CoundNotFindSpecWorkflow", workflowName ) ) );
      } else {
        out.println( "<H1>"
          + Encode.forHtml( BaseMessages.getString(
          PKG, "RemoveWorkflowServlet.JobRemoved.Log.CoundNotFindWorkflow", workflowName, id ) ) + "</H1>" );
        out.println( "<a href=\""
          + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + "\">"
          + BaseMessages.getString( PKG, "PipelineStatusServlet.BackToStatusPage" ) + "</a><p>" );
        response.setStatus( HttpServletResponse.SC_BAD_REQUEST );
      }
    }
  }

  public String toString() {
    return "Remove workflow servlet";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

}
