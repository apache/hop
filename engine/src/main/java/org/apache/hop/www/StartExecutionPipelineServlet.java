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

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.owasp.encoder.Encode;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;


public class StartExecutionPipelineServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static Class<?> PKG = StartExecutionPipelineServlet.class; // for i18n purposes, needed by Translator!!

  private static final long serialVersionUID = 3634806745372015720L;
  public static final String CONTEXT_PATH = "/hop/startExec";

  public StartExecutionPipelineServlet() {
  }

  public StartExecutionPipelineServlet( PipelineMap pipelineMap ) {
    super( pipelineMap );
  }

  /**
   * <div id="mindtouch">
   * <h1>/hop/startExec</h1>
   * <a name="GET"></a>
   * <h2>GET</h2>
   * <p>Starts pipeline. If the pipeline is not ready, an error is returned.</p>
   *
   * <p><b>Example Request:</b><br />
   * <pre function="syntax.xml">
   * GET /hop/startExec/?name=dummy-pipeline&xml=Y
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
   * <td>Name of the pipeline to be executed.</td>
   * <td>query</td>
   * </tr>
   * <tr>
   * <td>xml</td>
   * <td>Boolean flag which sets the output format required. Use <code>Y</code> to receive XML response.</td>
   * <td>boolean, optional</td>
   * </tr>
   * <tr>
   * <td>id</td>
   * <td>HopServer pipeline ID of the pipeline to be executed. This parameter is optional when xml=Y is used.</td>
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
      logDebug( "Start execution of pipeline requested" );
    }
    response.setStatus( HttpServletResponse.SC_OK );

    String pipelineName = request.getParameter( "name" );
    String id = request.getParameter( "id" );
    boolean useXML = "Y".equalsIgnoreCase( request.getParameter( "xml" ) );

    PrintWriter out = response.getWriter();
    if ( useXML ) {
      response.setContentType( "text/xml" );
      out.print( XmlHandler.getXMLHeader( Const.XML_ENCODING ) );
    } else {
      response.setContentType( "text/html;charset=UTF-8" );
      out.println( "<HTML>" );
      out.println( "<HEAD>" );
      out.println( "<TITLE>"
        + BaseMessages.getString( PKG, "PrepareExecutionPipelineServlet.PipelinePrepareExecution" ) + "</TITLE>" );
      out.println( "<META http-equiv=\"Refresh\" content=\"2;url="
        + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + "?name="
        + URLEncoder.encode( pipelineName, "UTF-8" ) + "\">" );
      out.println( "<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">" );
      out.println( "</HEAD>" );
      out.println( "<BODY>" );
    }

    try {
      // ID is optional...
      //
      Pipeline pipeline;
      HopServerObjectEntry entry;
      if ( Utils.isEmpty( id ) ) {
        // get the first pipeline that matches...
        //
        entry = getPipelineMap().getFirstServerObjectEntry( pipelineName );
        if ( entry == null ) {
          pipeline = null;
        } else {
          id = entry.getId();
          pipeline = getPipelineMap().getPipeline( entry );
        }
      } else {
        // Take the ID into account!
        //
        entry = new HopServerObjectEntry( pipelineName, id );
        pipeline = getPipelineMap().getPipeline( entry );
      }

      if ( pipeline != null ) {
        if ( pipeline.isReadyToStart() ) {
          startThreads( pipeline );

          if ( useXML ) {
            out.println( WebResult.OK.getXml() );
          } else {
            out
              .println( "<H1>Pipeline "
                + Encode.forHtml( "\'" + pipelineName + "\'" ) + " has been executed.</H1>" );
            out.println( "<a href=\""
              + convertContextPath( GetPipelineStatusServlet.CONTEXT_PATH ) + "?name="
              + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id=" + URLEncoder.encode( id, "UTF-8" )
              + "\">Back to the pipeline status page</a><p>" );
          }
        } else {
          String message =
            "The specified pipeline ["
              + pipelineName + "] is not ready to be started. (Was not prepared for execution)";
          if ( useXML ) {
            out.println( new WebResult( WebResult.STRING_ERROR, message ) );
          } else {
            out.println( "<H1>" + Encode.forHtml( message ) + "</H1>" );
            out.println( "<a href=\""
              + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + "\">"
              + BaseMessages.getString( PKG, "PipelineStatusServlet.BackToStatusPage" ) + "</a><p>" );
          }
        }
      } else {
        if ( useXML ) {
          out.println( new WebResult( WebResult.STRING_ERROR, BaseMessages.getString(
            PKG, "PipelineStatusServlet.Log.CoundNotFindSpecPipeline", pipelineName ) ) );
        } else {
          out.println( "<H1>"
            + Encode.forHtml( BaseMessages.getString(
            PKG, "PipelineStatusServlet.Log.CoundNotFindPipeline", pipelineName ) ) + "</H1>" );
          out.println( "<a href=\""
            + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + "\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.BackToStatusPage" ) + "</a><p>" );
        }
      }
    } catch ( Exception ex ) {
      if ( useXML ) {
        out.println( new WebResult(
          WebResult.STRING_ERROR, "Unexpected error during pipeline execution preparation:"
          + Const.CR + Const.getStackTracker( ex ) ) );
      } else {
        out.println( "<p>" );
        out.println( "<pre>" );
        out.println( Encode.forHtml( Const.getStackTracker( ex ) ) );
        out.println( "</pre>" );
      }
    }

    if ( !useXML ) {
      out.println( "<p>" );
      out.println( "</BODY>" );
      out.println( "</HTML>" );
    }
  }

  public String toString() {
    return "Start pipeline";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  protected void startThreads( Pipeline pipeline ) throws HopException {
    pipeline.startThreads();
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

}
