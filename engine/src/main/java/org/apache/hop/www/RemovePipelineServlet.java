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
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.www.cache.HopServerStatusCache;
import org.owasp.encoder.Encode;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;


public class RemovePipelineServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static Class<?> PKG = RemovePipelineServlet.class; // for i18n purposes, needed by Translator!!

  private static final long serialVersionUID = 6618979989596401783L;

  public static final String CONTEXT_PATH = "/hop/removePipeline";

  @VisibleForTesting
  private HopServerStatusCache cache = HopServerStatusCache.getInstance();

  public RemovePipelineServlet() {
  }

  public RemovePipelineServlet( PipelineMap pipelineMap ) {
    super( pipelineMap );
  }

  /**
   * <div id="mindtouch">
   * <h1>/hop/removePipeline</h1>
   * <a name="GET"></a>
   * <h2>GET</h2>
   * <p>Removes specified pipeline from HopServer server.</p>
   *
   * <p><b>Example Request:</b><br />
   * <pre function="syntax.xml">
   * GET /hop/removePipeline/?name=dummy-pipeline&xml=Y
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
   * <td>Name of the pipeline to be removed.</td>
   * <td>query</td>
   * </tr>
   * <tr>
   * <td>xml</td>
   * <td>Boolean flag which sets the output format required. Use <code>Y</code> to receive XML response.</td>
   * <td>boolean, optional</td>
   * </tr>
   * <tr>
   * <td>id</td>
   * <td>HopServer pipeline ID of the pipeline to be removed. This parameter is optional when xml=Y is used.</td>
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
      logDebug( BaseMessages.getString( PKG, "PipelineStatusServlet.Log.RemovePipelineRequested" ) );
    }

    String pipelineName = request.getParameter( "name" );
    String id = request.getParameter( "id" );
    boolean useXML = "Y".equalsIgnoreCase( request.getParameter( "xml" ) );

    response.setStatus( HttpServletResponse.SC_OK );

    if ( useXML ) {
      response.setContentType( "text/xml" );
      response.setCharacterEncoding( Const.XML_ENCODING );
    } else {
      response.setContentType( "text/html;charset=UTF-8" );
    }

    response.setCharacterEncoding( "UTF-8" );

    PrintWriter out = response.getWriter();

    // ID is optional...
    //
    IPipelineEngine<PipelineMeta> pipeline;
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

      cache.remove( pipeline.getLogChannelId() );
      HopLogStore.discardLines( pipeline.getLogChannelId(), true );
      getPipelineMap().removePipeline( entry );

      if ( useXML ) {
        response.setContentType( "text/xml" );
        response.setCharacterEncoding( Const.XML_ENCODING );
        out.print( XmlHandler.getXMLHeader( Const.XML_ENCODING ) );
        out.print( WebResult.OK.getXml() );
      } else {
        response.setContentType( "text/html;charset=UTF-8" );

        out.println( "<HTML>" );
        out.println( "<HEAD>" );
        out.println( "<TITLE>" + BaseMessages.getString( PKG, "RemovePipelineServlet.PipelineRemoved" ) + "</TITLE>" );
        out.println( "<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">" );
        out.println( "</HEAD>" );
        out.println( "<BODY>" );
        out.println( "<H3>"
          + Encode.forHtml( BaseMessages.getString(
          PKG, "RemovePipelineServlet.ThePipelineWasRemoved", pipelineName, id ) ) + "</H3>" );
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
          PKG, "PipelineStatusServlet.Log.CoundNotFindSpecPipeline", pipelineName ) ) );
      } else {
        out.println( "<H1>"
          + Encode.forHtml( BaseMessages.getString(
          PKG, "RemovePipelineServlet.PipelineRemoved.Log.CoundNotFindPipeline", pipelineName, id ) ) + "</H1>" );
        out.println( "<a href=\""
          + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + "\">"
          + BaseMessages.getString( PKG, "PipelineStatusServlet.BackToStatusPage" ) + "</a><p>" );
        response.setStatus( HttpServletResponse.SC_BAD_REQUEST );
      }
    }
  }

  public String toString() {
    return "Remove pipeline servlet";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

}
