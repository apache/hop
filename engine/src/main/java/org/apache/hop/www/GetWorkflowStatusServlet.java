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
import org.apache.hop.cluster.HttpUtil;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.www.cache.HopServerStatusCache;
import org.owasp.encoder.Encode;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.nio.charset.Charset;


public class GetWorkflowStatusServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static Class<?> PKG = GetWorkflowStatusServlet.class; // for i18n purposes, needed by Translator!!

  private static final long serialVersionUID = 3634806745372015720L;

  public static final String CONTEXT_PATH = "/hop/workflowStatus";

  private static final byte[] XML_HEADER =
    XmlHandler.getXmlHeader( Const.XML_ENCODING ).getBytes( Charset.forName( Const.XML_ENCODING ) );

  @VisibleForTesting
  HopServerStatusCache cache = HopServerStatusCache.getInstance();

  public GetWorkflowStatusServlet() {
  }

  public GetWorkflowStatusServlet( WorkflowMap workflowMap ) {
    super( workflowMap );
  }

  /**
   * <div id="mindtouch">
   * <h1>/hop/workflowStatus</h1>
   * <a name="GET"></a>
   * <h2>GET</h2>
   * <p>Retrieves status of the specified workflow.
   * Status is returned as HTML or XML output depending on the input parameters.
   * Status contains information about last execution of the workflow.</p>
   *
   * <p><b>Example Request:</b><br />
   * <pre function="syntax.xml">
   * GET /hop/workflowStatus/?name=dummy_job&xml=Y
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
   * <td>Name of the workflow to be used for status generation.</td>
   * <td>query</td>
   * </tr>
   * <tr>
   * <td>xml</td>
   * <td>Boolean flag which defines output format <code>Y</code> forces XML output to be generated.
   * HTML is returned otherwise.</td>
   * <td>boolean, optional</td>
   * </tr>
   * <tr>
   * <td>id</td>
   * <td>HopServer id of the workflow to be used for status generation.</td>
   * <td>query, optional</td>
   * </tr>
   * <tr>
   * <td>from</td>
   * <td>Start line number of the execution log to be included into response.</td>
   * <td>integer, optional</td>
   * </tr>
   * </tbody>
   * </table>
   *
   * <h3>Response Body</h3>
   * <table class="hop-table">
   * <tbody>
   * <tr>
   * <td align="right">element:</td>
   * <td>(custom)</td>
   * </tr>
   * <tr>
   * <td align="right">media types:</td>
   * <td>text/xml, text/html</td>
   * </tr>
   * </tbody>
   * </table>
   * <p>Response XML or HTML response containing details about the workflow specified.
   * If an error occurs during method invocation <code>result</code> field of the response
   * will contain <code>ERROR</code> status.</p>
   *
   * <p><b>Example Response:</b></p>
   * <pre function="syntax.xml">
   * <?xml version="1.0" encoding="UTF-8"?>
   * <jobstatus>
   * <workflowname>dummy_job</workflowname>
   * <id>a4d54106-25db-41c5-b9f8-73afd42766a6</id>
   * <status_desc>Finished</status_desc>
   * <error_desc/>
   * <logging_string>&#x3c;&#x21;&#x5b;CDATA&#x5b;H4sIAAAAAAAAADMyMDTRNzTUNzRXMDC3MjS2MjJQ0FVIKc3NrYzPyk8CsoNLEotKFPLTFEDc1IrU5NKSzPw8Xi4j4nRm5qUrpOaVFFUqRLuE&#x2b;vpGxhKj0y0zL7M4IzUFYieybgWNotTi0pwS2&#x2b;iSotLUWE1iTPNCdrhCGtRsXi4AOMIbLPwAAAA&#x3d;&#x5d;&#x5d;&#x3e;</logging_string>
   * <first_log_line_nr>0</first_log_line_nr>
   * <last_log_line_nr>20</last_log_line_nr>
   * <result>
   * <lines_input>0</lines_input>
   * <lines_output>0</lines_output>
   * <lines_read>0</lines_read>
   * <lines_written>0</lines_written>
   * <lines_updated>0</lines_updated>
   * <lines_rejected>0</lines_rejected>
   * <lines_deleted>0</lines_deleted>
   * <nr_errors>0</nr_errors>
   * <nr_files_retrieved>0</nr_files_retrieved>
   * <entry_nr>0</entry_nr>
   * <result>Y</result>
   * <exit_status>0</exit_status>
   * <is_stopped>N</is_stopped>
   * <log_channel_id/>
   * <log_text>null</log_text>
   * <result-file></result-file>
   * <result-rows></result-rows>
   * </result>
   * </jobstatus>
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
      logDebug( BaseMessages.getString( PKG, "GetWorkflowStatusServlet.Log.WorkflowStatusRequested" ) );
    }

    String workflowName = request.getParameter( "name" );
    String id = request.getParameter( "id" );
    String root = request.getRequestURI() == null ? StatusServletUtils.HOP_ROOT
      : request.getRequestURI().substring( 0, request.getRequestURI().indexOf( CONTEXT_PATH ) );
    String prefix = isJettyMode() ? StatusServletUtils.STATIC_PATH : root + StatusServletUtils.RESOURCES_PATH;
    boolean useXML = "Y".equalsIgnoreCase( request.getParameter( "xml" ) );
    int startLineNr = Const.toInt( request.getParameter( "from" ), 0 );

    response.setStatus( HttpServletResponse.SC_OK );

    if ( useXML ) {
      response.setContentType( "text/xml" );
      response.setCharacterEncoding( Const.XML_ENCODING );
    } else {
      response.setContentType( "text/html;charset=UTF-8" );
    }

    // ID is optional...
    //
    IWorkflowEngine<WorkflowMeta> workflow;
    HopServerObjectEntry entry;
    if ( Utils.isEmpty( id ) ) {
      // get the first workflow that matches...
      //
      entry = getWorkflowMap().getFirstHopServerObjectEntry( workflowName );
      if ( entry == null ) {
        workflow = null;
      } else {
        id = entry.getId();
        workflow = getWorkflowMap().getWorkflow( entry );
      }
    } else {
      // Actually, just providing the ID should be enough to identify the workflow
      //
      if ( Utils.isEmpty( workflowName ) ) {
        // Take the ID into account!
        //
        workflow = getWorkflowMap().findWorkflow( id );
      } else {
        entry = new HopServerObjectEntry( workflowName, id );
        workflow = getWorkflowMap().getWorkflow( entry );
        if ( workflow != null ) {
          workflowName = workflow.getWorkflowName();
        }
      }
    }

    if ( workflow != null ) {

      if ( useXML ) {
        try {
          OutputStream out = null;
          byte[] data = null;
          String logId = workflow.getLogChannelId();
          boolean finishedOrStopped = workflow.isFinished() || workflow.isStopped();
          if ( finishedOrStopped && ( data = cache.get( logId, startLineNr ) ) != null ) {
            response.setContentLength( XML_HEADER.length + data.length );
            out = response.getOutputStream();
            out.write( XML_HEADER );
            out.write( data );
            out.flush();
          } else {
            int lastLineNr = HopLogStore.getLastBufferLineNr();
            String logText = getLogText( workflow, startLineNr, lastLineNr );

            response.setContentType( "text/xml" );
            response.setCharacterEncoding( Const.XML_ENCODING );

            SlaveServerWorkflowStatus jobStatus = new SlaveServerWorkflowStatus( workflowName, id, workflow.getStatusDescription() );
            jobStatus.setFirstLoggingLineNr( startLineNr );
            jobStatus.setLastLoggingLineNr( lastLineNr );
            jobStatus.setLogDate( workflow.getExecutionStartDate() );

            // The log can be quite large at times, we are going to putIfAbsent a base64 encoding around a compressed
            // stream


            // of bytes to handle this one.
            String loggingString = HttpUtil.encodeBase64ZippedString( logText );
            jobStatus.setLoggingString( loggingString );

            // Also set the result object...
            //
            jobStatus.setResult( workflow.getResult() ); // might be null


            String xml = jobStatus.getXml();
            data = xml.getBytes( Charset.forName( Const.XML_ENCODING ) );
            out = response.getOutputStream();
            response.setContentLength( XML_HEADER.length + data.length );
            out.write( XML_HEADER );
            out.write( data );
            out.flush();
            if ( finishedOrStopped && ( jobStatus.isFinished() || jobStatus.isStopped() ) && logId != null ) {
              cache.put( logId, xml, startLineNr );
            }
          }
          response.flushBuffer();
        } catch ( HopException e ) {
          throw new ServletException( "Unable to get the workflow status in XML format", e );
        }
      } else {

        PrintWriter out = response.getWriter();

        int lastLineNr = HopLogStore.getLastBufferLineNr();
        int tableBorder = 0;

        response.setContentType( "text/html" );

        out.println( "<HTML>" );
        out.println( "<HEAD>" );
        out
          .println( "<TITLE>"
            + BaseMessages.getString( PKG, "GetWorkflowStatusServlet.HopWorkflowStatus" ) + "</TITLE>" );
        if ( EnvUtil.getSystemProperty( Const.HOP_CARTE_REFRESH_STATUS, "N" ).equalsIgnoreCase( "Y" ) ) {
          out.println( "<META http-equiv=\"Refresh\" content=\"10;url="
            + convertContextPath( GetWorkflowStatusServlet.CONTEXT_PATH ) + "?name="
            + URLEncoder.encode( Const.NVL( workflowName, "" ), "UTF-8" ) + "&id=" + URLEncoder.encode( id, "UTF-8" )
            + "\">" );
        }
        out.println( "<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">" );
        if ( isJettyMode() ) {
          out.println( "<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/css/hop-server.css\" />" );
        }
        out.println( "</HEAD>" );
        out.println( "<BODY style=\"overflow: auto;\">" );
        out.println( "<div class=\"row\" id=\"pucHeader\">" );
        out.println( "<div class=\"workspaceHeading\" style=\"padding: 0px 0px 0px 10px;\">" + Encode.forHtml( BaseMessages.getString( PKG, "GetWorkflowStatusServlet.WorkflowStatus", workflowName ) ) + "</div>" );
        out.println( "</div>" );

        try {
          out.println( "<div class=\"row\" style=\"padding: 0px 0px 0px 30px\">" );
          out.println( "<div class=\"row\" style=\"padding-top: 30px;\">" );
          out.print( "<a href=\"" + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + "\">" );
          out.print( "<img src=\"" + prefix + "/images/back.svg\" style=\"margin-right: 5px; width: 16px; height: 16px; vertical-align: middle;\">" );
          out.print( BaseMessages.getString( PKG, "HopServerStatusServlet.BackToHopServerStatus" ) + "</a>" );
          out.println( "</div>" );
          out.println( "<div class=\"row\" style=\"padding: 30px 0px 75px 0px; display: table;\">" );
          out.println( "<div style=\"display: table-row;\">" );
          out.println( "<div style=\"padding: 0px 30px 0px 0px; width: 60px; display: table-cell; vertical-align: top;\">" );
          out.println( "<img src=\"" + prefix + "/images/workflow.svg\" style=\"width: 60px; height: 60px;\"></img>" );
          out.println( "</div>" );
          out.println( "<div style=\"vertical-align: top; display: table-cell;\">" );
          out.println( "<table style=\"border-collapse: collapse;\" border=\"" + tableBorder + "\">" );
          out.print(
            "<tr class=\"cellTableRow\" style=\"border: solid; border-width: 1px 0; border-top: none; border-color: #E3E3E3; font-size: 12; text-align: left;\"> <th style=\"font-weight: normal; "
              + "padding: 8px 10px 10px 10px\" class=\"cellTableHeader\">"
              + BaseMessages.getString( PKG, "PipelineStatusServlet.ServerObjectId" ) + "</th> <th style=\"font-weight: normal; padding: 8px 10px 10px 10px\" class=\"cellTableHeader\">"
              + BaseMessages.getString( PKG, "PipelineStatusServlet.PipelineStatus" ) + "</th> <th style=\"font-weight: normal; padding: 8px 10px 10px 10px\" class=\"cellTableHeader\">"
              + BaseMessages.getString( PKG, "PipelineStatusServlet.StartDate" ) + "</th> </tr>" );
          out.print( "<tr class=\"cellTableRow\" style=\"border: solid; border-width: 1px 0; border-bottom: none; font-size: 12; text-align:left\">" );
          out.print( "<td style=\"padding: 8px 10px 10px 10px\" class=\"cellTableCell cellTableFirstColumn\">" + Const.NVL( Encode.forHtml( id ), "" ) + "</td>" );
          out.print( "<td style=\"padding: 8px 10px 10px 10px\" class=\"cellTableCell\" id=\"statusColor\" style=\"font-weight: bold;\">" + workflow.getStatusDescription() + "</td>" );
          String dateStr = XmlHandler.date2string( workflow.getExecutionStartDate() );
          out.print( "<td style=\"padding: 8px 10px 10px 10px\" class=\"cellTableCell cellTableLastColumn\">" +
            ( dateStr!=null ? dateStr.substring( 0, dateStr.indexOf( ' ' ) ) : "" ) + "</td>" );
          out.print( "</tr>" );
          out.print( "</table>" );
          out.print( "</div>" );
          out.println( "<div style=\"padding: 0px 0px 0px 20px; width: 90px; display: table-cell; vertical-align: top;\">" );
          out.print( "<div style=\"display: block; margin-left: auto; margin-right: auto; padding: 5px 0px;\">" );
          out.print( "<a target=\"_blank\" href=\""
            + convertContextPath( GetWorkflowStatusServlet.CONTEXT_PATH ) + "?name="
            + URLEncoder.encode( workflowName, "UTF-8" ) + "&id=" + URLEncoder.encode( id, "UTF-8" ) + "&xml=y\">"
            + "<img src=\"" + prefix + "/images/view-as-xml.svg\" style=\"display: block; margin: auto; width: 22px; height: 22px;\"></a>" );
          out.print( "</div>" );
          out.println( "<div style=\"text-align: center; padding-top: 12px; font-size: 12px;\">" );
          out.print( "<a target=\"_blank\" href=\""
            + convertContextPath( GetWorkflowStatusServlet.CONTEXT_PATH ) + "?name="
            + URLEncoder.encode( workflowName, "UTF-8" ) + "&id=" + URLEncoder.encode( id, "UTF-8" ) + "&xml=y\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.ShowAsXml" ) + "</a>" );
          out.print( "</div>" );
          out.print( "</div>" );
          out.print( "</div>" );
          out.print( "</div>" );

          out.print( "<div class=\"row\" style=\"padding: 0px 0px 75px 0px;\">" );
          out.print( "<div class=\"workspaceHeading\">Canvas preview</div>" );
          // Show workflow image?
          //
          Point max = workflow.getWorkflowMeta().getMaximum();
          //max.x += 20;
          max.y += 20;
          out
            .print( "<iframe height=\""
              + max.y + "\" width=\"" + 875 + "\" seamless src=\""
              + convertContextPath( GetWorkflowImageServlet.CONTEXT_PATH ) + "?name="
              + URLEncoder.encode( workflowName, "UTF-8" ) + "&id=" + URLEncoder.encode( id, "UTF-8" )
              + "\"></iframe>" );
          out.print( "</div>" );

          // Put the logging below that.

          out.print( "<div class=\"row\" style=\"padding: 0px 0px 30px 0px;\">" );
          out.print( "<div class=\"workspaceHeading\">Workflow log</div>" );
          out.println( "<textarea id=\"joblog\" cols=\"120\" rows=\"20\" wrap=\"off\" "
            + "name=\"Workflow log\" readonly=\"readonly\" style=\"height: auto;\">"
            + Encode.forHtml( getLogText( workflow, startLineNr, lastLineNr ) ) + "</textarea>" );
          out.print( "</div>" );

          out.println( "<script type=\"text/javascript\">" );
          out.println( "element = document.getElementById( 'statusColor' );" );
          out.println( "if( element.innerHTML == 'Running' || element.innerHTML == 'Finished' ){" );
          out.println( "element.style.color = '#009900';" );
          out.println( "} else if( element.innerHTML == 'Stopped' ) {" );
          out.println( "element.style.color = '#7C0B2B';" );
          out.println( "} else {" );
          out.println( "element.style.color = '#F1C40F';" );
          out.println( "}" );
          out.println( "</script>" );
          out.println( "<script type=\"text/javascript\"> " );
          out.println( "  joblog.scrollTop=joblog.scrollHeight; " );
          out.println( "</script> " );
        } catch ( Exception ex ) {
          out.println( "<pre>" );
          out.println( Encode.forHtml( Const.getStackTracker( ex ) ) );
          out.println( "</pre>" );
        }

        out.println( "</div>" );
        out.println( "</BODY>" );
        out.println( "</HTML>" );
      }
    } else {
      PrintWriter out = response.getWriter();
      if ( useXML ) {
        out.println( new WebResult( WebResult.STRING_ERROR, BaseMessages.getString(
          PKG, "StartWorkflowServlet.Log.SpecifiedWorkflowNotFound", workflowName, id ) ) );
      } else {
        out.println( "<H1>Workflow " + Encode.forHtml( "\'" + workflowName + "\'" ) + " could not be found.</H1>" );
        out.println( "<a href=\""
          + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + "\">"
          + BaseMessages.getString( PKG, "WorkflowStatusServlet.BackToStatusPage" ) + "</a><p>" );
      }
    }
  }

  public String toString() {
    return "Workflow Status IHandler";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

  private String getLogText( IWorkflowEngine<WorkflowMeta> workflow, int startLineNr, int lastLineNr ) throws HopException {
    try {
      return HopLogStore.getAppender().getBuffer(
        workflow.getLogChannel().getLogChannelId(), false, startLineNr, lastLineNr ).toString();
    } catch ( OutOfMemoryError error ) {
      throw new HopException( "Log string is too long" );
    }
  }
}
