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
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineComponent;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformStatus;
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
import java.util.Date;


public class GetPipelineStatusServlet extends BaseHttpServlet implements IHopServerPlugin {

  private static Class<?> PKG = GetPipelineStatusServlet.class; // for i18n purposes, needed by Translator!!

  private static final long serialVersionUID = 3634806745372015720L;

  public static final String CONTEXT_PATH = "/hop/pipelineStatus";

  public static final String SEND_RESULT = "sendResult";

  private static final byte[] XML_HEADER =
    XmlHandler.getXMLHeader( Const.XML_ENCODING ).getBytes( Charset.forName( Const.XML_ENCODING ) );

  @VisibleForTesting
  HopServerStatusCache cache = HopServerStatusCache.getInstance();

  public GetPipelineStatusServlet() {
  }

  public GetPipelineStatusServlet( PipelineMap pipelineMap ) {
    super( pipelineMap );
  }

  /**
   * <div id="mindtouch">
   * <h1>/hop/pipelineStatus</h1>
   * <a name="GET"></a>
   * <h2>GET</h2>
   * <p>Retrieves status of the specified pipeline. Status is returned as HTML or XML output
   * depending on the input parameters. Status contains information about last execution of the pipeline.</p>
   * <p><b>Example Request:</b><br />
   * <pre function="syntax.xml">
   * GET /hop/pipelineStatus/?name=dummy-pipeline&xml=Y
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
   * <td>Name of the pipeline to be used for status generation.</td>
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
   * <td>HopServer id of the pipeline to be used for status generation.</td>
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
   * <p> Response XML or HTML response containing details about the pipeline specified.
   * If an error occurs during method invocation <code>result</code> field of the response
   * will contain <code>ERROR</code> status.</p>
   *
   * <p><b>Example Response:</b></p>
   * <pre function="syntax.xml">
   * <?xml version="1.0" encoding="UTF-8"?>
   * <pipeline-status>
   * <pipeline_name>dummy-pipeline</pipeline_Name>
   * <id>c56961b2-c848-49b8-abde-76c8015e29b0</id>
   * <status_desc>Stopped</status_desc>
   * <error_desc/>
   * <paused>N</paused>
   * <transform_status_list>
   * <transform_status><transformName>Dummy &#x28;do nothing&#x29;</transformName>
   * <copy>0</copy><linesRead>0</linesRead>
   * <linesWritten>0</linesWritten><linesInput>0</linesInput>
   * <linesOutput>0</linesOutput><linesUpdated>0</linesUpdated>
   * <linesRejected>0</linesRejected><errors>0</errors>
   * <statusDescription>Stopped</statusDescription><seconds>0.0</seconds>
   * <speed>-</speed><priority>-</priority><stopped>Y</stopped>
   * <paused>N</paused>
   * </transform_status>
   * </transform_status_list>
   * <first_log_line_nr>0</first_log_line_nr>
   * <last_log_line_nr>37</last_log_line_nr>
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
   * <is_stopped>Y</is_stopped>
   * <log_channel_id>10e2c832-07da-409a-a5ba-4b90a234e957</log_channel_id>
   * <log_text/>
   * <result-file></result-file>
   * <result-rows></result-rows>
   * </result>
   * <logging_string>&#x3c;&#x21;&#x5b;CDATA&#x5b;H4sIAAAAAAAAADMyMDTRNzTUNzJRMDSyMrC0MjFV0FVIKc3NrdQtKUrMKwbyXDKLCxJLkjMy89IViksSi0pSUxTS8osUwPJARm5iSWZ&#x2b;nkI0kq5YXi4AQVH5bFoAAAA&#x3d;&#x5d;&#x5d;&#x3e;</logging_string>
   * </pipeline-status>
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
      logDebug( BaseMessages.getString( PKG, "PipelineStatusServlet.Log.PipelineStatusRequested" ) );
    }

    String pipelineName = request.getParameter( "name" );
    String id = request.getParameter( "id" );
    String root = request.getRequestURI() == null ? StatusServletUtils.PENTAHO_ROOT
      : request.getRequestURI().substring( 0, request.getRequestURI().indexOf( CONTEXT_PATH ) );
    String prefix = isJettyMode() ? StatusServletUtils.STATIC_PATH : root + StatusServletUtils.RESOURCES_PATH;
    boolean useXML = "Y".equalsIgnoreCase( request.getParameter( "xml" ) );
    int startLineNr = Const.toInt( request.getParameter( "from" ), 0 );

    response.setStatus( HttpServletResponse.SC_OK );

    if ( useXML ) {
      response.setContentType( "text/xml" );
      response.setCharacterEncoding( Const.XML_ENCODING );
    } else {
      response.setCharacterEncoding( "UTF-8" );
      response.setContentType( "text/html;charset=UTF-8" );

    }

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
      if ( useXML ) {
        try {
          OutputStream out = null;
          byte[] data = null;
          String logId = pipeline.getLogChannelId();
          boolean finishedOrStopped = pipeline.isFinished() || pipeline.isStopped();
          boolean sendResultXmlWithStatus = "Y".equalsIgnoreCase( request.getParameter( SEND_RESULT ) );
          boolean dontUseCache = sendResultXmlWithStatus;
          if ( finishedOrStopped && ( data = cache.get( logId, startLineNr ) ) != null && !dontUseCache ) {
            response.setContentLength( XML_HEADER.length + data.length );
            out = response.getOutputStream();
            out.write( XML_HEADER );
            out.write( data );
            out.flush();
          } else {
            int lastLineNr = HopLogStore.getLastBufferLineNr();

            String logText = getLogText( pipeline, startLineNr, lastLineNr );

            response.setContentType( "text/xml" );
            response.setCharacterEncoding( Const.XML_ENCODING );

            SlaveServerPipelineStatus pipelineStatus = new SlaveServerPipelineStatus( pipelineName, entry.getId(), pipeline.getStatusDescription() );
            pipelineStatus.setFirstLoggingLineNr( startLineNr );
            pipelineStatus.setLastLoggingLineNr( lastLineNr );
            pipelineStatus.setLogDate( new Date() );
            pipelineStatus.setExecutionStartDate( pipeline.getExecutionStartDate() );
            pipelineStatus.setExecutionEndDate( pipeline.getExecutionEndDate() );

            for ( IEngineComponent component : pipeline.getComponents()) {
              if ( ( component.isRunning() ) || ( component.getStatus() != ComponentExecutionStatus.STATUS_EMPTY ) ) {
                TransformStatus transformStatus = new TransformStatus( component );
                pipelineStatus.getTransformStatusList().add( transformStatus );
              }
            }

            // The log can be quite large at times, we are going to putIfAbsent a base64 encoding around a compressed
            // stream
            // of bytes to handle this one.
            String loggingString = HttpUtil.encodeBase64ZippedString( logText );
            pipelineStatus.setLoggingString( loggingString );
            //        pipelineStatus.setLoggingUncompressedSize( logText.length() );

            // Also set the result object...
            //
            pipelineStatus.setResult( pipeline.getResult() );

            // Is the pipeline paused?
            //
            pipelineStatus.setPaused( pipeline.isPaused() );

            // Send the result back as XML
            //
            String xml = pipelineStatus.getXML( sendResultXmlWithStatus );
            data = xml.getBytes( Charset.forName( Const.XML_ENCODING ) );
            out = response.getOutputStream();
            response.setContentLength( XML_HEADER.length + data.length );
            out.write( XML_HEADER );
            out.write( data );
            out.flush();
            if ( finishedOrStopped && ( pipelineStatus.isFinished() || pipelineStatus.isStopped() ) && logId != null && !dontUseCache ) {
              cache.put( logId, xml, startLineNr );
            }
          }
          response.flushBuffer();
        } catch ( HopException e ) {
          throw new ServletException( "Unable to get the pipeline status in XML format", e );
        }

      } else {
        PrintWriter out = response.getWriter();

        int lastLineNr = HopLogStore.getLastBufferLineNr();
        int tableBorder = 0;

        response.setContentType( "text/html;charset=UTF-8" );

        out.println( "<HTML>" );
        out.println( "<HEAD>" );
        out.println( "<TITLE>"
          + BaseMessages.getString( PKG, "PipelineStatusServlet.HopPipelineStatus" ) + "</TITLE>" );
        if ( EnvUtil.getSystemProperty( Const.HOP_CARTE_REFRESH_STATUS, "N" ).equalsIgnoreCase( "Y" ) ) {
          out.println( "<META http-equiv=\"Refresh\" content=\"10;url="
            + convertContextPath( CONTEXT_PATH ) + "?name=" + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id="
            + URLEncoder.encode( id, "UTF-8" ) + "\">" );
        }
        out.println( "<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">" );

        if ( isJettyMode() ) {
          out.println( "<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/css/carte.css\" />" );
        } else {
          out.print( StatusServletUtils.getPentahoStyles( root ) );
        }

        out.println( "</HEAD>" );
        out.println( "<BODY style=\"overflow: auto;\">" );
        out.println( "<div class=\"row\" id=\"pucHeader\">" );
        out.println( "<div class=\"workspaceHeading\" style=\"padding: 0px 0px 0px 10px;\">"
          + Encode.forHtml( BaseMessages.getString( PKG, "PipelineStatusServlet.TopPipelineStatus", pipelineName ) )
          + "</div>" );
        out.println( "</div>" );

        try {
          out.println( "<div class=\"row\" style=\"padding: 0px 0px 0px 30px\">" );
          out.println( "<div class=\"row\" style=\"padding-top: 30px;\">" );
          out.print( "<a href=\"" + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + "\">" );
          out.print( "<img src=\"" + prefix + "/images/back.svg\" style=\"margin-right: 5px; width: 16px; height: 16px; vertical-align: middle;\">" );
          out.print( BaseMessages.getString( PKG, "CarteStatusServlet.BackToCarteStatus" ) + "</a>" );
          out.println( "</div>" );
          out.println( "<div class=\"row\" style=\"padding: 30px 0px 75px 0px; display: table;\">" );
          out.println( "<div style=\"display: table-row;\">" );
          out.println( "<div style=\"padding: 0px 30px 0px 0px; width: 60px; display: table-cell; vertical-align: top;\">" );
          out.println( "<img src=\"" + prefix + "/images/pipeline.svg\" style=\"width: 60px; height: 60px;\"></img>" );
          out.println( "</div>" );
          out.println( "<div style=\"vertical-align: top; display: table-cell;\">" );
          out.println( "<table style=\"border-collapse: collapse;\" border=\"" + tableBorder + "\">" );
          out.print(
            "<tr class=\"cellTableRow\" style=\"border: solid; border-width: 1px 0; border-top: none; border-color: #E3E3E3; font-size: 12; text-align: left;\"> <th style=\"font-weight: normal; "
              + "padding: 8px 10px 10px 10px\" class=\"cellTableHeader\">"
              + BaseMessages.getString( PKG, "PipelineStatusServlet.CarteObjectId" ) + "</th> <th style=\"font-weight: normal; padding: 8px 10px 10px 10px\" class=\"cellTableHeader\">"
              + BaseMessages.getString( PKG, "PipelineStatusServlet.PipelineStatus" ) + "</th> <th style=\"font-weight: normal; padding: 8px 10px 10px 10px\" class=\"cellTableHeader\">"
              + BaseMessages.getString( PKG, "PipelineStatusServlet.LastLogDate" ) + "</th> </tr>" );
          out.print( "<tr class=\"cellTableRow\" style=\"border: solid; border-width: 1px 0; border-bottom: none; font-size: 12; text-align: left;\">" );
          out.print( "<td style=\"padding: 8px 10px 10px 10px\" class=\"cellTableCell cellTableFirstColumn\">" + Encode.forHtml( id ) + "</td>" );
          out.print( "<td style=\"padding: 8px 10px 10px 10px\" class=\"cellTableCell\" id=\"statusColor\" style=\"font-weight: bold;\">" + Encode.forHtml( pipeline.getStatusDescription() ) + "</td>" );
          String dateStr = XmlHandler.date2string( pipeline.getExecutionStartDate() );
          out.print( "<td style=\"padding: 8px 10px 10px 10px\" class=\"cellTableCell cellTableLastColumn\">" + dateStr.substring( 0, dateStr.indexOf( ' ' ) ) + "</td>" );
          out.print( "</tr>" );
          out.print( "</table>" );
          out.print( "</div>" );
          out.println( "<div style=\"padding: 0px 0px 0px 20px; width: 90px; display: table-cell; vertical-align: top;\">" );
          out.print( "<div style=\"display: block; margin-left: auto; margin-right: auto; padding: 5px 0px;\">" );
          out.print( "<a target=\"_blank\" href=\""
            + convertContextPath( GetPipelineStatusServlet.CONTEXT_PATH ) + "?name="
            + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id=" + URLEncoder.encode( id, "UTF-8" ) + "&xml=y\">"
            + "<img src=\"" + prefix + "/images/view-as-xml.svg\" style=\"display: block; margin: auto; width: 22px; height: 22px;\"></a>" );
          out.print( "</div>" );
          out.println( "<div style=\"text-align: center; padding-top: 12px; font-size: 12px;\">" );
          out.print( "<a target=\"_blank\" href=\""
            + convertContextPath( GetPipelineStatusServlet.CONTEXT_PATH ) + "?name="
            + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id=" + URLEncoder.encode( id, "UTF-8" ) + "&xml=y\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.ShowAsXml" ) + "</a>" );
          out.print( "</div>" );
          out.print( "</div>" );
          out.print( "</div>" );
          out.print( "</div>" );

          out.print( "<div class=\"row\" style=\"padding: 0px 0px 75px 0px;\">" );
          out.print( "<div class=\"workspaceHeading\" style=\"padding: 0px 0px 30px 0px;\">Transform detail</div>" );
          out.println( "<table class=\"pentaho-table\" border=\"" + tableBorder + "\">" );
          out.print( "<tr class=\"cellTableRow\"> <th class=\"cellTableHeader\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.TransformName" ) + "</th> <th class=\"cellTableHeader\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.CopyNr" ) + "</th> <th class=\"cellTableHeader\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.Read" ) + "</th> <th class=\"cellTableHeader\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.Written" ) + "</th> <th class=\"cellTableHeader\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.Input" ) + "</th> <th class=\"cellTableHeader\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.Output" ) + "</th> <th class=\"cellTableHeader\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.Updated" ) + "</th> <th class=\"cellTableHeader\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.Rejected" ) + "</th> <th class=\"cellTableHeader\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.Errors" ) + "</th> <th class=\"cellTableHeader\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.Active" ) + "</th> <th class=\"cellTableHeader\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.Time" ) + "</th> <th class=\"cellTableHeader\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.Speed" ) + "</th> <th class=\"cellTableHeader\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.prinout" ) + "</th> </tr>" );

          boolean evenRow = true;
          for ( IEngineComponent component : pipeline.getComponents() ) {
            if ( ( component.isRunning() ) || component.getStatus() != ComponentExecutionStatus.STATUS_EMPTY ) {
              TransformStatus transformStatus = new TransformStatus( component );
              boolean snif = false;
              String htmlString = "";
              if ( component.isRunning() && !component.isStopped() && !component.isPaused() ) {
                snif = true;
                String sniffLink =
                  " <a href=\""
                    + convertContextPath( SniffTransformServlet.CONTEXT_PATH ) + "?pipeline="
                    + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id=" + URLEncoder.encode( id, "UTF-8" )
                    + "&lines=50" + "&copynr=" + component.getCopyNr() + "&type=" + SniffTransformServlet.TYPE_OUTPUT
                    + "&transform=" + URLEncoder.encode( component.getName(), "UTF-8" ) + "\">"
                    + Encode.forHtml( transformStatus.getTransformName() ) + "</a>";
                transformStatus.setTransformName( sniffLink );
              }

              String rowClass = evenRow ? "cellTableEvenRow" : "cellTableOddRow";
              String cellClass = evenRow ? "cellTableEvenRowCell" : "cellTableOddRowCell";
              htmlString = "<tr class=\"" + rowClass + "\"><td class=\"cellTableCell cellTableFirstColumn " + cellClass + "\">" + transformStatus.getTransformName() + "</td>"
                + "<td class=\"cellTableCell " + cellClass + "\">" + transformStatus.getCopy() + "</td>"
                + "<td class=\"cellTableCell " + cellClass + "\">" + transformStatus.getLinesRead() + "</td>"
                + "<td class=\"cellTableCell " + cellClass + "\">" + transformStatus.getLinesWritten() + "</td>"
                + "<td class=\"cellTableCell " + cellClass + "\">" + transformStatus.getLinesInput() + "</td>"
                + "<td class=\"cellTableCell " + cellClass + "\">" + transformStatus.getLinesOutput() + "</td>"
                + "<td class=\"cellTableCell " + cellClass + "\">" + transformStatus.getLinesUpdated() + "</td>"
                + "<td class=\"cellTableCell " + cellClass + "\">" + transformStatus.getLinesRejected() + "</td>"
                + "<td class=\"cellTableCell " + cellClass + "\">" + transformStatus.getErrors() + "</td>"
                + "<td class=\"cellTableCell " + cellClass + "\">" + transformStatus.getStatusDescription() + "</td>"
                + "<td class=\"cellTableCell " + cellClass + "\">" + transformStatus.getSeconds() + "</td>"
                + "<td class=\"cellTableCell " + cellClass + "\">" + transformStatus.getSpeed() + "</td>"
                + "<td class=\"cellTableCell cellTableLastColumn " + cellClass + "\">" + transformStatus.getPriority() + "</td></tr>";
              evenRow = !evenRow;
              out.print( htmlString );
            }
          }
          out.println( "</table>" );
          out.println( "</div>" );

          out.print( "<div class=\"row\" style=\"padding: 0px 0px 75px 0px;\">" );
          out.print( "<div class=\"workspaceHeading\" style=\"padding: 0px 0px 30px 0px;\">Canvas preview</div>" );
          // Get the pipeline image
          //
          // out.print("<a href=\"" + convertContextPath(GetPipelineImageServlet.CONTEXT_PATH) + "?name=" +
          // URLEncoder.encode(pipelineName, "UTF-8") + "&id="+id+"\">"
          // + BaseMessages.getString(PKG, "PipelineStatusServlet.GetPipelineImage") + "</a>");
          Point max = pipeline.getSubject().getMaximum();
          max.x += 20;
          max.y += 20;
          out.print( "<iframe height=\""
            + max.y + "\" width=\"" + 875 + "\" seamless src=\""
            + convertContextPath( GetPipelineImageServlet.CONTEXT_PATH ) + "?name="
            + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id=" + URLEncoder.encode( id, "UTF-8" )
            + "\"></iframe>" );
          out.print( "</div>" );

          // Put the logging below that.
          out.print( "<div class=\"row\" style=\"padding: 0px 0px 30px 0px;\">" );
          out.print( "<div class=\"workspaceHeading\" style=\"padding: 0px 0px 30px 0px;\">Pipeline log</div>" );
          out
            .println( "<textarea id=\"pipelinelog\" cols=\"120\" rows=\"20\" "
              + "wrap=\"off\" name=\"Pipeline log\" readonly=\"readonly\" style=\"height: auto;\">"
              + Encode.forHtml( getLogText( pipeline, startLineNr, lastLineNr ) ) + "</textarea>" );
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
          out.println( "  pipelinelog.scrollTop=pipelinelog.scrollHeight; " );
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
  }

  public String toString() {
    return "Pipeline Status IHandler";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

  private String getLogText( IPipelineEngine pipeline, int startLineNr, int lastLineNr ) throws HopException {
    try {
      return HopLogStore.getAppender().getBuffer( pipeline.getLogChannel().getLogChannelId(), false, startLineNr, lastLineNr ).toString();
    } catch ( OutOfMemoryError error ) {
      throw new HopException( "Log string is too long", error );
    }
  }

}
