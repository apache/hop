/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.www;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.server.HttpUtil;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
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

@HopServerServlet(id="pipelineStatus", name = "Get the status of a pipeline")
public class GetPipelineStatusServlet extends BaseHttpServlet implements IHopServerPlugin {

  private static final Class<?> PKG = GetPipelineStatusServlet.class; // For Translator

  private static final long serialVersionUID = 3634806745372015720L;

  public static final String CONTEXT_PATH = "/hop/pipelineStatus";

  public static final String SEND_RESULT = "sendResult";

  private static final byte[] XML_HEADER =
    XmlHandler.getXmlHeader( Const.XML_ENCODING ).getBytes( Charset.forName( Const.XML_ENCODING ) );

  @VisibleForTesting
  HopServerStatusCache cache = HopServerStatusCache.getInstance();

  public GetPipelineStatusServlet() {
  }

  public GetPipelineStatusServlet( PipelineMap pipelineMap ) {
    super( pipelineMap );
  }


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
    String root = request.getRequestURI() == null ? "/hop"
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

            HopServerPipelineStatus pipelineStatus = new HopServerPipelineStatus( pipelineName, entry.getId(), pipeline.getStatusDescription() );
            pipelineStatus.setFirstLoggingLineNr( startLineNr );
            pipelineStatus.setLastLoggingLineNr( lastLineNr );
            pipelineStatus.setLogDate( new Date() );
            pipelineStatus.setExecutionStartDate( pipeline.getExecutionStartDate() );
            pipelineStatus.setExecutionEndDate( pipeline.getExecutionEndDate() );

            for ( IEngineComponent component : pipeline.getComponents() ) {
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
        if ( EnvUtil.getSystemProperty( Const.HOP_SERVER_REFRESH_STATUS, "N" ).equalsIgnoreCase( "Y" ) ) {
          out.println( "<META http-equiv=\"Refresh\" content=\"10;url="
            + convertContextPath( CONTEXT_PATH ) + "?name=" + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id="
            + URLEncoder.encode( id, "UTF-8" ) + "\">" );
        }
        out.println( "<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">" );

        if ( isJettyMode() ) {
          out.println( "<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/css/hop-server.css\" />" );
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
          out.print( BaseMessages.getString( PKG, "HopServerStatusServlet.BackToHopServerStatus" ) + "</a>" );
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
              + BaseMessages.getString( PKG, "PipelineStatusServlet.ServerObjectId" ) + "</th> <th style=\"font-weight: normal; padding: 8px 10px 10px 10px\" class=\"cellTableHeader\">"
              + BaseMessages.getString( PKG, "PipelineStatusServlet.PipelineStatus" ) + "</th> <th style=\"font-weight: normal; padding: 8px 10px 10px 10px\" class=\"cellTableHeader\">"
              + BaseMessages.getString( PKG, "PipelineStatusServlet.StartDate" ) + "</th> </tr>" );
          out.print( "<tr class=\"cellTableRow\" style=\"border: solid; border-width: 1px 0; border-bottom: none; font-size: 12; text-align: left;\">" );
          out.print( "<td style=\"padding: 8px 10px 10px 10px\" class=\"cellTableCell cellTableFirstColumn\">" + Encode.forHtml( id ) + "</td>" );
          out.print(
            "<td style=\"padding: 8px 10px 10px 10px\" class=\"cellTableCell\" id=\"statusColor\" style=\"font-weight: bold;\">" + Encode.forHtml( pipeline.getStatusDescription() ) + "</td>" );
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
            + "<img src=\"" + prefix + "/images/download.svg\" style=\"display: block; margin: auto; width: 22px; height: 22px;\"></a>" );
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
          out.println( "<table class=\"hop-table\" border=\"" + tableBorder + "\">" );
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
          Point max = pipeline.getPipelineMeta().getMaximum();
          max.x = (int)(max.x * GetPipelineImageServlet.ZOOM_FACTOR) + 100;
          max.y = (int)(max.y * GetPipelineImageServlet.ZOOM_FACTOR) + 50;
          out.print( "<iframe height=\"" + (max.y+100) + "px\" width=\"" + (max.x+100) + "px\" "
            + "src=\"" + convertContextPath( GetPipelineImageServlet.CONTEXT_PATH ) + "?name="
            + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id=" + URLEncoder.encode( id, "UTF-8" )
            + "\" frameborder=\"0\"></iframe>" );
          ;
          out.print( "</div>" );

          // Put the logging below that.
          out.print( "<div class=\"row\" style=\"padding: 0px 0px 30px 0px;\">" );
          out.print( "<div class=\"workspaceHeading\" style=\"padding: 0px 0px 30px 0px;\">Pipeline log</div>" );
          out.println( "<textarea id=\"pipelinelog\" cols=\"120\" rows=\"20\" "
              + "wrap=\"off\" name=\"Pipeline log\" readonly=\"readonly\" style=\"height: auto; width: 100%;\">"
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
