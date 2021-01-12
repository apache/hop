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

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.core.annotations.HopServerServlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

@HopServerServlet(id="status", name = "Get the status of the server")
public class GetStatusServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static final Class<?> PKG = GetStatusServlet.class; // For Translator

  private static final long serialVersionUID = 3634806745372015720L;

  public static final String CONTEXT_PATH = "/hop/status";

  public GetStatusServlet() {
  }

  public GetStatusServlet( PipelineMap pipelineMap, WorkflowMap workflowMap ) {
    super( pipelineMap, workflowMap );
  }

  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException,
    IOException {
    if ( isJettyMode() && !request.getContextPath().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "GetStatusServlet.StatusRequested" ) );
    }
    response.setStatus( HttpServletResponse.SC_OK );
    String root = request.getRequestURI() == null ? StatusServletUtils.HOP_ROOT
      : request.getRequestURI().substring( 0, request.getRequestURI().indexOf( CONTEXT_PATH ) );
    String prefix = isJettyMode() ? StatusServletUtils.STATIC_PATH : root + StatusServletUtils.RESOURCES_PATH;
    boolean useXML = "Y".equalsIgnoreCase( request.getParameter( "xml" ) );
    boolean useLightTheme = "Y".equalsIgnoreCase( request.getParameter( "useLightTheme" ) );

    if ( useXML ) {
      response.setContentType( "text/xml" );
      response.setCharacterEncoding( Const.XML_ENCODING );
    } else {
      response.setContentType( "text/html;charset=UTF-8" );
    }

    PrintWriter out = response.getWriter();

    List<HopServerObjectEntry> pipelineEntries = getPipelineMap().getPipelineObjects();
    List<HopServerObjectEntry> actions = getWorkflowMap().getWorkflowObjects();

    if ( useXML ) {
      out.print( XmlHandler.getXmlHeader( Const.XML_ENCODING ) );
      HopServerStatus serverStatus = new HopServerStatus();
      serverStatus.setStatusDescription( "Online" );

      getSystemInfo( serverStatus );

      for ( HopServerObjectEntry entry : pipelineEntries ) {
        IPipelineEngine<PipelineMeta> pipeline = getPipelineMap().getPipeline( entry );
        String statusDescription = pipeline.getStatusDescription();

        HopServerPipelineStatus pipelineStatus = new HopServerPipelineStatus( entry.getName(), entry.getId(), statusDescription );
        pipelineStatus.setLogDate( pipeline.getExecutionStartDate() );
        pipelineStatus.setPaused( pipeline.isPaused() );
        serverStatus.getPipelineStatusList().add( pipelineStatus );
      }

      for ( HopServerObjectEntry entry : actions ) {
        IWorkflowEngine<WorkflowMeta> workflow = getWorkflowMap().getWorkflow( entry );
        String status = workflow.getStatusDescription();
        HopServerWorkflowStatus jobStatus = new HopServerWorkflowStatus( entry.getName(), entry.getId(), status );
        jobStatus.setLogDate( workflow.getExecutionStartDate() );
        serverStatus.getJobStatusList().add( jobStatus );
      }

      try {
        out.println( serverStatus.getXml() );
      } catch ( HopException e ) {
        throw new ServletException( "Unable to get the server status in XML format", e );
      }
    } else {
      out.println( "<HTML>" );
      out.println( "<HEAD><TITLE>"
        + BaseMessages.getString( PKG, "GetStatusServlet.HopHopServerStatus" ) + "</TITLE>" );
      out.println( "<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">" );

      int tableBorder = 1;
      if ( !useLightTheme ) {
        if ( isJettyMode() ) {
          out.println( "<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/css/hop-server.css\" />" );
        } else {
          out.println( "<style>" );
          out.println( ".hop-table td, tr.cellTableRow, td.gwt-MenuItem, .toolbar-button:not(.toolbar-button-disabled) {" );
          out.println( "  cursor: pointer;" );
          out.println( "}" );
          out.println( ".toolbar-button-disabled {" );
          out.println( "  opacity: 0.4;" );
          out.println( "}" );
          out.println( "div#messageDialogBody:first-letter {" );
          out.println( "  text-transform: capitalize;" );
          out.println( "}" );
          out.println( "</style>" );
        }
        tableBorder = 0;
      }

      out.println( "</HEAD>" );
      out.println( "<BODY class=\"hop-page-background dragdrop-dropTarget dragdrop-boundary\" style=\"overflow: auto;\">" );

      // Empty div for containing currently selected item
      out.println( "<div id=\"selectedTableItem\">" );
      out.println( "<value></value>" ); //initialize to none
      out.println( "</div>" );

      out.println( "<div class=\"row\" id=\"pucHeader\">" );

      String htmlClass = useLightTheme ? "h1" : "div";
      out.println( "<" + htmlClass + " class=\"workspaceHeading\" style=\"padding: 0px 0px 0px 10px;\">" + BaseMessages.getString( PKG, "GetStatusServlet.TopStatus" ) + "</" + htmlClass + ">" );
      out.println( "</div>" );

      try {
        // Tooltips
        String run = BaseMessages.getString( PKG, "HopServerStatusServlet.Run" );
        String stop = BaseMessages.getString( PKG, "HopServerStatusServlet.StopPipeline" );
        String cleanup = BaseMessages.getString( PKG, "HopServerStatusServlet.CleanupPipeline" );
        String view = BaseMessages.getString( PKG, "HopServerStatusServlet.ViewPipelineDetails" );
        String remove = BaseMessages.getString( PKG, "HopServerStatusServlet.RemovePipeline" );
        out.println( "<div class=\"row\" style=\"padding: 0px 0px 0px 30px\">" );
        htmlClass = useLightTheme ? "h2" : "div";
        out.println( "<div class=\"row\" style=\"padding: 25px 30px 75px 0px;\">" );
        out.println( "<" + htmlClass + " class=\"workspaceHeading\" style=\"padding: 0px 0px 0px 0px;\">Pipelines</" + htmlClass + ">" );
        out.println( "<table id=\"pipeline-table\" cellspacing=\"0\" cellpadding=\"0\"><tbody><tr><td align=\"left\" width=\"100%\" style=\"vertical-align:middle;\">" );
        out.println( "<table cellspacing=\"0\" cellpadding=\"0\" class=\"toolbar\" style=\"width: 100%; height: 26px; margin-bottom: 2px; border: 0;\">" );
        out.println( "<tbody><tr>" );
        out.println( "<td align=\"left\" style=\"vertical-align: middle; width: 100%\" id=\"pipeline-align\"></td>" );
        out.println( "<td " + setupIconEnterLeaveJavascript( "pause" )
          + " align=\"left\" style=\"vertical-align: middle;\"><div style=\"padding: 2px;\" onClick=\"resumeFunction( this )\" class=\"toolbar-button toolbar-button-disabled\" id=\"pause\"><img "
          + "style=\"width: 22px; height: 22px\" src=\""
          + prefix + "/images/run.svg\" title=\"" + run + "\"/></div></td>" );
        out.println( "<td " + setupIconEnterLeaveJavascript( "stop" )
          + " align=\"left\" style=\"vertical-align: middle;\"><div style=\"padding: 2px;\" onClick=\"stopFunction( this )\" class=\"toolbar-button toolbar-button-disabled\" id=\"stop\"><img "
          + "style=\"width: 22px; height: 22px\"src=\""
          + prefix + "/images/stop.svg\" title=\"" + stop + "\"/></div></td>" );
        out.println( "<td " + setupIconEnterLeaveJavascript( "cleanup" )
          + " align=\"left\" style=\"vertical-align: middle;\"><div style=\"padding: 2px; margin-left: 10px !important;\" onClick=\"cleanupFunction( this )\" class=\"toolbar-button "
          + "toolbar-button-disabled\" id=\"cleanup\"><img style=\"width: 22px; height: 22px\"src=\""
          + prefix + "/images/cleanup.svg\" title=\"" + cleanup + "\"/></div></td>" );
        out.println( "<td " + setupIconEnterLeaveJavascript( "view" )
          + " align=\"left\" style=\"vertical-align: middle;\"><div style=\"padding: 2px; margin-left: 0 !important;\" onClick=\"viewFunction( this )\" class=\"toolbar-button "
          + "toolbar-button-disabled\" id=\"view\"><img style=\"width: 22px; height: 22px\" src=\""
          + prefix + "/images/view.svg\" title=\"" + view + "\"/></div></td>" );
        out.println( "<td " + setupIconEnterLeaveJavascript( "close" )
          + " align=\"left\" style=\"vertical-align: middle;\"><div style=\"padding: 2px; margin-right: 10px;\" onClick=\"removeFunction( this )\" class=\"toolbar-button toolbar-button-disabled\" "
          + "id=\"close\"><img style=\"width: 22px; height: 22px\" src=\""
          + prefix + "/images/close.svg\" title=\"" + remove + "\"/></div></td>" );
        out.println( "</tr></tbody></table>" );
        out.println(
          "<div id=\"runActions\" class=\"custom-dropdown-popup\" style=\"visibility: hidden; overflow: visible; position: fixed;\" onLoad=\"repositionActions( this, document.getElementById( "
            + "'pause' ) )\" onMouseLeave=\"this.style='visibility: hidden; overflow: visible; position: fixed;'\"><div class=\"popupContent\"><div style=\"padding: 0;\" class=\"gwt-MenuBar "
            + "gwt-MenuBar-vertical\"><table><tbody><tr><td class=\"gwt-MenuItem\" onClick=\"runPipelineSelector( this )\" onMouseEnter=\"this.className='gwt-MenuItem gwt-MenuItem-selected'\" "
            + "onMouseLeave=\"this.className='gwt-MenuItem'\">Prepare the execution</td></tr><tr><td class=\"gwt-MenuItem\" onClick=\"runPipelineSelector( this )\" onMouseEnter=\"this"
            + ".className='gwt-MenuItem gwt-MenuItem-selected'\" onMouseLeave=\"this.className='gwt-MenuItem'\">Run</td></tr></tbody></table></div></div></div>" );
        out.println(
          "<div id=\"stopActions\" class=\"custom-dropdown-popup\" style=\"visibility: hidden; overflow: visible; position: fixed;\" onLoad=\"repositionActions( this, document.getElementById( "
            + "'stop' ) )\" onMouseLeave=\"this.style='visibility: hidden; overflow: visible; position: fixed;'\"><div class=\"popupContent\"><div style=\"padding: 0;\" class=\"gwt-MenuBar "
            + "gwt-MenuBar-vertical\"><table><tbody><tr><td class=\"gwt-MenuItem\" onClick=\"stopPipelineSelector( this )\" onMouseEnter=\"this.className='gwt-MenuItem gwt-MenuItem-selected'\" "
            + "onMouseLeave=\"this.className='gwt-MenuItem'\">Stop pipeline</td></tr><tr><td class=\"gwt-MenuItem\" onClick=\"stopPipelineSelector( this )\" onMouseEnter=\"this"
            + ".className='gwt-MenuItem gwt-MenuItem-selected'\" onMouseLeave=\"this.className='gwt-MenuItem'\">Stop input processing</td></tr></tbody></table></div></div></div>" );
        out.println( messageDialog() );
        out.println( "<table class=\"hop-table\" border=\"" + tableBorder + "\">" );
        out.print( "<tr> <th class=\"cellTableHeader\">"
          + BaseMessages.getString( PKG, "GetStatusServlet.PipelineName" ) + "</th> <th class=\"cellTableHeader\">"
          + BaseMessages.getString( PKG, "GetStatusServlet.ServerId" ) + "</th> <th class=\"cellTableHeader\">"
          + BaseMessages.getString( PKG, "GetStatusServlet.Status" ) + "</th> <th class=\"cellTableHeader\">"
          + BaseMessages.getString( PKG, "GetStatusServlet.StartDate" ) + "</th> <th class=\"cellTableHeader\">"
          + BaseMessages.getString( PKG, "GetStatusServlet.LastLogTime" ) + "</th> </tr>" );

        Comparator<HopServerObjectEntry> pipelineComparator = ( o1, o2 ) -> {
          IPipelineEngine<PipelineMeta> t1 = getPipelineMap().getPipeline( o1 );
          IPipelineEngine<PipelineMeta> t2 = getPipelineMap().getPipeline( o2 );
          Date d1 = t1.getExecutionStartDate();
          Date d2 = t2.getExecutionStartDate();
          // if both pipelines have last log date, desc sort by log date
          if ( d1 != null && d2 != null ) {
            int logDateCompare = d2.compareTo( d1 );
            if ( logDateCompare != 0 ) {
              return logDateCompare;
            }
          }
          return o1.compareTo( o2 );
        };

        Collections.sort( pipelineEntries, pipelineComparator );

        boolean evenRow = true;
        for ( int i = 0; i < pipelineEntries.size(); i++ ) {
          String name = pipelineEntries.get( i ).getName();
          String id = pipelineEntries.get( i ).getId();
          IPipelineEngine<PipelineMeta> pipeline = getPipelineMap().getPipeline( pipelineEntries.get( i ) );
          String statusDescription = pipeline.getStatusDescription();
          String trClass = evenRow ? "cellTableEvenRow" : "cellTableOddRow"; // alternating row color
          String tdClass = evenRow ? "cellTableEvenRowCell" : "cellTableOddRowCell";
          evenRow = !evenRow; // flip
          String firstColumn = i == 0 ? "cellTableFirstColumn" : "";
          String lastColumn = i == pipelineEntries.size() - 1 ? "cellTableLastColumn" : "";
          out.print( "<tr onMouseEnter=\"mouseEnterFunction( this, '" + trClass + "' )\" "
            + "onMouseLeave=\"mouseLeaveFunction( this, '" + trClass + "' )\" "
            + "onClick=\"clickFunction( this, '" + trClass + "' )\" "
            + "id=\"cellTableRow_" + i + "\" class=\"" + trClass + "\">" );
          out.print( "<td onMouseEnter=\"mouseEnterFunction( this, '" + tdClass + "' )\" "
            + "onMouseLeave=\"mouseLeaveFunction( this, '" + tdClass + "' )\" "
            + "onClick=\"clickFunction( this, '" + tdClass + "' )\" "
            + "id=\"cellTableFirstCell_" + i + "\" class=\"cellTableCell cellTableFirstColumn " + tdClass + "\">" + name + "</td>" );
          out.print( "<td onMouseEnter=\"mouseEnterFunction( this, '" + tdClass + "' )\" "
            + "onMouseLeave=\"mouseLeaveFunction( this, '" + tdClass + "' )\" "
            + "onClick=\"clickFunction( this, '" + tdClass + "' )\" "
            + "id=\"cellTableCell_" + i + "\" class=\"cellTableCell " + tdClass + "\">" + id + "</td>" );
          out.print( "<td onMouseEnter=\"mouseEnterFunction( this, '" + tdClass + "' )\" "
            + "onMouseLeave=\"mouseLeaveFunction( this, '" + tdClass + "' )\" "
            + "onClick=\"clickFunction( this, '" + tdClass + "' )\" "
            + "id=\"cellTableCellStatus_" + i + "\" class=\"cellTableCell " + tdClass + "\">" + statusDescription + "</td>" );
          String dateStr = XmlHandler.date2string( pipeline.getExecutionStartDate() );
          out.print( "<td onMouseEnter=\"mouseEnterFunction( this, '" + tdClass + "' )\" "
            + "onMouseLeave=\"mouseLeaveFunction( this, '" + tdClass + "' )\" "
            + "onClick=\"clickFunction( this, '" + tdClass + "' )\" "
            + "id=\"cellTableCell_" + i + "\" class=\"cellTableCell " + tdClass + "\">"
            + ( pipeline.getExecutionStartDate() == null ? "-" : dateStr.substring( 0, dateStr.indexOf( ' ' ) ) ) + "</td>" );
          out.print( "<td onMouseEnter=\"mouseEnterFunction( this, '" + tdClass + "' )\" "
            + "onMouseLeave=\"mouseLeaveFunction( this, '" + tdClass + "' )\" "
            + "onClick=\"clickFunction( this, '" + tdClass + "' )\" "
            + "id=\"cellTableLastCell_" + i + "\" class=\"cellTableCell cellTableLastColumn " + tdClass + "\">" + dateStr.substring( dateStr.indexOf( ' ' ) ) + "</td>" );
          out.print( "</tr>" );
        }
        out.print( "</table></table>" );
        out.print( "</div>" ); // end div

        // Tooltips
        String runJ = BaseMessages.getString( PKG, "HopServerStatusServlet.Run" );
        String stopJ = BaseMessages.getString( PKG, "HopServerStatusServlet.StopWorkflow" );
        String viewJ = BaseMessages.getString( PKG, "HopServerStatusServlet.ViewWorkflowDetails" );
        String removeJ = BaseMessages.getString( PKG, "HopServerStatusServlet.RemoveWorkflow" );

        out.println( "<div class=\"row\" style=\"padding: 0px 30px 75px 0px;\">" );
        out.println( "<" + htmlClass + " class=\"workspaceHeading\" style=\"padding: 0px 0px 0px 0px;\">Workflows</" + htmlClass + ">" );
        out.println( "<table cellspacing=\"0\" cellpadding=\"0\"><tbody><tr><td align=\"left\" width=\"100%\" style=\"vertical-align:middle;\">" );
        out.println( "<table cellspacing=\"0\" cellpadding=\"0\" class=\"toolbar\" style=\"width: 100%; height: 26px; margin-bottom: 2px; border: 0;\">" );
        out.println( "<tbody><tr>" );
        out.println( "<td align=\"left\" style=\"vertical-align: middle; width: 100%\"></td>" );
        out.println( "<td " + setupIconEnterLeaveJavascript( "j-pause" )
          + " align=\"left\" style=\"vertical-align: middle;\"><div style=\"padding: 2px;\" onClick=\"resumeFunction( this )\" class=\"toolbar-button toolbar-button-disabled\" id=\"j-pause\"><img "
          + "style=\"width: 22px; height: 22px\" src=\""
          + prefix + "/images/run.svg\" title=\"" + runJ + "\"/></div></td>" );
        out.println( "<td " + setupIconEnterLeaveJavascript( "j-stop" )
          + " align=\"left\" style=\"vertical-align: middle;\"><div style=\"padding: 2px;\" onClick=\"stopFunction( this )\" class=\"toolbar-button toolbar-button-disabled\" id=\"j-stop\"><img "
          + "style=\"width: 22px; height: 22px\"src=\""
          + prefix + "/images/stop.svg\" title=\"" + stopJ + "\"/></div></td>" );
        out.println( "<td " + setupIconEnterLeaveJavascript( "j-view" )
          + " align=\"left\" style=\"vertical-align: middle;\"><div style=\"padding: 2px;\" onClick=\"viewFunction( this )\" class=\"toolbar-button toolbar-button-disabled\" id=\"j-view\"><img "
          + "style=\"width: 22px; height: 22px\" src=\""
          + prefix + "/images/view.svg\" title=\"" + viewJ + "\"/></div></td>" );
        out.println( "<td " + setupIconEnterLeaveJavascript( "j-close" )
          + " align=\"left\" style=\"vertical-align: middle;\"><div style=\"padding: 2px; margin-right: 10px;\" onClick=\"removeFunction( this )\" class=\"toolbar-button toolbar-button-disabled\" "
          + "id=\"j-close\"><img style=\"width: 22px; height: 22px\" src=\""
          + prefix + "/images/close.svg\" title=\"" + removeJ + "\"/></div></td>" );
        out.println( "</tr></tbody></table>" );
        out.println( "<table class=\"hop-table\" border=\"" + tableBorder + "\">" );
        out.print( "<tr> <th class=\"cellTableHeader\">"
          + BaseMessages.getString( PKG, "GetStatusServlet.WorkflowName" ) + "</th> <th class=\"cellTableHeader\">"
          + BaseMessages.getString( PKG, "GetStatusServlet.ServerId" ) + "</th> <th class=\"cellTableHeader\">"
          + BaseMessages.getString( PKG, "GetStatusServlet.Status" ) + "</th> <th class=\"cellTableHeader\">"
          + BaseMessages.getString( PKG, "GetStatusServlet.StartDate" ) + "</th> <th class=\"cellTableHeader\">"
          + BaseMessages.getString( PKG, "GetStatusServlet.LastLogTime" ) + "</th> </tr>" );

        Comparator<HopServerObjectEntry> jobComparator = ( o1, o2 ) -> {
          IWorkflowEngine<WorkflowMeta> t1 = getWorkflowMap().getWorkflow( o1 );
          IWorkflowEngine<WorkflowMeta> t2 = getWorkflowMap().getWorkflow( o2 );
          Date d1 = t1.getExecutionStartDate();
          Date d2 = t2.getExecutionStartDate();
          // if both workflows have last log date, desc sort by log date
          if ( d1 != null && d2 != null ) {
            int logDateCompare = d2.compareTo( d1 );
            if ( logDateCompare != 0 ) {
              return logDateCompare;
            }
          }
          return o1.compareTo( o2 );
        };

        Collections.sort( actions, jobComparator );

        evenRow = true;
        for ( int i = 0; i < actions.size(); i++ ) {
          String name = actions.get( i ).getName();
          String id = actions.get( i ).getId();
          IWorkflowEngine<WorkflowMeta> workflow = getWorkflowMap().getWorkflow( actions.get( i ) );
          String status = workflow.getStatusDescription();
          String trClass = evenRow ? "cellTableEvenRow" : "cellTableOddRow"; // alternating row color
          String tdClass = evenRow ? "cellTableEvenRowCell" : "cellTableOddRowCell";
          evenRow = !evenRow; // flip
          out.print( "<tr onMouseEnter=\"mouseEnterFunction( this, '" + trClass + "' )\" "
            + "onMouseLeave=\"mouseLeaveFunction( this, '" + trClass + "' )\" "
            + "onClick=\"clickFunction( this, '" + trClass + "' )\" "
            + "id=\"j-cellTableRow_" + i + "\" class=\"cellTableCell " + trClass + "\">" );
          out.print( "<td onMouseEnter=\"mouseEnterFunction( this, '" + tdClass + "' )\" "
            + "onMouseLeave=\"mouseLeaveFunction( this, '" + tdClass + "' )\" "
            + "onClick=\"clickFunction( this, '" + tdClass + "' )\" "
            + "id=\"j-cellTableFirstCell_" + i + "\" class=\"cellTableCell cellTableFirstColumn " + tdClass + "\">" + name + "</a></td>" );
          out.print( "<td onMouseEnter=\"mouseEnterFunction( this, '" + tdClass + "' )\" "
            + "onMouseLeave=\"mouseLeaveFunction( this, '" + tdClass + "' )\" "
            + "onClick=\"clickFunction( this, '" + tdClass + "' )\" "
            + "id=\"j-cellTableCell_" + i + "\" class=\"cellTableCell " + tdClass + "\">" + id + "</td>" );
          out.print( "<td onMouseEnter=\"mouseEnterFunction( this, '" + tdClass + "' )\" "
            + "onMouseLeave=\"mouseLeaveFunction( this, '" + tdClass + "' )\" "
            + "onClick=\"clickFunction( this, '" + tdClass + "' )\" "
            + "id=\"j-cellTableCell_" + i + "\" class=\"cellTableCell " + tdClass + "\">" + status + "</td>" );
          String dateStr = XmlHandler.date2string( workflow.getExecutionStartDate() );
          out.print( "<td onMouseEnter=\"mouseEnterFunction( this, '" + tdClass + "' )\" "
            + "onMouseLeave=\"mouseLeaveFunction( this, '" + tdClass + "' )\" "
            + "onClick=\"clickFunction( this, '" + tdClass + "' )\" "
            + "id=\"j-cellTableCell_" + i + "\" class=\"cellTableCell " + tdClass + "\">"
            + ( workflow.getExecutionStartDate() == null ? "-" : dateStr.substring( 0, dateStr.indexOf( ' ' ) ) ) + "</td>" );
          out.print( "<td onMouseEnter=\"mouseEnterFunction( this, '" + tdClass + "' )\" "
            + "onMouseLeave=\"mouseLeaveFunction( this, '" + tdClass + "' )\" "
            + "onClick=\"clickFunction( this, '" + tdClass + "' )\" "
            + "id=\"j-cellTableLastCell_" + i + "\" class=\"cellTableCell cellTableLastColumn " + tdClass + "\">" +
              ( dateStr!=null ? dateStr.substring( dateStr.indexOf( ' ' ) ) : "")
            + "</td>" );
          out.print( "</tr>" );
        }
        out.print( "</table></table>" );
        out.print( "</div>" ); // end div

      } catch ( Exception ex ) {
        out.println( "<pre>" );
        ex.printStackTrace( out );
        out.println( "</pre>" );
      }

      out.println( "<div class=\"row\" style=\"padding: 0px 0px 30px 0px;\">" );
      htmlClass = useLightTheme ? "h3" : "div";
      out.println( "<div><" + htmlClass + " class=\"workspaceHeading\">"
        + BaseMessages.getString( PKG, "GetStatusServlet.ConfigurationDetails.Title" ) + "</" + htmlClass + "></div>" );
      out.println( "<table border=\"" + tableBorder + "\">" );
      //      out.print( "<tr> <th class=\"cellTableHeader\">"
      //          + BaseMessages.getString( PKG, "GetStatusServlet.Parameter.Title" ) + "</th> <th class=\"cellTableHeader\">"
      //          + BaseMessages.getString( PKG, "GetStatusServlet.Value.Title" ) + "</th> </tr>" );

      // The max number of log lines in the back-end
      //
      HopServerConfig serverConfig = getPipelineMap().getHopServerConfig();
      if ( serverConfig != null ) {
        String maxLines = "";
        if ( serverConfig.getMaxLogLines() == 0 ) {
          maxLines = BaseMessages.getString( PKG, "GetStatusServlet.NoLimit" );
        } else {
          maxLines = serverConfig.getMaxLogLines() + BaseMessages.getString( PKG, "GetStatusServlet.Lines" );
        }
        out.print( "<tr style=\"font-size: 12;\"> <td style=\"padding: 2px 10px 2px 10px\" class=\"cellTableCell cellTableEvenRowCell cellTableFirstColumn\">"
          + BaseMessages.getString( PKG, "GetStatusServlet.Parameter.MaxLogLines" ) + "</td> <td style=\"padding: 2px 10px 2px 10px\" class=\"cellTableCell cellTableEvenRowCell cellTableLastColumn\">"
          + maxLines
          + "</td> </tr>" );

        // The max age of log lines
        //
        String maxAge = "";
        if ( serverConfig.getMaxLogTimeoutMinutes() == 0 ) {
          maxAge = BaseMessages.getString( PKG, "GetStatusServlet.NoLimit" );
        } else {
          maxAge = serverConfig.getMaxLogTimeoutMinutes() + BaseMessages.getString( PKG, "GetStatusServlet.Minutes" );
        }
        out.print( "<tr style=\"font-size: 12;\"> <td style=\"padding: 2px 10px 2px 10px\" class=\"cellTableCell cellTableEvenRowCell cellTableFirstColumn\">"
          + BaseMessages.getString( PKG, "GetStatusServlet.Parameter.MaxLogLinesAge" )
          + "</td> <td style=\"padding: 2px 10px 2px 10px\" class=\"cellTableCell cellTableEvenRowCell cellTableLastColumn\">" + maxAge
          + "</td> </tr>" );

        // The max age of stale objects
        //
        String maxObjAge = "";
        if ( serverConfig.getObjectTimeoutMinutes() == 0 ) {
          maxObjAge = BaseMessages.getString( PKG, "GetStatusServlet.NoLimit" );
        } else {
          maxObjAge = serverConfig.getObjectTimeoutMinutes() + BaseMessages.getString( PKG, "GetStatusServlet.Minutes" );
        }
        out.print( "<tr style=\"font-size: 12;\"> <td style=\"padding: 2px 10px 2px 10px\" class=\"cellTableCell cellTableEvenRowCell cellTableFirstColumn\">"
          + BaseMessages.getString( PKG, "GetStatusServlet.Parameter.MaxObjectsAge" )
          + "</td> <td style=\"padding: 2px 10px 2px 10px\" class=\"cellTableCell cellTableEvenRowCell cellTableLastColumn\">" + maxObjAge
          + "</td> </tr>" );

        out.print( "</table>" );

        String filename = serverConfig.getFilename();
        if ( filename == null ) {
          filename = BaseMessages.getString( PKG, "GetStatusServlet.ConfigurationDetails.UsingDefaults" );
        }
        out.println( "</div>" ); // end div
        out.print( "<div class=\"row\">" );
        out
          .println( "<i>"
            + BaseMessages.getString( PKG, "GetStatusServlet.ConfigurationDetails.Advice", filename )
            + "</i>" );
        out.print( "</div>" );
        out.print( "</div>" );
        out.print( "</div>" );
      }

      out.println( "<script type=\"text/javascript\">" );
      out.println( "if (!String.prototype.endsWith) {" );
      out.println( "  String.prototype.endsWith = function(suffix) {" );
      out.println( "    return this.indexOf(suffix, this.length - suffix.length) !== -1;" );
      out.println( "  };" );
      out.println( "}" );
      out.println( "if (!String.prototype.startsWith) {" );
      out.println( "  String.prototype.startsWith = function(searchString, position) {" );
      out.println( "    position = position || 0;" );
      out.println( "    return this.indexOf(searchString, position) === position;" );
      out.println( "  };" );
      out.println( "}" );
      out.println( "var selectedPipelineRowIndex = -1;" ); // currently selected table item
      out.println( "var selectedWorkflowRowIndex = -1;" ); // currently selected table item
      out.println( "var removeElement = null;" ); // element of remove button clicked
      out.println( "var selectedPipelineName = \"\";" );
      out.println( "var selectedWorkflowName = \"\";" );

      // Click function for stop button
      out.println( "function repositionActions( element, elementFrom ) {" );
      out.println( "element.style.left = ( 10 + elementFrom.getBoundingClientRect().left ) + 'px';" );
      //out.println( "element.style.top = document.getElementById( 'pipeline-table' ).offsetTop + 'px';" );
      out.println( "}" );

      // Click function for resume button
      out.println( "function resumeFunction( element ) {" );
      out.println( "if( !element.classList.contains('toolbar-button-disabled') ) {" );
      out.println( "if( element.id.startsWith( 'j-' ) && selectedWorkflowRowIndex != -1 ) {" );
      out.println( setupAjaxCall( setupJobURI( convertContextPath( StartWorkflowServlet.CONTEXT_PATH ) ),
        BaseMessages.getString( PKG, "GetStatusServlet.StartWorkflow.Title" ),
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.TheWorkflow.Label" ) + " ' + selectedWorkflowName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.StartWorkflow.Success.Body" ) + "'",
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.TheWorkflow.Label" ) + " ' + selectedWorkflowName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.StartWorkflow.Failure.Body" ) + "'" ) );
      out.println( "} else if ( !element.id.startsWith( 'j-' ) && selectedPipelineRowIndex != -1 && document.getElementById( 'cellTableCellStatus_' + selectedPipelineRowIndex ).innerHTML == 'Running') {" );
      out.println( setupAjaxCall( setupPipelineURI( convertContextPath( PausePipelineServlet.CONTEXT_PATH ) ),
        BaseMessages.getString( PKG, "GetStatusServlet.PausePipeline.Title" ),
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.ThePipeline.Label" ) + " ' + selectedPipelineName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.PausePipeline.Success.Body" )
          + "'",
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.ThePipeline.Label" ) + " ' + selectedPipelineName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.PausePipeline.Failure.Body" )
          + "'" ) );
      out.println( "} else if( !element.id.startsWith( 'j-' ) && selectedPipelineRowIndex != -1 && document.getElementById( 'cellTableCellStatus_' + selectedPipelineRowIndex ).innerHTML == 'Paused') {" );
      out.println( setupAjaxCall( setupPipelineURI( convertContextPath( PausePipelineServlet.CONTEXT_PATH ) ),
        BaseMessages.getString( PKG, "GetStatusServlet.ResumePipeline.Title" ),
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.ThePipeline.Label" ) + " ' + selectedPipelineName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.ResumePipeline.Success.Body" )
          + "'",
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.ThePipeline.Label" ) + " ' + selectedPipelineName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.ResumePipeline.Failure.Body" )
          + "'" ) );
      out.println( "} else if( !element.id.startsWith( 'j-' ) && selectedPipelineRowIndex != -1 ){" );
      out.println( "repositionActions( document.getElementById( 'runActions' ), element );" );
      out.println( "document.getElementById( 'runActions' ).style.visibility = 'visible';" );
      out.println( "}" );
      out.println( "}" );
      out.println( "}" );

      // Click function for stop button
      out.println( "function stopFunction( element ) {" );
      out.println( "if( !element.classList.contains('toolbar-button-disabled') ) {" );
      out.println( "if( element.id.startsWith( 'j-' ) && selectedWorkflowRowIndex != -1 ) {" );
      out.println( setupAjaxCall( setupJobURI( convertContextPath( StopWorkflowServlet.CONTEXT_PATH ) ),
        BaseMessages.getString( PKG, "GetStatusServlet.StopWorkflow.Title" ),
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.StopWorkflow.Success.Body1" ) + " " + BaseMessages.getString( PKG, "GetStatusServlet.TheWorkflow.Label" ) + " ' + selectedWorkflowName + ' "
          + BaseMessages.getString( PKG, "GetStatusServlet.StopWorkflow.Success.Body2" ) + "'",
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.TheWorkflow.Label" ) + " ' + selectedWorkflowName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.StopWorkflow.Failure.Body" ) + "'" ) );
      out.println( "} else if ( !element.id.startsWith( 'j-' ) && selectedPipelineRowIndex != -1 ) {" );
      out.println( "repositionActions( document.getElementById( 'stopActions' ), element );" );
      out.println( "document.getElementById( 'stopActions' ).style.visibility = 'visible';" );
      out.println( "}" );
      out.println( "}" );
      out.println( "}" );

      out.println( "function runPipelineSelector( element ) {" );
      out.println( "if( element.innerHTML == 'Prepare the execution' ){" );
      out.println( setupAjaxCall( setupPipelineURI( convertContextPath( PrepareExecutionPipelineServlet.CONTEXT_PATH ) ),
        BaseMessages.getString( PKG, "GetStatusServlet.PreparePipeline.Title" ),
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.ThePipeline.Label" ) + " ' + selectedPipelineName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.PreparePipeline.Success.Body" )
          + "'",
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.ThePipeline.Label" ) + " ' + selectedPipelineName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.PreparePipeline.Failure.Body" )
          + "'" ) );
      out.println( "} else {" );
      out.println( setupAjaxCall( setupPipelineURI( convertContextPath( StartPipelineServlet.CONTEXT_PATH ) ),
        BaseMessages.getString( PKG, "GetStatusServlet.StartPipeline.Title" ),
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.ThePipeline.Label" ) + " ' + selectedPipelineName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.StartPipeline.Success.Body" )
          + "'",
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.ThePipeline.Label" ) + " ' + selectedPipelineName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.StartPipeline.Failure.Body" )
          + "'" ) );
      out.println( "}" );
      out.println( "}" );

      // Click function for stop button
      out.println( "function stopPipelineSelector( element ) {" );
      out.println( "if( element.innerHTML == 'Stop pipeline' ) {" );
      out.println( setupAjaxCall( setupPipelineURI( convertContextPath( StopPipelineServlet.CONTEXT_PATH ) ),
        BaseMessages.getString( PKG, "GetStatusServlet.StopPipeline.Title" ),
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.PipelineStop.Success.Body1" ) + " " + BaseMessages.getString( PKG, "GetStatusServlet.ThePipeline.Label" )
          + " ' + selectedPipelineName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.PipelineStop.Success.Body2" ) + "'",
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.ThePipeline.Label" ) + " ' + selectedPipelineName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.StopPipeline.Failure.Body" )
          + "'" ) );
      out.println( "} else if( element.innerHTML == 'Stop input processing' ) {" );
      out.println( setupAjaxCall( setupPipelineURI( convertContextPath( StopPipelineServlet.CONTEXT_PATH ) ) + " + '&inputOnly=Y'",
        BaseMessages.getString( PKG, "GetStatusServlet.StopInputPipeline.Title" ),
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.StopInputPipeline.Success.Body1" ) + " " + BaseMessages.getString( PKG, "GetStatusServlet.ThePipeline.Label" )
          + " ' + selectedPipelineName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.StopInputPipeline.Success.Body2" ) + "'",
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.ThePipeline.Label" ) + " ' + selectedPipelineName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.StopInputPipeline.Failure.Body" )
          + "'" ) );
      out.println( "}" );
      out.println( "document.getElementById( 'stopActions' ).style.visibility = 'hidden';" );
      out.println( "}" );

      // Click function for view button
      out.println( "function viewFunction( element ) {" );
      out.println( "if( !element.classList.contains('toolbar-button-disabled') ) {" );
      out.println( "if( element.id.startsWith( 'j-' ) && selectedWorkflowRowIndex != -1 ) {" );
      out.println( "window.location.replace( '"
        + convertContextPath( GetWorkflowStatusServlet.CONTEXT_PATH ) + "'"
        + " + '?name=' + document.getElementById( 'j-cellTableFirstCell_' + selectedWorkflowRowIndex ).innerHTML"
        + " + '&id=' + document.getElementById( 'j-cellTableCell_' + selectedWorkflowRowIndex ).innerHTML );" );
      out.println( "} else if ( selectedPipelineRowIndex != -1 ) {" );
      out.println( "window.location.replace( '"
        + convertContextPath( GetPipelineStatusServlet.CONTEXT_PATH ) + "'"
        + " + '?name=' + document.getElementById( 'cellTableFirstCell_' + selectedPipelineRowIndex ).innerHTML"
        + " + '&id=' + document.getElementById( 'cellTableCell_' + selectedPipelineRowIndex ).innerHTML );" );
      out.println( "}" );
      out.println( "}" );
      out.println( "}" );

      // Click function for remove button
      out.println( "function removeFunction( element ) {" );
      out.println( "if( !element.classList.contains('toolbar-button-disabled') ) {" );
      out.println( "removeElement = element;" );
      out.println( "if( element.id.startsWith( 'j-' ) && selectedWorkflowRowIndex != -1 ) {" );
      out.println( "openMessageDialog( '" + BaseMessages.getString( PKG, "GetStatusServlet.RemoveWorkflow.Title" ) + "',"
        + "'" + BaseMessages.getString( PKG, "GetStatusServlet.RemoveWorkflow.Confirm.Body" ) + " " + BaseMessages.getString( PKG, "GetStatusServlet.TheWorkflow.Label" ) + " ' + selectedWorkflowName + '?" + "'"
        + ", false );" );
      out.println( "} else if ( selectedPipelineRowIndex != -1 ) {" );
      out.println( "openMessageDialog( '" + BaseMessages.getString( PKG, "GetStatusServlet.RemovePipeline.Title" ) + "',"
        + "'" + BaseMessages.getString( PKG, "GetStatusServlet.RemovePipeline.Confirm.Body" ) + " " + BaseMessages.getString( PKG, "GetStatusServlet.ThePipeline.Label" )
        + " ' + selectedPipelineName + '?" + "'" + ", false );" );
      out.println( "}" );
      out.println( "}" );
      out.println( "}" );

      // OnClick function for table element
      out.println( "function clickFunction( element, tableClass ) {" );
      out.println( "var prefix = element.id.startsWith( 'j-' ) ? 'j-' : '';" );
      out.println( "var rowNum = getRowNum( element.id );" );
      out.println( "if( tableClass.endsWith( 'Row' ) ) {" );
      out.println( "element.className='cellTableRow ' + tableClass + ' cellTableSelectedRow';" );
      out.println( "} else {" );
      out.println( "document.getElementById( prefix + 'cellTableFirstCell_' + rowNum ).className='cellTableCell cellTableFirstColumn ' + tableClass + ' cellTableSelectedRowCell';" );
      out.println( "element.className='cellTableCell ' + tableClass + ' cellTableSelectedRowCell';" );
      out.println( "}" );
      out.println( "if( element.id.startsWith( 'j-' ) ) {" );
      out.println( "document.getElementById( \"j-pause\" ).classList.remove( \"toolbar-button-disabled\" )" );
      out.println( "document.getElementById( \"j-stop\" ).classList.remove( \"toolbar-button-disabled\" )" );
      out.println( "document.getElementById( \"j-view\" ).classList.remove( \"toolbar-button-disabled\" )" );
      out.println( "document.getElementById( \"j-close\" ).classList.remove( \"toolbar-button-disabled\" )" );
      out.println( "if( selectedWorkflowRowIndex != -1 && rowNum != selectedWorkflowRowIndex ) {" );
      out.println( "document.getElementById( prefix + 'cellTableRow_' + selectedWorkflowRowIndex ).className='cellTableRow ' + tableClass;" );
      out.println( "document.getElementById( prefix + 'cellTableFirstCell_' + selectedWorkflowRowIndex ).className='cellTableCell cellTableFirstColumn ' + tableClass;" );
      out.println( "document.getElementById( prefix + 'cellTableCell_' + selectedWorkflowRowIndex ).className='cellTableCell ' + tableClass;" );
      out.println( "document.getElementById( prefix + 'cellTableLastCell_' + selectedWorkflowRowIndex ).className='cellTableCell cellTableLastColumn ' + tableClass;" );
      out.println( "}" );
      out.println( "selectedWorkflowRowIndex = rowNum;" );
      out.println( "} else {" );
      out.println( "document.getElementById( \"pause\" ).classList.remove( \"toolbar-button-disabled\" )" );
      out.println( "document.getElementById( \"stop\" ).classList.remove( \"toolbar-button-disabled\" )" );
      out.println( "document.getElementById( \"cleanup\" ).classList.remove( \"toolbar-button-disabled\" )" );
      out.println( "document.getElementById( \"view\" ).classList.remove( \"toolbar-button-disabled\" )" );
      out.println( "document.getElementById( \"close\" ).classList.remove( \"toolbar-button-disabled\" )" );
      out.println( "if( selectedPipelineRowIndex != -1 && rowNum != selectedPipelineRowIndex ) {" );
      out.println( "document.getElementById( prefix + 'cellTableRow_' + selectedPipelineRowIndex ).className='cellTableRow ' + tableClass;" );
      out.println( "document.getElementById( prefix + 'cellTableFirstCell_' + selectedPipelineRowIndex ).className='cellTableCell cellTableFirstColumn ' + tableClass;" );
      out.println( "document.getElementById( prefix + 'cellTableCell_' + selectedPipelineRowIndex ).className='cellTableCell ' + tableClass;" );
      out.println( "document.getElementById( prefix + 'cellTableLastCell_' + selectedPipelineRowIndex ).className='cellTableCell cellTableLastColumn ' + tableClass;" );
      out.println( "}" );
      out.println( "selectedPipelineRowIndex = rowNum;" );
      out.println( "if( document.getElementById( 'cellTableCellStatus_' + selectedPipelineRowIndex ).innerHTML == 'Running' ) {" );
      out.println( "document.getElementById( 'pause' ).innerHTML = '<img style=\"width: 22px; height: 22px\" src=\"" + prefix + "/images/pause.svg\"/ title=\"Pause pipeline\">';" );
      out.println( "} else if( document.getElementById( 'cellTableCellStatus_' + selectedPipelineRowIndex ).innerHTML == 'Paused' ) {" );
      out.println( "document.getElementById( 'pause' ).innerHTML = '<img style=\"width: 22px; height: 22px\" src=\"" + prefix + "/images/pause.svg\" title=\"Resume pipeline\"/>';" );
      out.println( "}" );
      out.println( "}" );
      out.println( "setSelectedNames();" );
      out.println( "}" );

      // Function to set the pipeline or workflow name of the selected pipeline or workflow
      out.println( "function setSelectedNames() {" );
      out.println( "  selectedWorkflowName = selectedPipelineName = \"\";" );
      out.println( "  var selectedElementNames = document.getElementsByClassName( \"cellTableFirstColumn cellTableSelectedRowCell\" );" );
      out.println( "  if( selectedElementNames ) {" );
      out.println( "    for(var i = 0; i < selectedElementNames.length; i++) {" );
      out.println( "      if(selectedElementNames[i].id.startsWith(\"j-\")) {" );
      out.println( "        selectedWorkflowName = selectedElementNames[i].innerHTML;" );
      out.println( "      } else {" );
      out.println( "        selectedPipelineName = selectedElementNames[i].innerHTML;" );
      out.println( "      }" );
      out.println( "    }" );
      out.println( "  }" );
      out.println( "}" );

      // OnMouseEnter function
      out.println( "function mouseEnterFunction( element, tableClass ) {" );
      out.println( "var prefix = '';" );
      out.println( "var rowNum = getRowNum( element.id );" );
      out.println( "var selectedIndex = selectedPipelineRowIndex;" );
      out.println( "if( element.id.startsWith( 'j-' ) ) {" );
      out.println( "prefix = 'j-';" );
      out.println( "selectedIndex = selectedWorkflowRowIndex;" );
      out.println( "}" );
      out.println( "if( rowNum != selectedIndex ) {" );
      out.println( "if( tableClass.endsWith( 'Row' ) ) {" );
      out.println( "element.className='cellTableRow ' + tableClass + ' cellTableHoveredRow';" );
      out.println( "} else {" );
      out.println(
        "document.getElementById( prefix + 'cellTableFirstCell_' + element.id.charAt( element.id.length - 1 ) ).className='cellTableCell cellTableFirstColumn ' + tableClass + ' "
          + "cellTableHoveredRowCell';" );
      out.println( "document.getElementById( prefix + 'cellTableCell_' + element.id.charAt( element.id.length - 1 ) ).className='cellTableCell ' + tableClass + ' cellTableHoveredRowCell';" );
      out.println(
        "document.getElementById( prefix + 'cellTableLastCell_' + element.id.charAt( element.id.length - 1 ) ).className='cellTableCell cellTableLastColumn ' + tableClass + ' "
          + "cellTableHoveredRowCell';" );
      out.println( "}" );
      out.println( "}" );
      out.println( "}" );

      // OnMouseLeave function
      out.println( "function mouseLeaveFunction( element, tableClass ) {" );
      out.println( "var prefix = '';" );
      out.println( "var rowNum = getRowNum( element.id );" );
      out.println( "var selectedIndex = selectedPipelineRowIndex;" );
      out.println( "if( element.id.startsWith( 'j-' ) ) {" );
      out.println( "prefix = 'j-';" );
      out.println( "selectedIndex = selectedWorkflowRowIndex;" );
      out.println( "}" );
      out.println( "if( rowNum != selectedIndex ) {" );
      out.println( "if( tableClass.endsWith( 'Row' ) ) {" );
      out.println( "element.className='cellTableRow ' + tableClass;" );
      out.println( "} else {" );
      out.println( "document.getElementById( prefix + 'cellTableFirstCell_' + element.id.charAt( element.id.length - 1 ) ).className='cellTableCell cellTableFirstColumn ' + tableClass;" );
      out.println( "document.getElementById( prefix + 'cellTableCell_' + element.id.charAt( element.id.length - 1 ) ).className='cellTableCell ' + tableClass;" );
      out.println( "document.getElementById( prefix + 'cellTableLastCell_' + element.id.charAt( element.id.length - 1 ) ).className='cellTableCell cellTableLastColumn ' + tableClass;" );
      out.println( "}" );
      out.println( "}" );
      out.println( "}" );

      // Onclick function for closing message dialog-->make it hidden
      out.println( "function closeMessageDialog( refresh ) {" );
      out.println( "  document.getElementById( \"messageDialogBackdrop\" ).style.visibility = 'hidden';" );
      out.println( "  document.getElementById( \"messageDialog\" ).style.visibility = 'hidden';" );
      out.println( "  if( refresh ) {" );
      out.println( "    window.location.reload();" );
      out.println( "  }" );
      out.println( "}" );

      // Function to open the message dialog--> make it visible
      out.println( "function openMessageDialog( title, body, single ) {" );
      out.println( "  document.getElementById( \"messageDialogBackdrop\" ).style.visibility = 'visible';" );
      out.println( "  document.getElementById( \"messageDialog\" ).style.visibility = 'visible';" );
      out.println( "  document.getElementById( \"messageDialogTitle\" ).innerHTML = title;" );
      out.println( "  document.getElementById( \"messageDialogBody\" ).innerHTML = body;" );
      out.println( "  if( single ) {" );
      out.println( "    document.getElementById( \"singleButton\" ).style.display = 'block';" );
      out.println( "    document.getElementById( \"doubleButton\" ).style.display = 'none';" );
      out.println( "  } else {" );
      out.println( "    document.getElementById( \"singleButton\" ).style.display = 'none';" );
      out.println( "    document.getElementById( \"doubleButton\" ).style.display = 'block';" );
      out.println( "  }" );
      out.println( "}" );

      // Function to remove selected pipeline/workflow after user confirms
      out.println( "function removeSelection() {" );
      out.println( "  if( removeElement !== null ) {" );
      out.println( "    if( removeElement.id.startsWith( 'j-' ) && selectedWorkflowRowIndex != -1 ) {" );
      out.println( setupAjaxCall( setupJobURI( convertContextPath( RemoveWorkflowServlet.CONTEXT_PATH ) ),
        BaseMessages.getString( PKG, "GetStatusServlet.RemoveWorkflow.Title" ),
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.TheWorkflow.Label" ) + " ' + selectedWorkflowName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.RemoveWorkflow.Success.Body" ) + "'",
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.TheWorkflow.Label" ) + " ' + selectedWorkflowName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.RemoveWorkflow.Failure.Body" ) + "'" ) );
      out.println( "} else if ( selectedPipelineRowIndex != -1 ) {" );
      out.println( setupAjaxCall( setupPipelineURI( convertContextPath( RemovePipelineServlet.CONTEXT_PATH ) ),
        BaseMessages.getString( PKG, "GetStatusServlet.RemovePipeline.Title" ),
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.ThePipeline.Label" ) + " ' + selectedPipelineName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.RemovePipeline.Success.Body" )
          + "'",
        "'" + BaseMessages.getString( PKG, "GetStatusServlet.ThePipeline.Label" ) + " ' + selectedPipelineName + ' " + BaseMessages.getString( PKG, "GetStatusServlet.RemovePipeline.Failure.Body" )
          + "'" ) );
      out.println( "    }" );
      out.println( "  }" );
      out.println( "}" );

      out.println( "function getRowNum( id ) {" );
      out.println( "  return id.substring( id.indexOf('_') + 1, id.length);" );
      out.println( "}" );

      out.println( "</script>" );

      out.println( "</BODY>" );
      out.println( "</HTML>" );
    }
  }

  private static void getSystemInfo( HopServerStatus serverStatus ) {
    OperatingSystemMXBean operatingSystemMXBean =
      java.lang.management.ManagementFactory.getOperatingSystemMXBean();
    ThreadMXBean threadMXBean = java.lang.management.ManagementFactory.getThreadMXBean();
    RuntimeMXBean runtimeMXBean = java.lang.management.ManagementFactory.getRuntimeMXBean();

    int cores = Runtime.getRuntime().availableProcessors();

    long freeMemory = Runtime.getRuntime().freeMemory();
    long totalMemory = Runtime.getRuntime().totalMemory();
    String osArch = operatingSystemMXBean.getArch();
    String osName = operatingSystemMXBean.getName();
    String osVersion = operatingSystemMXBean.getVersion();
    double loadAvg = operatingSystemMXBean.getSystemLoadAverage();

    int threadCount = threadMXBean.getThreadCount();
    long allThreadsCpuTime = 0L;

    long[] threadIds = threadMXBean.getAllThreadIds();
    for ( int i = 0; i < threadIds.length; i++ ) {
      allThreadsCpuTime += threadMXBean.getThreadCpuTime( threadIds[ i ] );
    }

    long uptime = runtimeMXBean.getUptime();

    serverStatus.setCpuCores( cores );
    serverStatus.setCpuProcessTime( allThreadsCpuTime );
    serverStatus.setUptime( uptime );
    serverStatus.setThreadCount( threadCount );
    serverStatus.setLoadAvg( loadAvg );
    serverStatus.setOsName( osName );
    serverStatus.setOsVersion( osVersion );
    serverStatus.setOsArchitecture( osArch );
    serverStatus.setMemoryFree( freeMemory );
    serverStatus.setMemoryTotal( totalMemory );
  }

  public String toString() {
    return "Status IHandler";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

  private String setupIconEnterLeaveJavascript( String id ) {
    return "onMouseEnter=\"if( !document.getElementById('"
      + id + "').classList.contains('toolbar-button-disabled') ) { document.getElementById('"
      + id + "').classList.add('toolbar-button-hovering') }\" onMouseLeave=\"document.getElementById('"
      + id + "').classList.remove('toolbar-button-hovering')\"";
  }

  private String messageDialog() {
    String retVal =
      "<div id=\"messageDialogBackdrop\" style=\"visibility: hidden; position: absolute; top: 0; right: 0; bottom: 0; left: 0; opacity: 0.5; background-color: #000; z-index: 1000;\"></div>\n";
    retVal +=
      "<div class=\"hop-dialog\" id=\"messageDialog\" style=\"visibility: hidden; margin: 0; position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); -ms-transform: translate"
        + "(-50%, -50%);"
        + "-webkit-transform: translate(-50%, -50%); padding: 30px; height: auto; width: 423px; border: 1px solid #CCC; -webkit-box-shadow: none; -moz-box-shadow: none;"
        + "box-shadow: none; -webkit-box-sizing: border-box; -moz-box-sizing: border-box; box-sizing: border-box; overflow: hidden; line-height: 20px; background-color: #FFF; z-index: 10000;\">\n";
    retVal += "<div id=\"messageDialogTitle\" class=\"Caption\"></div>\n";
    retVal += "<div id=\"messageDialogBody\" class=\"dialog-content\"></div>\n";
    retVal += "<div id=\"singleButton\" style=\"margin-top: 30px;\">\n<button class=\"hop-button\" style=\"float: right;\" onclick=\"closeMessageDialog( true );\">\n<span>" + BaseMessages
      .getString( PKG, "GetStatusServlet.Button.OK" ) + "</span>\n</button>\n</div>\n";
    retVal += "<div id=\"doubleButton\" style=\"margin-top: 30px;\">\n<button class=\"hop-button\" style=\"float: right; margin-left: 10px;\" onclick=\"closeMessageDialog( false );\">\n<span>"
      + BaseMessages.getString( PKG, "GetStatusServlet.Button.No" )
      + "</span>\n</button>\n<button class=\"hop-button\" style=\"float: right;\" onclick=\"closeMessageDialog( false ); removeSelection();\">\n<span>" + BaseMessages
      .getString( PKG, "GetStatusServlet.Button.YesRemove" ) + "</span>\n</button>\n</div>\n";
    retVal += "</div>\n";
    return retVal;
  }

  private String setupAjaxCall( String uri, String title, String success, String failure ) {
    String retVal = "";
    retVal += "var xhttp = new XMLHttpRequest();\n";
    retVal += "xhttp.onreadystatechange = function() {\n";
    retVal += " if ( this.readyState === 4 ) {\n";
    retVal += "   if ( this.status === 200 ) {\n";
    retVal += "     openMessageDialog( '" + title + "', " + success + ", true );\n";
    retVal += "   } else {\n";
    retVal += "     openMessageDialog( '" + BaseMessages.getString( PKG, "GetStatusServlet.UnableTo.Label" )
      + " " + title + "', " + failure + ", true );\n";
    retVal += "   }\n";
    retVal += " }\n";
    retVal += "};\n";
    retVal += "xhttp.open( \"GET\", " + uri + ", true );\n";
    retVal += "xhttp.send();\n";
    return retVal;
  }

  private String setupPipelineURI( String context ) {
    return "'" + context + "'"
      + " + '?name=' + document.getElementById( 'cellTableFirstCell_' + selectedPipelineRowIndex ).innerHTML"
      + " + '&id=' + document.getElementById( 'cellTableCell_' + selectedPipelineRowIndex ).innerHTML";
  }

  private String setupJobURI( String context ) {
    return "'" + context + "'"
      + " + '?name=' + document.getElementById( 'j-cellTableFirstCell_' + selectedWorkflowRowIndex ).innerHTML"
      + " + '&id=' + document.getElementById( 'j-cellTableCell_' + selectedWorkflowRowIndex ).innerHTML";
  }
}
