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
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.www.cache.HopServerStatusCache;
import org.owasp.encoder.Encode;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;

@HopServerServlet(id="prepareExec", name = "Prepare the execution of a pipeline")
public class PrepareExecutionPipelineServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static final Class<?> PKG = PrepareExecutionPipelineServlet.class; // For Translator

  private static final long serialVersionUID = 3634806745372015720L;
  public static final String CONTEXT_PATH = "/hop/prepareExec";
  private HopServerStatusCache cache = HopServerStatusCache.getInstance();

  public PrepareExecutionPipelineServlet() {
  }

  public PrepareExecutionPipelineServlet( PipelineMap pipelineMap ) {
    super( pipelineMap );
  }

  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException,
    IOException {
    if ( isJettyMode() && !request.getContextPath().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "PrepareExecutionPipelineServlet.PipelinePrepareExecutionRequested" ) );
    }

    String pipelineName = request.getParameter( "name" );
    String id = request.getParameter( "id" );
    boolean useXML = "Y".equalsIgnoreCase( request.getParameter( "xml" ) );

    response.setStatus( HttpServletResponse.SC_OK );

    PrintWriter out = response.getWriter();
    if ( useXML ) {
      response.setContentType( "text/xml" );
      out.print( XmlHandler.getXmlHeader( Const.XML_ENCODING ) );
    } else {

      response.setCharacterEncoding( "UTF-8" );
      response.setContentType( "text/html;charset=UTF-8" );

      out.println( "<HTML>" );
      out.println( "<HEAD>" );
      out.println( "<TITLE>"
        + BaseMessages.getString( PKG, "PrepareExecutionPipelineServlet.PipelinePrepareExecution" ) + "</TITLE>" );
      out.println( "<META http-equiv=\"Refresh\" content=\"2;url="
        + convertContextPath( GetPipelineStatusServlet.CONTEXT_PATH ) + "?name="
        + URLEncoder.encode( pipelineName, "UTF-8" ) + "\">" );
      out.println( "<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">" );
      out.println( "</HEAD>" );
      out.println( "<BODY>" );
    }

    try {
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

      PipelineConfiguration pipelineConfiguration = getPipelineMap().getConfiguration( entry );

      if ( pipeline != null && pipelineConfiguration != null ) {
        PipelineExecutionConfiguration executionConfiguration = pipelineConfiguration.getPipelineExecutionConfiguration();
        // Set the appropriate logging, variables, arguments, replay date, ...
        // etc.
        pipeline.setVariables( executionConfiguration.getVariablesMap() );
        pipeline.setPreviousResult( executionConfiguration.getPreviousResult() );

        try {
          pipeline.prepareExecution();
          cache.remove( pipeline.getLogChannelId() );

          if ( useXML ) {
            out.println( WebResult.OK.getXml() );
          } else {

            out.println( "<H1>"+ Encode.forHtml( BaseMessages.getString(PKG, "PrepareExecutionPipelineServlet.PipelinePrepared", pipelineName ) ) + "</H1>" );
            out.println( "<a href=\""
              + convertContextPath( GetPipelineStatusServlet.CONTEXT_PATH ) + "?name="
              + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id=" + URLEncoder.encode( id, "UTF-8" ) + "\">"
              + BaseMessages.getString( PKG, "PipelineStatusServlet.BackToPipelineStatusPage" ) + "</a><p>" );
          }
        } catch ( Exception e ) {
          String logText = HopLogStore.getAppender().getBuffer( pipeline.getLogChannel().getLogChannelId(), true ).toString();
          if ( useXML ) {
            out.println( new WebResult( WebResult.STRING_ERROR,
              BaseMessages.getString( PKG, "PrepareExecutionPipelineServlet.Error.PipelineInitFailed", Const.CR  +
                logText + Const.CR + Const.getSimpleStackTrace( e ) + Const.CR + Const.getStackTracker( e ) ) )
            );
          } else {
            out.println( "<H1>" + Encode.forHtml( BaseMessages.getString( PKG, "PrepareExecutionPipelineServlet.Log.PipelineNotInit", pipelineName ) ) + "</H1>" );
            out.println( "<pre>" );
            out.println( Encode.forHtml( logText ) );
            out.println( Encode.forHtml( Const.getStackTracker( e ) ) );
            out.println( "</pre>" );
            out.println( "<a href=\""
              + convertContextPath( GetPipelineStatusServlet.CONTEXT_PATH ) + "?name="
              + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id=" + id + "\">"
              + BaseMessages.getString( PKG, "PipelineStatusServlet.BackToPipelineStatusPage" ) + "</a><p>" );
            response.setStatus( HttpServletResponse.SC_BAD_REQUEST );
          }
        }
      } else {
        if ( useXML ) {
          out.println( new WebResult(
            WebResult.STRING_ERROR,
            BaseMessages.getString( PKG, "PipelineStatusServlet.Log.CoundNotFindSpecPipeline", pipelineName ) )
          );
        } else {
          out.println( "<H1>" + Encode.forHtml( BaseMessages.getString( PKG, "PipelineStatusServlet.Log.CoundNotFindPipeline", pipelineName ) ) + "</H1>" );
          out.println( "<a href=\"" + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + "\">" +
            BaseMessages.getString( PKG, "PipelineStatusServlet.BackToStatusPage" ) +
            "</a><p>" );
          response.setStatus( HttpServletResponse.SC_BAD_REQUEST );
        }
      }
    } catch ( Exception ex ) {
      if ( useXML ) {
        out.println( new WebResult(
          WebResult.STRING_ERROR,
          BaseMessages.getString( PKG, "PrepareExecutionPipelineServlet.Error.UnexpectedError", Const.CR + Const.getStackTracker( ex ) ) )
        );
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
    return "Start pipeline";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

}
