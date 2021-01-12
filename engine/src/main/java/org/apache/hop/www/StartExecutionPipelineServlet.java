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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.owasp.encoder.Encode;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;

@HopServerServlet(id="startExec", name = "Start the execution of a pipeline")
public class StartExecutionPipelineServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static final Class<?> PKG = StartExecutionPipelineServlet.class; // For Translator

  private static final long serialVersionUID = 3634806745372015720L;
  public static final String CONTEXT_PATH = "/hop/startExec";

  public StartExecutionPipelineServlet() {
  }

  public StartExecutionPipelineServlet( PipelineMap pipelineMap ) {
    super( pipelineMap );
  }

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
      out.print( XmlHandler.getXmlHeader( Const.XML_ENCODING ) );
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

  protected void startThreads( IPipelineEngine<PipelineMeta> pipeline ) throws HopException {
    pipeline.startThreads();
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

}
