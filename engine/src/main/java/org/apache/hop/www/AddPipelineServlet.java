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
import org.apache.hop.core.logging.LogChannelFileWriter;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.util.FileUtil;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.UUID;

@HopServerServlet(id="addPipeline", name = "Add a pipeline for execution")
public class AddPipelineServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static final long serialVersionUID = -6850701762586992604L;

  public static final String CONTEXT_PATH = "/hop/addPipeline";

  public AddPipelineServlet() {
  }

  public AddPipelineServlet( PipelineMap pipelineMap ) {
    super( pipelineMap );
  }

  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException,
    IOException {
    if ( isJettyMode() && !request.getRequestURI().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    if ( log.isDebug() ) {
      logDebug( "Addition of pipeline requested" );
    }

    boolean useXML = "Y".equalsIgnoreCase( request.getParameter( "xml" ) );

    PrintWriter out = response.getWriter();
    BufferedReader in = request.getReader();
    if ( log.isDetailed() ) {
      logDetailed( "Encoding: " + request.getCharacterEncoding() );
    }

    if ( useXML ) {
      response.setContentType( "text/xml" );
      out.print( XmlHandler.getXmlHeader() );
    } else {
      response.setContentType( "text/html" );
      out.println( "<HTML>" );
      out.println( "<HEAD><TITLE>Add pipeline</TITLE></HEAD>" );
      out.println( "<BODY>" );
    }

    response.setStatus( HttpServletResponse.SC_OK );

    String realLogFilename = null;
    PipelineExecutionConfiguration pipelineExecutionConfiguration = null;

    try {
      // First read the complete pipeline in memory from the request
      //
      StringBuilder xml = new StringBuilder( request.getContentLength() );
      int c;
      while ( ( c = in.read() ) != -1 ) {
        xml.append( (char) c );
      }

      // Parse the XML, create a pipeline configuration
      //
      PipelineConfiguration pipelineConfiguration = PipelineConfiguration.fromXml( xml.toString() );
      PipelineMeta pipelineMeta = pipelineConfiguration.getPipelineMeta();
      pipelineExecutionConfiguration = pipelineConfiguration.getPipelineExecutionConfiguration();
      pipelineMeta.setLogLevel( pipelineExecutionConfiguration.getLogLevel() );
      if ( log.isDetailed() ) {
        logDetailed( "Logging level set to " + log.getLogLevel().getDescription() );
      }

      String serverObjectId = UUID.randomUUID().toString();
      SimpleLoggingObject servletLoggingObject =
        new SimpleLoggingObject( CONTEXT_PATH, LoggingObjectType.HOP_SERVER, null );
      servletLoggingObject.setContainerObjectId( serverObjectId );
      servletLoggingObject.setLogLevel( pipelineExecutionConfiguration.getLogLevel() );

      IHopMetadataProvider metadataProvider = pipelineConfiguration.getMetadataProvider();

      String runConfigurationName = pipelineExecutionConfiguration.getRunConfiguration();
      final IPipelineEngine<PipelineMeta> pipeline = PipelineEngineFactory.createPipelineEngine( variables, runConfigurationName, metadataProvider, pipelineMeta );
      pipeline.setParent( servletLoggingObject );

      if ( pipelineExecutionConfiguration.isSetLogfile() ) {
        realLogFilename = pipelineExecutionConfiguration.getLogFileName();
        final LogChannelFileWriter logChannelFileWriter;
        try {
          FileUtil.createParentFolder( AddPipelineServlet.class, realLogFilename, pipelineExecutionConfiguration
            .isCreateParentFolder(), pipeline.getLogChannel() );
          logChannelFileWriter =
            new LogChannelFileWriter( servletLoggingObject.getLogChannelId(), HopVfs.getFileObject( realLogFilename ), pipelineExecutionConfiguration.isSetAppendLogfile() );
          logChannelFileWriter.startLogging();

          pipeline.addExecutionFinishedListener( pipelineEngine -> {
            if ( logChannelFileWriter != null ) {
              logChannelFileWriter.stopLogging();
            }
          } );

        } catch ( HopException e ) {
          logError( Const.getStackTracker( e ) );
        }

      }

      getPipelineMap().addPipeline( pipelineMeta.getName(), serverObjectId, pipeline, pipelineConfiguration );
      pipeline.setContainerId( serverObjectId );

      String message = "Pipeline '" + pipeline.getPipelineMeta().getName() + "' was added to HopServer with id " + serverObjectId;

      if ( useXML ) {
        // Return the log channel id as well
        //
        out.println( new WebResult( WebResult.STRING_OK, message, serverObjectId ) );
      } else {
        out.println( "<H1>" + message + "</H1>" );
        out.println( "<p><a href=\""
          + convertContextPath( GetPipelineStatusServlet.CONTEXT_PATH ) + "?name=" + pipeline.getPipelineMeta().getName() + "&id="
          + serverObjectId + "\">Go to the pipeline status page</a><p>" );
      }
    } catch ( Exception ex ) {
      if ( useXML ) {
        out.println( new WebResult( WebResult.STRING_ERROR, Const.getStackTracker( ex ) ) );
      } else {
        out.println( "<p>" );
        out.println( "<pre>" );
        ex.printStackTrace( out );
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
    return "Add Pipeline";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }
}
