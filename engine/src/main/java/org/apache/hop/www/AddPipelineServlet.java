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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannelFileWriter;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.util.FileUtil;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.IExecutionFinishedListener;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
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
      logDebug( "Addition of transformation requested" );
    }

    boolean useXML = "Y".equalsIgnoreCase( request.getParameter( "xml" ) );

    PrintWriter out = response.getWriter();
    BufferedReader in = request.getReader();
    if ( log.isDetailed() ) {
      logDetailed( "Encoding: " + request.getCharacterEncoding() );
    }

    if ( useXML ) {
      response.setContentType( "text/xml" );
      out.print( XmlHandler.getXMLHeader() );
    } else {
      response.setContentType( "text/html" );
      out.println( "<HTML>" );
      out.println( "<HEAD><TITLE>Add transformation</TITLE></HEAD>" );
      out.println( "<BODY>" );
    }

    response.setStatus( HttpServletResponse.SC_OK );

    String realLogFilename = null;
    PipelineExecutionConfiguration pipelineExecutionConfiguration = null;

    try {
      // First read the complete transformation in memory from the request
      //
      StringBuilder xml = new StringBuilder( request.getContentLength() );
      int c;
      while ( ( c = in.read() ) != -1 ) {
        xml.append( (char) c );
      }

      // Parse the XML, create a transformation configuration
      //
      IMetaStore metaStore = pipelineMap.getSlaveServerConfig().getMetaStore();
      PipelineConfiguration pipelineConfiguration = PipelineConfiguration.fromXML( xml.toString(), metaStore );
      PipelineMeta pipelineMeta = pipelineConfiguration.getPipelineMeta();
      pipelineExecutionConfiguration = pipelineConfiguration.getPipelineExecutionConfiguration();
      pipelineMeta.setLogLevel( pipelineExecutionConfiguration.getLogLevel() );
      if ( log.isDetailed() ) {
        logDetailed( "Logging level set to " + log.getLogLevel().getDescription() );
      }
      pipelineMeta.injectVariables( pipelineExecutionConfiguration.getVariablesMap() );

      // Also copy the parameters over...
      //
      Map<String, String> params = pipelineExecutionConfiguration.getParametersMap();
      for ( String param : params.keySet() ) {
        String value = params.get( param );
        pipelineMeta.setParameterValue( param, value );
      }

      String carteObjectId = UUID.randomUUID().toString();
      SimpleLoggingObject servletLoggingObject =
        new SimpleLoggingObject( CONTEXT_PATH, LoggingObjectType.HOP_SERVER, null );
      servletLoggingObject.setContainerObjectId( carteObjectId );
      servletLoggingObject.setLogLevel( pipelineExecutionConfiguration.getLogLevel() );

      String runConfigurationName = pipelineExecutionConfiguration.getRunConfiguration();
      if ( StringUtils.isEmpty(runConfigurationName)) {
        throw new HopException( "We need to know which pipeline run configuration to use to execute the pipeline");
      }
      PipelineRunConfiguration runConfiguration;
      try {
        runConfiguration = PipelineRunConfiguration.createFactory( metaStore ).loadElement( runConfigurationName );
      } catch(Exception e) {
        throw new HopException( "Error loading pipeline run configuration '"+runConfigurationName+"'", e );
      }
      if (runConfiguration==null) {
        throw new HopException( "Pipeline run configuration '"+runConfigurationName+"' could not be found" );
      }
      final IPipelineEngine<PipelineMeta> pipeline = PipelineEngineFactory.createPipelineEngine( runConfiguration, pipelineMeta );
      pipeline.setParent( servletLoggingObject );

      if ( pipelineExecutionConfiguration.isSetLogfile() ) {
        realLogFilename = pipelineExecutionConfiguration.getLogFileName();
        final LogChannelFileWriter logChannelFileWriter;
        try {
          FileUtil.createParentFolder( AddPipelineServlet.class, realLogFilename, pipelineExecutionConfiguration
            .isCreateParentFolder(), pipeline.getLogChannel(), pipeline );
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

      getPipelineMap().addPipeline( pipelineMeta.getName(), carteObjectId, pipeline, pipelineConfiguration );
      pipeline.setContainerId( carteObjectId );

      String message = "Pipeline '" + pipeline.getSubject().getName() + "' was added to HopServer with id " + carteObjectId;

      if ( useXML ) {
        // Return the log channel id as well
        //
        out.println( new WebResult( WebResult.STRING_OK, message, carteObjectId ) );
      } else {
        out.println( "<H1>" + message + "</H1>" );
        out.println( "<p><a href=\""
          + convertContextPath( GetPipelineStatusServlet.CONTEXT_PATH ) + "?name=" + pipeline.getSubject().getName() + "&id="
          + carteObjectId + "\">Go to the transformation status page</a><p>" );
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
