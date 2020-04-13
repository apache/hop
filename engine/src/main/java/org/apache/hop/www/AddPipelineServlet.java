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
import org.apache.hop.core.logging.LogChannelFileWriter;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.util.FileUtil;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.IExecutionFinishedListener;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;

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

  public AddPipelineServlet( PipelineMap pipelineMap, SocketRepository socketRepository ) {
    super( pipelineMap, socketRepository );
  }

  /**
   * <div id="mindtouch">
   * <h1>/hop/addPipeline</h1>
   * <a name="POST"></a>
   * <h2>POST</h2>
   * <p>Uploads and executes transformation configuration XML file.
   * Uploads xml file containing transformation and transformation_execution_configuration
   * (wrapped in transformation_configuration tag) to be executed and executes it. Method relies
   * on the input parameter to determine if xml or html reply should be produced. The transformation_configuration xml is
   * transferred within request body.
   *
   * <code>transformation name of the executed transformation </code> will be returned in the Response object
   * or <code>message</code> describing error occurred. To determine if the call successful or not you should
   * rely on <code>result</code> parameter in response.</p>
   *
   * <p><b>Example Request:</b><br />
   * <pre function="syntax.xml">
   * POST /hop/addPipeline/?xml=Y
   * </pre>
   * <p>Request body should contain xml containing transformation_configuration (transformation and
   * transformation_execution_configuration wrapped in transformation_configuration tag).</p>
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
   * <td>xml</td>
   * <td>Boolean flag set to either <code>Y</code> or <code>N</code> describing if xml or html reply
   * should be produced.</td>
   * <td>boolean, optional</td>
   * </tr>
   * </tbody>
   * </table>
   *
   * <h3>Response Body</h3>
   *
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
   * <p>Response wraps transformation name that was executed or error stack trace
   * if an error occurred. Response has <code>result</code> OK if there were no errors. Otherwise it returns ERROR.</p>
   *
   * <p><b>Example Response:</b></p>
   * <pre function="syntax.xml">
   * <?xml version="1.0" encoding="UTF-8"?>
   * <webresult>
   * <result>OK</result>
   * <message>Pipeline &#x27;dummy-pipeline&#x27; was added to HopServer with id eb4a92ff-6852-4307-9f74-3c74bd61f829</message>
   * <id>eb4a92ff-6852-4307-9f74-3c74bd61f829</id>
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
   * <td>Request was processed and XML response is returned.</td>
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
      PipelineConfiguration pipelineConfiguration = PipelineConfiguration.fromXML( xml.toString() );
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
        new SimpleLoggingObject( CONTEXT_PATH, LoggingObjectType.CARTE, null );
      servletLoggingObject.setContainerObjectId( carteObjectId );
      servletLoggingObject.setLogLevel( pipelineExecutionConfiguration.getLogLevel() );

      // Create the transformation and store in the list...
      //
      final Pipeline pipeline = new Pipeline( pipelineMeta, servletLoggingObject );

      if ( pipelineExecutionConfiguration.isSetLogfile() ) {
        realLogFilename = pipelineExecutionConfiguration.getLogFileName();
        final LogChannelFileWriter logChannelFileWriter;
        try {
          FileUtil.createParentFolder( AddPipelineServlet.class, realLogFilename, pipelineExecutionConfiguration
            .isCreateParentFolder(), pipeline.getLogChannel(), pipeline );
          logChannelFileWriter =
            new LogChannelFileWriter( servletLoggingObject.getLogChannelId(), HopVfs
              .getFileObject( realLogFilename ), pipelineExecutionConfiguration.isSetAppendLogfile() );
          logChannelFileWriter.startLogging();

          pipeline.addExecutionFinishedListener( (IExecutionFinishedListener<PipelineMeta>) pipelineEngine -> {
            if ( logChannelFileWriter != null ) {
              logChannelFileWriter.stopLogging();
            }
          } );

        } catch ( HopException e ) {
          logError( Const.getStackTracker( e ) );
        }

      }

      pipeline.setSocketRepository( getSocketRepository() );

      getPipelineMap().addPipeline( pipelineMeta.getName(), carteObjectId, pipeline, pipelineConfiguration );
      pipeline.setContainerObjectId( carteObjectId );

      String message = "Pipeline '" + pipeline.getName() + "' was added to HopServer with id " + carteObjectId;

      if ( useXML ) {
        // Return the log channel id as well
        //
        out.println( new WebResult( WebResult.STRING_OK, message, carteObjectId ) );
      } else {
        out.println( "<H1>" + message + "</H1>" );
        out.println( "<p><a href=\""
          + convertContextPath( GetPipelineStatusServlet.CONTEXT_PATH ) + "?name=" + pipeline.getName() + "&id="
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
