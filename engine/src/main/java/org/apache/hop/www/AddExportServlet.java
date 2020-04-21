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
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.w3c.dom.Document;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Map;
import java.util.UUID;

/**
 * This servlet allows you to transport an exported workflow or transformation over to the carte server as a zip file. It
 * ends up in a temporary file.
 * <p>
 * The servlet returns the name of the file stored.
 *
 * @author matt
 */
public class AddExportServlet extends BaseHttpServlet implements IHopServerPlugin {
  public static final String PARAMETER_LOAD = "load";
  public static final String PARAMETER_TYPE = "type";

  public static final String TYPE_JOB = "workflow";
  public static final String TYPE_PIPELINE = "pipeline";

  private static final long serialVersionUID = -6850701762586992604L;
  public static final String CONTEXT_PATH = "/hop/addExport";

  public AddExportServlet() {
  }

  public AddExportServlet( WorkflowMap workflowMap, PipelineMap transformationMap ) {
    super( transformationMap, workflowMap );
  }

  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException,
    IOException {
    if ( isJettyMode() && !request.getRequestURI().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    if ( log.isDebug() ) {
      logDebug( "Addition of export requested" );
    }

    PrintWriter out = response.getWriter();
    InputStream in = request.getInputStream(); // read from the client
    if ( log.isDetailed() ) {
      logDetailed( "Encoding: " + request.getCharacterEncoding() );
    }

    boolean isJob = TYPE_JOB.equalsIgnoreCase( request.getParameter( PARAMETER_TYPE ) );
    String load = request.getParameter( PARAMETER_LOAD ); // the resource to load

    response.setContentType( "text/xml" );
    out.print( XmlHandler.getXMLHeader() );

    response.setStatus( HttpServletResponse.SC_OK );

    OutputStream outputStream = null;

    try {
      FileObject tempFile = HopVfs.createTempFile( "export", ".zip", System.getProperty( "java.io.tmpdir" ) );
      outputStream = HopVfs.getOutputStream( tempFile, false );

      // Pass the input directly to a temporary file
      //
      // int size = 0;
      int c;
      while ( ( c = in.read() ) != -1 ) {
        outputStream.write( c );
        // size++;
      }

      outputStream.flush();
      outputStream.close();
      outputStream = null; // don't close it twice

      String archiveUrl = tempFile.getName().toString();
      String fileUrl = null;

      String serverObjectId = null;
      SimpleLoggingObject servletLoggingObject = new SimpleLoggingObject( CONTEXT_PATH, LoggingObjectType.HOP_SERVER, null );

      // Now open the top level resource...
      //
      if ( !Utils.isEmpty( load ) ) {

        fileUrl = "zip:" + archiveUrl + "!" + load;

        if ( isJob ) {
          // Open the workflow from inside the ZIP archive
          //
          HopVfs.getFileObject( fileUrl );

          WorkflowMeta workflowMeta = new WorkflowMeta( fileUrl );

          // Also read the execution configuration information
          //
          String configUrl = "zip:" + archiveUrl + "!" + Workflow.CONFIGURATION_IN_EXPORT_FILENAME;
          Document configDoc = XmlHandler.loadXmlFile( configUrl );
          WorkflowExecutionConfiguration workflowExecutionConfiguration = new WorkflowExecutionConfiguration( XmlHandler.getSubNode( configDoc, WorkflowExecutionConfiguration.XML_TAG ) );

          serverObjectId = UUID.randomUUID().toString();
          servletLoggingObject.setContainerObjectId( serverObjectId );
          servletLoggingObject.setLogLevel( workflowExecutionConfiguration.getLogLevel() );

          Workflow workflow = new Workflow( workflowMeta, servletLoggingObject );

          // store it all in the map...
          //
          getWorkflowMap().addWorkflow( workflow.getJobname(), serverObjectId, workflow, new WorkflowConfiguration( workflowMeta, workflowExecutionConfiguration ) );

          // Apply the execution configuration...
          //
          log.setLogLevel( workflowExecutionConfiguration.getLogLevel() );
          workflowMeta.injectVariables( workflowExecutionConfiguration.getVariablesMap() );

          // Also copy the parameters over...
          //
          Map<String, String> params = workflowExecutionConfiguration.getParametersMap();
          for ( String param : params.keySet() ) {
            String value = params.get( param );
            workflowMeta.setParameterValue( param, value );
          }

        } else {
          // Open the transformation from inside the ZIP archive
          //
          IMetaStore metaStore = pipelineMap.getSlaveServerConfig().getMetaStore();
          PipelineMeta pipelineMeta = new PipelineMeta( fileUrl, metaStore, true, Variables.getADefaultVariableSpace() );

          // Also read the execution configuration information
          //
          String configUrl = "zip:" + archiveUrl + "!" + Pipeline.CONFIGURATION_IN_EXPORT_FILENAME;
          Document configDoc = XmlHandler.loadXmlFile( configUrl );
          PipelineExecutionConfiguration executionConfiguration = new PipelineExecutionConfiguration( XmlHandler.getSubNode( configDoc, PipelineExecutionConfiguration.XML_TAG ) );

          serverObjectId = UUID.randomUUID().toString();
          servletLoggingObject.setContainerObjectId( serverObjectId );
          servletLoggingObject.setLogLevel( executionConfiguration.getLogLevel() );

          String runConfigurationName = executionConfiguration.getRunConfiguration();
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
          IPipelineEngine<PipelineMeta> pipeline = PipelineEngineFactory.createPipelineEngine( runConfiguration, pipelineMeta );
          pipeline.setParent( servletLoggingObject );

          // store it all in the map...
          //
          getPipelineMap().addPipeline(
            pipeline.getSubject().getName(),
            serverObjectId,
            pipeline,
            new PipelineConfiguration( pipelineMeta, executionConfiguration )
          );
        }
      } else {
        fileUrl = archiveUrl;
      }

      out.println( new WebResult( WebResult.STRING_OK, fileUrl, serverObjectId ) );
    } catch ( Exception ex ) {
      out.println( new WebResult( WebResult.STRING_ERROR, Const.getStackTracker( ex ) ) );
    } finally {
      if ( outputStream != null ) {
        outputStream.close();
      }
    }
  }

  public String toString() {
    return "Add export";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }
}
