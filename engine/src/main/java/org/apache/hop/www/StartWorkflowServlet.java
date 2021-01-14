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
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEngineFactory;
import org.apache.hop.www.cache.HopServerStatusCache;
import org.owasp.encoder.Encode;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.UUID;

@HopServerServlet(id="startWorkflow", name = "Start a workflow")
public class StartWorkflowServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static final Class<?> PKG = StartWorkflowServlet.class; // For Translator
  // needed by
  // Translator!!

  private static final long serialVersionUID = -8487225953910464032L;

  public static final String CONTEXT_PATH = "/hop/startWorkflow";

  @VisibleForTesting
  HopServerStatusCache cache = HopServerStatusCache.getInstance();

  public StartWorkflowServlet() {
  }

  public StartWorkflowServlet( WorkflowMap workflowMap ) {
    super( workflowMap );
  }

  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException,
    IOException {
    if ( isJettyMode() && !request.getContextPath().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "StartWorkflowServlet.Log.StartWorkflowRequested" ) );
    }

    String workflowName = request.getParameter( "name" );
    String id = request.getParameter( "id" );
    boolean useXML = "Y".equalsIgnoreCase( request.getParameter( "xml" ) );

    response.setStatus( HttpServletResponse.SC_OK );

    PrintWriter out = response.getWriter();
    if ( useXML ) {
      response.setContentType( "text/xml" );
      response.setCharacterEncoding( Const.XML_ENCODING );
      out.print( XmlHandler.getXmlHeader( Const.XML_ENCODING ) );
    } else {
      response.setContentType( "text/html;charset=UTF-8" );
      out.println( "<HTML>" );
      out.println( "<HEAD>" );
      out.println( "<TITLE>Start workflow</TITLE>" );
      out.println( "<META http-equiv=\"Refresh\" content=\"2;url="
        + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + "?name=" + URLEncoder.encode( workflowName, "UTF-8" )
        + "\">" );
      out.println( "<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">" );
      out.println( "</HEAD>" );
      out.println( "<BODY>" );
    }

    try {
      // ID is optional...
      //
      IWorkflowEngine<WorkflowMeta> workflow = getWorkflowMap().findWorkflow(workflowName, id);

      if ( workflow != null ) {
        // First see if this workflow already ran to completion.
        // If so, we get an exception is we try to start() the workflow thread
        //
        if ( workflow.isInitialized() && !workflow.isActive() ) {
          // Re-create the workflow from the workflowMeta
          //

          cache.remove( workflow.getLogChannelId() );

          // Create a new workflow object to start from a sane state. Then replace
          // the new workflow in the workflow map
          //
          synchronized ( this ) {
            WorkflowConfiguration workflowConfiguration = getWorkflowMap().getConfiguration( workflowName );
            IHopMetadataProvider metadataProvider = workflowConfiguration.getMetadataProvider();

            String serverObjectId = UUID.randomUUID().toString();
            SimpleLoggingObject servletLoggingObject = new SimpleLoggingObject( CONTEXT_PATH, LoggingObjectType.HOP_SERVER, null );
            servletLoggingObject.setContainerObjectId( serverObjectId );

            String runConfigurationName = workflowConfiguration.getWorkflowExecutionConfiguration().getRunConfiguration();
            IWorkflowEngine<WorkflowMeta> newWorkflow = WorkflowEngineFactory.createWorkflowEngine( variables, runConfigurationName, metadataProvider, workflow.getWorkflowMeta(), servletLoggingObject );
            newWorkflow.setLogLevel( workflow.getLogLevel() );

            // TODO: start workflow with parameters and variables?
            //
            newWorkflow.activateParameters( newWorkflow );

            // Discard old log lines from the old workflow
            //
            HopLogStore.discardLines( workflow.getLogChannelId(), true );

            getWorkflowMap().replaceWorkflow( workflow, newWorkflow, workflowConfiguration );
            workflow = newWorkflow;
          }
        }

        runWorkflow( workflow );

        String message = BaseMessages.getString( PKG, "StartWorkflowServlet.Log.WorkflowStarted", workflowName );
        if ( useXML ) {
          out.println( new WebResult( WebResult.STRING_OK, message, id ).getXml() );
        } else {

          out.println( "<H1>" + Encode.forHtml( message ) + "</H1>" );
          out.println( "<a href=\""
            + convertContextPath( GetWorkflowStatusServlet.CONTEXT_PATH ) + "?name="
            + URLEncoder.encode( workflowName, "UTF-8" ) + "&id=" + URLEncoder.encode( id, "UTF-8" ) + "\">"
            + BaseMessages.getString( PKG, "WorkflowStatusServlet.BackToWorkflowStatusPage" ) + "</a><p>" );
        }
      } else {
        String message = BaseMessages.getString( PKG, "StartWorkflowServlet.Log.SpecifiedWorkflowNotFound", workflowName );
        if ( useXML ) {
          out.println( new WebResult( WebResult.STRING_ERROR, message ) );
        } else {
          out.println( "<H1>" + Encode.forHtml( message ) + "</H1>" );
          out.println( "<a href=\""
            + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + "\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.BackToStatusPage" ) + "</a><p>" );
          response.setStatus( HttpServletResponse.SC_BAD_REQUEST );
        }
      }
    } catch ( Exception ex ) {
      if ( useXML ) {
        out.println( new WebResult( WebResult.STRING_ERROR, BaseMessages.getString(
          PKG, "StartWorkflowServlet.Error.UnexpectedError", Const.CR + Const.getStackTracker( ex ) ) ) );
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
    return "Start workflow";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  protected void runWorkflow( final IWorkflowEngine workflow ) throws HopException {
    new Thread( () -> workflow.startExecution() ).start();
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

}
