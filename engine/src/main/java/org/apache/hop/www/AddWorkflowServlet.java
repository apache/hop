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
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEngineFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

@HopServerServlet(id="addWorkflow", name = "Add a workflow to the server")
public class AddWorkflowServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static final long serialVersionUID = -6850701762586992604L;

  public static final String CONTEXT_PATH = "/hop/addWorkflow";

  public AddWorkflowServlet() {
  }

  public AddWorkflowServlet( WorkflowMap workflowMap ) {
    super( workflowMap );
  }

  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException,
    IOException {
    if ( isJettyMode() && !request.getRequestURI().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    if ( log.isDebug() ) {
      logDebug( "Addition of workflow requested" );
    }

    boolean useXML = "Y".equalsIgnoreCase( request.getParameter( "xml" ) );

    PrintWriter out = response.getWriter();
    BufferedReader in = request.getReader(); // read from the client
    if ( log.isDetailed() ) {
      logDetailed( "Encoding: " + request.getCharacterEncoding() );
    }

    if ( useXML ) {
      response.setContentType( "text/xml" );
      out.print( XmlHandler.getXmlHeader() );
    } else {
      response.setContentType( "text/html" );
      out.println( "<HTML>" );
      out.println( "<HEAD><TITLE>Add workflow</TITLE></HEAD>" );
      out.println( "<BODY>" );
    }

    response.setStatus( HttpServletResponse.SC_OK );

    try {
      // First read the complete pipeline in memory from the request
      int c;
      StringBuilder xml = new StringBuilder();
      while ( ( c = in.read() ) != -1 ) {
        xml.append( (char) c );
      }

      // Parse the XML, create a workflow configuration
      //
      WorkflowConfiguration workflowConfiguration = WorkflowConfiguration.fromXml( xml.toString(), variables);
      IHopMetadataProvider metadataProvider = workflowConfiguration.getMetadataProvider();
      WorkflowMeta workflowMeta = workflowConfiguration.getWorkflowMeta();
      WorkflowExecutionConfiguration workflowExecutionConfiguration = workflowConfiguration.getWorkflowExecutionConfiguration();
      workflowMeta.setLogLevel( workflowExecutionConfiguration.getLogLevel() );

      String serverObjectId = UUID.randomUUID().toString();
      SimpleLoggingObject servletLoggingObject = new SimpleLoggingObject( CONTEXT_PATH, LoggingObjectType.HOP_SERVER, null );
      servletLoggingObject.setContainerObjectId( serverObjectId );
      servletLoggingObject.setLogLevel( workflowExecutionConfiguration.getLogLevel() );

      // Create the workflow and store in the list...
      //
      String runConfigurationName = workflowExecutionConfiguration.getRunConfiguration();
      final IWorkflowEngine<WorkflowMeta> workflow = WorkflowEngineFactory.createWorkflowEngine( variables, runConfigurationName, metadataProvider, workflowMeta, servletLoggingObject );

      // Setting variables
      //
      workflow.initializeFrom( null );
      workflow.getWorkflowMeta().setInternalHopVariables( workflow );
      workflow.setVariables( workflowConfiguration.getWorkflowExecutionConfiguration().getVariablesMap() );

      // Also copy the parameters over...
      //
      workflow.copyParametersFromDefinitions( workflowMeta );
      workflow.clearParameterValues();
      String[] parameterNames = workflow.listParameters();
      for ( int idx = 0; idx < parameterNames.length; idx++ ) {
        // Grab the parameter value set in the action
        //
        String thisValue = workflowExecutionConfiguration.getParametersMap().get( parameterNames[ idx ] );
        if ( !Utils.isEmpty( thisValue ) ) {
          // Set the value as specified by the user in the action
          //
          workflow.setParameterValue( parameterNames[ idx ], thisValue );
        }
      }
      workflow.activateParameters(workflow);

      // Check if there is a starting point specified.
      String startActionName = workflowExecutionConfiguration.getStartActionName();
      if ( startActionName != null && !startActionName.isEmpty() ) {        
        ActionMeta startActionMeta = workflowMeta.findAction( startActionName );
        workflow.setStartActionMeta( startActionMeta );
      }

      getWorkflowMap().addWorkflow( workflow.getWorkflowName(), serverObjectId, workflow, workflowConfiguration );


      String message = "Workflow '" + workflow.getWorkflowName() + "' was added to the list with id " + serverObjectId;

      if ( useXML ) {
        out.println( new WebResult( WebResult.STRING_OK, message, serverObjectId ) );
      } else {
        out.println( "<H1>" + message + "</H1>" );
        out.println( "<p><a href=\""
          + convertContextPath( GetWorkflowStatusServlet.CONTEXT_PATH ) + "?name=" + workflow.getWorkflowName() + "&id="
          + serverObjectId + "\">Go to the workflow status page</a><p>" );
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

  protected String[] getAllArgumentStrings( Map<String, String> arguments ) {
    if ( arguments == null || arguments.size() == 0 ) {
      return null;
    }

    String[] argNames = arguments.keySet().toArray( new String[ arguments.size() ] );
    Arrays.sort( argNames );

    String[] values = new String[ argNames.length ];
    for ( int i = 0; i < argNames.length; i++ ) {
      values[ i ] = arguments.get( argNames[ i ] );
    }

    return values;
  }

  public String toString() {
    return "Add Workflow";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }
}
