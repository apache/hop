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
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.owasp.encoder.Encode;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;

@HopServerServlet(id="stopWorkflow", name = "Stop a workflow")
public class StopWorkflowServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static final Class<?> PKG = StopWorkflowServlet.class; // For Translator

  private static final long serialVersionUID = 3634806745372015720L;
  public static final String CONTEXT_PATH = "/hop/stopWorkflow";

  public StopWorkflowServlet() {
  }

  public StopWorkflowServlet( WorkflowMap workflowMap ) {
    super( workflowMap );
  }

  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException,
    IOException {
    if ( isJettyMode() && !request.getContextPath().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "StopWorkflowServlet.log.StopWorkflowRequested" ) );
    }

    String workflowName = request.getParameter( "name" );
    String id = request.getParameter( "id" );
    boolean useXML = "Y".equalsIgnoreCase( request.getParameter( "xml" ) );

    PrintWriter out = response.getWriter();
    try {
      if ( useXML ) {
        response.setContentType( "text/xml" );
        response.setCharacterEncoding( Const.XML_ENCODING );
        out.print( XmlHandler.getXmlHeader( Const.XML_ENCODING ) );
      } else {
        response.setContentType( "text/html;charset=UTF-8" );
        out.println( "<HTML>" );
        out.println( "<HEAD>" );
        out.println( "<TITLE>Stop workflow</TITLE>" );
        out.println( "<META http-equiv=\"Refresh\" content=\"2;url="
          + convertContextPath( GetWorkflowStatusServlet.CONTEXT_PATH ) + "?name="
          + URLEncoder.encode( workflowName, "UTF-8" ) + "\">" );
        out.println( "<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">" );
        out.println( "</HEAD>" );
        out.println( "<BODY>" );
      }

      // ID is optional...
      //
      IWorkflowEngine<WorkflowMeta> workflow;
      HopServerObjectEntry entry;
      if ( Utils.isEmpty( id ) ) {
        // get the first workflow that matches...
        //
        entry = getWorkflowMap().getFirstHopServerObjectEntry( workflowName );
        if ( entry == null ) {
          workflow = null;
        } else {
          id = entry.getId();
          workflow = getWorkflowMap().getWorkflow( entry );
        }
      } else {
        // Take the ID into account!
        //
        entry = new HopServerObjectEntry( workflowName, id );
        workflow = getWorkflowMap().getWorkflow( entry );
      }

      if ( workflow != null ) {
        workflow.stopExecution();

        String message = BaseMessages.getString( PKG, "WorkflowStatusServlet.Log.WorkflowStopRequested", workflowName );
        if ( useXML ) {
          out.println( new WebResult( WebResult.STRING_OK, message ).getXml() );
        } else {
          out.println( "<H1>" + Encode.forHtml( message ) + "</H1>" );
          out.println( "<a href=\""
            + convertContextPath( GetWorkflowStatusServlet.CONTEXT_PATH ) + "?name="
            + URLEncoder.encode( workflowName, "UTF-8" ) + "&id=" + URLEncoder.encode( id, "UTF-8" ) + "\">"
            + BaseMessages.getString( PKG, "WorkflowStatusServlet.BackToWorkflowStatusPage" ) + "</a><p>" );
        }
      } else {
        String message = BaseMessages.getString( PKG, "StopWorkflowServlet.Log.CoundNotFindWorkflow", workflowName );
        if ( useXML ) {
          out.println( new WebResult( WebResult.STRING_ERROR, message ).getXml() );
        } else {
          out.println( "<H1>" + Encode.forHtml( message ) + "</H1>" );
          out.println( "<a href=\""
            + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + ">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.BackToStatusPage" ) + "</a><p>" );
          response.setStatus( HttpServletResponse.SC_BAD_REQUEST );
        }
      }
    } catch ( Exception ex ) {
      if ( useXML ) {
        out.println( new WebResult( WebResult.STRING_ERROR, Const.getStackTracker( ex ) ).getXml() );
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
    return "Stop workflow";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

}
