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

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

public class BaseHttpServlet extends HttpServlet {

  protected static final long serialVersionUID = -1348342810327662788L;

  protected PipelineMap pipelineMap;
  protected WorkflowMap workflowMap;
  protected HopServerConfig serverConfig;
  protected IVariables variables;

  private boolean jettyMode = false;

  protected ILogChannel log = new LogChannel( "Servlet" );

  public String convertContextPath( String contextPath ) {
    if ( jettyMode ) {
      return contextPath;
    }
    return contextPath.substring( contextPath.lastIndexOf( "/" ) + 1 );
  }

  public BaseHttpServlet() {
  }

  public BaseHttpServlet( PipelineMap pipelineMap ) {
    this.pipelineMap = pipelineMap;
    this.jettyMode = true;
    this.serverConfig = pipelineMap.getHopServerConfig();
    if (serverConfig==null) {
      this.variables = Variables.getADefaultVariableSpace();
    } else {
      this.variables = serverConfig.getVariables();
    }
  }

  public BaseHttpServlet( WorkflowMap workflowMap ) {
    this.workflowMap = workflowMap;
    this.jettyMode = true;
    this.serverConfig = workflowMap.getHopServerConfig();
    if (serverConfig==null) {
      this.variables = Variables.getADefaultVariableSpace();
    } else {
      this.variables = serverConfig.getVariables();
    }
  }

  public BaseHttpServlet( PipelineMap pipelineMap, WorkflowMap workflowMap ) {
    this.pipelineMap = pipelineMap;
    this.workflowMap = workflowMap;
    this.jettyMode = true;
    this.serverConfig = pipelineMap.getHopServerConfig();
    if (serverConfig==null) {
      this.variables = Variables.getADefaultVariableSpace();
    } else {
      this.variables = serverConfig.getVariables();
    }
  }

  protected void doPut( HttpServletRequest request, HttpServletResponse response ) throws ServletException,
    IOException {
    doGet( request, response );
  }

  protected void doPost( HttpServletRequest request, HttpServletResponse response ) throws ServletException,
    IOException {
    doGet( request, response );
  }

  protected void doDelete( HttpServletRequest req, HttpServletResponse resp ) throws ServletException, IOException {
    doGet( req, resp );
  }

  public PipelineMap getPipelineMap() {
    if ( pipelineMap == null ) {
      return HopServerSingleton.getInstance().getPipelineMap();
    }
    return pipelineMap;
  }

  public WorkflowMap getWorkflowMap() {
    if ( workflowMap == null ) {
      return HopServerSingleton.getInstance().getWorkflowMap();
    }
    return workflowMap;
  }

  public boolean isJettyMode() {
    return jettyMode;
  }

  public void setJettyMode( boolean jettyMode ) {
    this.jettyMode = jettyMode;
  }

  public void logMinimal( String s ) {
    log.logMinimal( s );
  }

  public void logBasic( String s ) {
    log.logBasic( s );
  }

  public void logError( String s ) {
    log.logError( s );
  }

  public void logError( String s, Throwable e ) {
    log.logError( s, e );
  }

  public void logBasic( String s, Object... arguments ) {
    log.logBasic( s, arguments );
  }

  public void logDetailed( String s, Object... arguments ) {
    log.logDetailed( s, arguments );
  }

  public void logError( String s, Object... arguments ) {
    log.logError( s, arguments );
  }

  public void logDetailed( String s ) {
    log.logDetailed( s );
  }

  public void logDebug( String s ) {
    log.logDebug( s );
  }

  public void logRowlevel( String s ) {
    log.logRowlevel( s );
  }

  public void setup( PipelineMap pipelineMap, WorkflowMap workflowMap ) {
    this.pipelineMap = pipelineMap;
    this.workflowMap = workflowMap;
    this.serverConfig = pipelineMap.getHopServerConfig();
    this.variables = serverConfig.getVariables();
  }

  /**
   * @param pipelineMap The pipelineMap to set
   */
  public void setPipelineMap( PipelineMap pipelineMap ) {
    this.pipelineMap = pipelineMap;
  }

  /**
   * @param workflowMap The workflowMap to set
   */
  public void setWorkflowMap( WorkflowMap workflowMap ) {
    this.workflowMap = workflowMap;
  }

  /**
   * Gets serverConfig
   *
   * @return value of serverConfig
   */
  public HopServerConfig getServerConfig() {
    return serverConfig;
  }

  /**
   * @param serverConfig The serverConfig to set
   */
  public void setServerConfig( HopServerConfig serverConfig ) {
    this.serverConfig = serverConfig;
  }

  /**
   * Gets log
   *
   * @return value of log
   */
  public ILogChannel getLog() {
    return log;
  }

  /**
   * @param log The log to set
   */
  public void setLog( ILogChannel log ) {
    this.log = log;
  }
}
