/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.commons.io.IOUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.json.simple.parser.ParseException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class RegisterWorkflowServlet extends BaseWorkflowServlet {

  private static final long serialVersionUID = 7416802722393075758L;
  public static final String CONTEXT_PATH = "/hop/registerWorkflow";

  @Override
  public String getContextPath() {
    return CONTEXT_PATH;
  }

  @Override
  WebResult generateBody( HttpServletRequest request, HttpServletResponse response, boolean useXML ) throws IOException, HopException, HopException, ParseException {

    final String xml = IOUtils.toString( request.getInputStream() );

    // Parse the XML, create a workflow configuration
    WorkflowConfiguration workflowConfiguration = WorkflowConfiguration.fromXML( xml );

    IWorkflowEngine<WorkflowMeta> workflow = createWorkflow( workflowConfiguration );

    String message = "Workflow '" + workflow.getWorkflowName() + "' was added to the list with id " + workflow.getContainerId();
    return new WebResult( WebResult.STRING_OK, message, workflow.getContainerId() );
  }
}
