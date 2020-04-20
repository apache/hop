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
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionCopy;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

public class AddWorkflowServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static final long serialVersionUID = -6850701762586992604L;

  public static final String CONTEXT_PATH = "/hop/addWorkflow";

  public AddWorkflowServlet() {
  }

  public AddWorkflowServlet( WorkflowMap workflowMap, SocketRepository socketRepository ) {
    super( workflowMap, socketRepository );
  }

  /**
   * /**
   *
   * <div id="mindtouch">
   * <h1>/hop/addJob</h1>
   * <a name="POST"></a>
   * <h2>POST</h2>
   * <p>Uploads and executes workflow configuration XML file.
   * Uploads xml file containing workflow and job_execution_configuration (wrapped in job_configuration tag)
   * to be executed and executes it. Method relies on the input parameter to determine if xml or html
   * reply should be produced. The job_configuration xml is
   * transferred within request body.
   *
   * <code>Workflow name of the executed workflow </code> will be returned in the Response object
   * or <code>message</code> describing error occurred. To determine if the call successful or not you should
   * rely on <code>result</code> parameter in response.</p>
   *
   * <p><b>Example Request:</b><br />
   * <pre function="syntax.xml">
   * POST /hop/addJob/?xml=Y
   * </pre>
   * <p>Request body should contain xml containing job_configuration (workflow + job_execution_configuration
   * wrapped in job_configuration tag).</p>
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
   * <p>Response wraps workflow name that was executed or error stack trace
   * if an error occurred. Response has <code>result</code> OK if there were no errors. Otherwise it returns ERROR.</p>
   *
   * <p><b>Example Response:</b></p>
   * <pre function="syntax.xml">
   * <?xml version="1.0" encoding="UTF-8"?>
   * <webresult>
   * <result>OK</result>
   * <message>Workflow &#x27;dummy_job&#x27; was added to the list with id 1e90eca8-4d4c-47f7-8e5c-99ec36525e7c</message>
   * <id>1e90eca8-4d4c-47f7-8e5c-99ec36525e7c</id>
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
      out.print( XmlHandler.getXMLHeader() );
    } else {
      response.setContentType( "text/html" );
      out.println( "<HTML>" );
      out.println( "<HEAD><TITLE>Add workflow</TITLE></HEAD>" );
      out.println( "<BODY>" );
    }

    response.setStatus( HttpServletResponse.SC_OK );

    try {
      // First read the complete transformation in memory from the request
      int c;
      StringBuilder xml = new StringBuilder();
      while ( ( c = in.read() ) != -1 ) {
        xml.append( (char) c );
      }

      // Parse the XML, create a workflow configuration
      //
      WorkflowConfiguration workflowConfiguration = WorkflowConfiguration.fromXML( xml.toString() );
      WorkflowMeta workflowMeta = workflowConfiguration.getWorkflowMeta();
      WorkflowExecutionConfiguration workflowExecutionConfiguration = workflowConfiguration.getWorkflowExecutionConfiguration();
      workflowMeta.setLogLevel( workflowExecutionConfiguration.getLogLevel() );
      workflowMeta.injectVariables( workflowExecutionConfiguration.getVariablesMap() );

      String carteObjectId = UUID.randomUUID().toString();
      SimpleLoggingObject servletLoggingObject =
        new SimpleLoggingObject( CONTEXT_PATH, LoggingObjectType.HOP_SERVER, null );
      servletLoggingObject.setContainerObjectId( carteObjectId );
      servletLoggingObject.setLogLevel( workflowExecutionConfiguration.getLogLevel() );

      // Create the transformation and store in the list...
      //
      final Workflow workflow = new Workflow( workflowMeta, servletLoggingObject );

      // Setting variables
      //
      workflow.initializeVariablesFrom( null );
      workflow.getWorkflowMeta().setInternalHopVariables( workflow );
      workflow.injectVariables( workflowConfiguration.getWorkflowExecutionConfiguration().getVariablesMap() );

      // Also copy the parameters over...
      //
      workflow.copyParametersFrom( workflowMeta );
      workflow.clearParameters();
      String[] parameterNames = workflow.listParameters();
      for ( int idx = 0; idx < parameterNames.length; idx++ ) {
        // Grab the parameter value set in the action
        //
        String thisValue = workflowExecutionConfiguration.getParametersMap().get( parameterNames[ idx ] );
        if ( !Utils.isEmpty( thisValue ) ) {
          // Set the value as specified by the user in the action
          //
          workflowMeta.setParameterValue( parameterNames[ idx ], thisValue );
        }
      }
      workflowMeta.activateParameters();
      // Check if there is a starting point specified.
      String startCopyName = workflowExecutionConfiguration.getStartCopyName();
      if ( startCopyName != null && !startCopyName.isEmpty() ) {
        int startCopyNr = workflowExecutionConfiguration.getStartCopyNr();
        ActionCopy startActionCopy = workflowMeta.findAction( startCopyName, startCopyNr );
        workflow.setStartActionCopy( startActionCopy );
      }

      workflow.setSocketRepository( getSocketRepository() );

      getWorkflowMap().addWorkflow( workflow.getJobname(), carteObjectId, workflow, workflowConfiguration );


      String message = "Workflow '" + workflow.getJobname() + "' was added to the list with id " + carteObjectId;

      if ( useXML ) {
        out.println( new WebResult( WebResult.STRING_OK, message, carteObjectId ) );
      } else {
        out.println( "<H1>" + message + "</H1>" );
        out.println( "<p><a href=\""
          + convertContextPath( GetWorkflowStatusServlet.CONTEXT_PATH ) + "?name=" + workflow.getJobname() + "&id="
          + carteObjectId + "\">Go to the workflow status page</a><p>" );
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
