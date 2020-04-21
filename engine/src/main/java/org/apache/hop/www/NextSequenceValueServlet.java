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
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.xml.XmlHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintStream;

public class NextSequenceValueServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static final long serialVersionUID = 3634806745372015720L;

  public static final String CONTEXT_PATH = "/hop/nextSequence";

  public static final String PARAM_NAME = "name";
  public static final String PARAM_INCREMENT = "increment";

  public static final String XML_TAG = "seq";
  public static final String XML_TAG_VALUE = "value";
  public static final String XML_TAG_INCREMENT = "increment";
  public static final String XML_TAG_ERROR = "error";

  public NextSequenceValueServlet() {
  }

  public NextSequenceValueServlet( PipelineMap pipelineMap ) {
    super( pipelineMap );
  }

  /**
   * <div id="mindtouch">
   * <h1>/hop/nextSequence</h1>
   * <a name="GET"></a>
   * <h2>GET</h2>
   * <p>Increments specified pre-configured sequence.
   * Method is used for reserving a number of IDs and incrementing a sequence pre-configured in HopServer server configuration
   * by specified amount. If no increment value provided 10000 is used by default.</p>
   *
   * <p><b>Example Request:</b><br />
   * <pre function="syntax.xml">
   * GET /hop/nextSequence?name=test_seq
   * </pre>
   *
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
   * <td>name</td>
   * <td>name of the sequence specified in HopServer configuration file.</td>
   * <td>query</td>
   * </tr>
   * <tr>
   * <td>increment</td>
   * <td>(optional) parameter used for incrementing sequence. If no parameter specified
   * 10000 is used by default.</td>
   * <td>integer, optional</td>
   * </tr>
   * </tbody>
   * </table>
   *
   * <h3>Response Body</h3>
   *
   * <table class="hop-table">
   * <tbody>
   * <tr>
   * <td align="right">text:</td>
   * <td>HTML</td>
   * </tr>
   * <tr>
   * <td align="right">media types:</td>
   * <td>text/xml</td>
   * </tr>
   * </tbody>
   * </table>
   * <p>Response XML containing sequence value and the increment value used.</p>
   *
   * <p><b>Example Response:</b></p>
   * <pre function="syntax.xml">
   * <seq><value>570000</value><increment>10000</increment></seq>
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
   * <td>Request was processed.</td>
   * </tr>
   * <tr>
   * <td>404</td>
   * <td>If the sequence was not found or error occurred during allocation</td>
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
    if ( isJettyMode() && !request.getContextPath().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    if ( log.isDebug() ) {
      logDebug( toString() );
    }

    String name = request.getParameter( PARAM_NAME );
    long increment = Const.toLong( request.getParameter( PARAM_INCREMENT ), 10000 );

    response.setStatus( HttpServletResponse.SC_OK );
    response.setContentType( "text/xml" );
    response.setCharacterEncoding( Const.XML_ENCODING );

    PrintStream out = new PrintStream( response.getOutputStream() );
    out.println( XmlHandler.getXMLHeader( Const.XML_ENCODING ) );
    out.println( XmlHandler.openTag( XML_TAG ) );

    try {

      SlaveSequence slaveSequence = getPipelineMap().getSlaveSequence( name );
      if ( slaveSequence == null && getPipelineMap().isAutomaticSlaveSequenceCreationAllowed() ) {
        slaveSequence = getPipelineMap().createSlaveSequence( name );
      }
      if ( slaveSequence == null ) {
        response.sendError( HttpServletResponse.SC_NOT_FOUND );
        out.println( XmlHandler.addTagValue( XML_TAG_ERROR, "Slave sequence '" + name + "' could not be found." ) );
      } else {
        ILoggingObject loggingObject = new SimpleLoggingObject( "HopServer", LoggingObjectType.HOP_SERVER, null );
        long nextValue = slaveSequence.getNextValue( loggingObject, increment );
        out.println( XmlHandler.addTagValue( XML_TAG_VALUE, nextValue ) );
        out.println( XmlHandler.addTagValue( XML_TAG_INCREMENT, increment ) );
      }

    } catch ( Exception e ) {
      response.sendError( HttpServletResponse.SC_NOT_FOUND );
      out.println( XmlHandler.addTagValue( XML_TAG_ERROR, "Error retrieving next value from slave sequence: "
        + Const.getStackTracker( e ) ) );
    }

    out.println( XmlHandler.closeTag( XML_TAG ) );
  }

  public String toString() {
    return "Retrieve the next value of slave server sequence requested.";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

}
