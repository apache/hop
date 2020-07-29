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
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.util.ExecutorUtil;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintStream;

@HopServerServlet( id = "StopHopServerServlet", name = "StopHopServerServlet" )
public class StopHopServerServlet extends BaseHttpServlet implements IHopServerPlugin {

  private static Class<?> PKG = StopHopServerServlet.class;

  private static final long serialVersionUID = -5459379367791045161L;
  public static final String CONTEXT_PATH = "/hop/stopHopServer";
  public static final String REQUEST_ACCEPTED = "request_accepted";
  private final DelayedExecutor delayedExecutor;

  public StopHopServerServlet() {
    this( new DelayedExecutor() );
  }

  public StopHopServerServlet( DelayedExecutor delayedExecutor ) {
    this.delayedExecutor = delayedExecutor;
  }

  @Override
  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  @Override
  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws IOException {
    if ( isJettyMode() && !request.getContextPath().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "StopHopServerServlet.shutdownRequest" ) );
    }

    response.setStatus( HttpServletResponse.SC_OK );
    boolean useXML = "Y".equalsIgnoreCase( request.getParameter( "xml" ) );

    if ( useXML ) {
      response.setContentType( "text/xml" );
      response.setCharacterEncoding( Const.XML_ENCODING );
    } else {
      response.setContentType( "text/html" );
    }

    PrintStream out = new PrintStream( response.getOutputStream() );
    final HopServer hopServer = HopServerSingleton.getHopServer();
    if ( useXML ) {
      out.print( XmlHandler.getXmlHeader( Const.XML_ENCODING ) );
      out.print( XmlHandler.addTagValue( REQUEST_ACCEPTED, hopServer != null ) );
      out.flush();
    } else {
      out.println( "<HTML>" );
      out.println(
        "<HEAD><TITLE>" + BaseMessages.getString( PKG, "StopHopServerServlet.shutdownRequest" ) + "</TITLE></HEAD>" );
      out.println( "<BODY>" );
      out.println( "<H1>" + BaseMessages.getString( PKG, "StopHopServerServlet.status.label" ) + "</H1>" );
      out.println( "<p>" );
      if ( hopServer != null ) {
        out.println( BaseMessages.getString( PKG, "StopHopServerServlet.shutdownRequest.status.ok" ) );
      } else {
        out.println( BaseMessages.getString( PKG, "StopHopServerServlet.shutdownRequest.status.notFound" ) );
      }
      out.println( "</p>" );
      out.println( "</BODY>" );
      out.println( "</HTML>" );
      out.flush();
    }
    if ( hopServer != null ) {
      delayedExecutor.execute( () -> {
        hopServer.getWebServer().stopServer();
        exitJVM( 0 );
      }, 1000 );
    }
  }

  @Override
  public String toString() {
    return BaseMessages.getString( PKG, "StopHopServerServlet.description" );
  }

  @Override
  public String getContextPath() {
    return CONTEXT_PATH;
  }

  public static class DelayedExecutor {
    public void execute( final Runnable runnable, final long delay ) {
      ExecutorUtil.getExecutor().execute( () -> {
        try {
          Thread.sleep( delay );
        } catch ( InterruptedException e ) {
          // Ignore
        }
        runnable.run();
      } );
    }
  }

  private static final void exitJVM( int status ) {
    System.exit( status );
  }
}

