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

/*!
 * HITACHI VANTARA PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2017 Hitachi Vantara. All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Hitachi Vantara and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Hitachi Vantara and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Hitachi Vantara is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Hitachi Vantara,
 * explicitly covering such access.
 */

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
  public static final String CONTEXT_PATH = "/hop/stopCarte";
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
      logDebug( BaseMessages.getString( PKG, "StopCarteServlet.shutdownRequest" ) );
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
      out.print( XmlHandler.getXMLHeader( Const.XML_ENCODING ) );
      out.print( XmlHandler.addTagValue( REQUEST_ACCEPTED, hopServer != null ) );
      out.flush();
    } else {
      out.println( "<HTML>" );
      out.println(
        "<HEAD><TITLE>" + BaseMessages.getString( PKG, "StopCarteServlet.shutdownRequest" ) + "</TITLE></HEAD>" );
      out.println( "<BODY>" );
      out.println( "<H1>" + BaseMessages.getString( PKG, "StopCarteServlet.status.label" ) + "</H1>" );
      out.println( "<p>" );
      if ( hopServer != null ) {
        out.println( BaseMessages.getString( PKG, "StopCarteServlet.shutdownRequest.status.ok" ) );
      } else {
        out.println( BaseMessages.getString( PKG, "StopHopServerServlet.shutdownRequest.status.notFound" ) );
      }
      out.println( "</p>" );
      out.println( "</BODY>" );
      out.println( "</HTML>" );
      out.flush();
    }
    if ( hopServer != null ) {
      delayedExecutor.execute( new Runnable() {
        @Override
        public void run() {
          hopServer.getWebServer().stopServer();
          exitJVM( 0 );
        }
      }, 1000 );
    }
  }

  @Override
  public String toString() {
    return BaseMessages.getString( PKG, "StopCarteServlet.description" );
  }

  @Override
  public String getContextPath() {
    return CONTEXT_PATH;
  }

  public static class DelayedExecutor {
    public void execute( final Runnable runnable, final long delay ) {
      ExecutorUtil.getExecutor().execute( new Runnable() {
        @Override
        public void run() {
          try {
            Thread.sleep( delay );
          } catch ( InterruptedException e ) {
            // Ignore
          }
          runnable.run();
        }
      } );
    }
  }

  private static final void exitJVM( int status ) {
    System.exit( status );
  }
}

