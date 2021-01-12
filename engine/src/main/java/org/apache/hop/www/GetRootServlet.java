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

import org.apache.hop.i18n.BaseMessages;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

public class GetRootServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static final Class<?> PKG = GetRootServlet.class; // For Translator

  private static final long serialVersionUID = 3634806745372015720L;
  public static final String CONTEXT_PATH = "/";

  public GetRootServlet() {
  }

  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException,
    IOException {
    if ( isJettyMode() && !request.getRequestURI().equals( CONTEXT_PATH ) ) {
      response.sendError( HttpServletResponse.SC_NOT_FOUND );
      return;
    }

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "GetRootServlet.RootRequested" ) );
    }

    response.setContentType( "text/html;charset=UTF-8" );
    response.setStatus( HttpServletResponse.SC_OK );

    PrintWriter out = response.getWriter();

    out.println( "<HTML>" );
    out.println( "<HEAD><TITLE>"
      + BaseMessages.getString( PKG, "GetRootServlet.HopHopServer.Title" ) + "</TITLE>" );
    out.println( "<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">" );
    out.println( "</HEAD>" );
    out.println( "<BODY>" );
    out.println( "<H2>" + BaseMessages.getString( PKG, "GetRootServlet.HopServerMenu" ) + "</H2>" );

    out.println( "<p>" );
    out.println( "<a href=\""
      + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + "\">"
      + BaseMessages.getString( PKG, "GetRootServlet.ShowStatus" ) + "</a><br>" );

    out.println( "<p>" );
    out.println( "</BODY>" );
    out.println( "</HTML>" );
  }

  public String toString() {
    return "Root IHandler";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

}
