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
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.xml.XmlHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintStream;

@HopServerServlet(id="nextSequence", name = "Get the next block of values for a sequence")
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
    out.println( XmlHandler.getXmlHeader( Const.XML_ENCODING ) );
    out.println( XmlHandler.openTag( XML_TAG ) );

    try {

      HopServerSequence hopServerSequence = getPipelineMap().getServerSequence( name );
      if ( hopServerSequence == null && getPipelineMap().isAutomaticServerSequenceCreationAllowed() ) {
        hopServerSequence = getPipelineMap().createServerSequence( name );
      }
      if ( hopServerSequence == null ) {
        response.sendError( HttpServletResponse.SC_NOT_FOUND );
        out.println( XmlHandler.addTagValue( XML_TAG_ERROR, "Server sequence '" + name + "' could not be found." ) );
      } else {
        ILoggingObject loggingObject = new SimpleLoggingObject( "HopServer", LoggingObjectType.HOP_SERVER, null );
        long nextValue = hopServerSequence.getNextValue( variables, loggingObject, increment );
        out.println( XmlHandler.addTagValue( XML_TAG_VALUE, nextValue ) );
        out.println( XmlHandler.addTagValue( XML_TAG_INCREMENT, increment ) );
      }

    } catch ( Exception e ) {
      response.sendError( HttpServletResponse.SC_NOT_FOUND );
      out.println( XmlHandler.addTagValue( XML_TAG_ERROR, "Error retrieving next value from server sequence: "
        + Const.getStackTracker( e ) ) );
    }

    out.println( XmlHandler.closeTag( XML_TAG ) );
  }

  public String toString() {
    return "Retrieve the next value of hop server sequence requested.";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

}
