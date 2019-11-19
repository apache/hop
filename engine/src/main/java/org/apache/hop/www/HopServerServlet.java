/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.plugins.HopServerPluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.PluginTypeListener;

public class HopServerServlet extends HttpServlet {

  private static final long serialVersionUID = 2434694833497859776L;

  public static final String STRING_HOP_SERVER_SERVLET = "HopServer Servlet";

  private Map<String, HopServerPluginInterface> hopServerPluginRegistry;

  private final LogChannelInterface log;
  private List<SlaveServerDetection> detections;

  public HopServerServlet() {
    this.log = new LogChannel(STRING_HOP_SERVER_SERVLET);
  }

  public void doPost( HttpServletRequest req, HttpServletResponse resp ) throws ServletException, IOException {
    doGet( req, resp );
  }

  public void doGet( HttpServletRequest req, HttpServletResponse resp ) throws ServletException, IOException {
    String servletPath = req.getPathInfo();
    if ( servletPath.endsWith( "/" ) ) {
      servletPath = servletPath.substring( 0, servletPath.length() - 1 );
    }
    HopServerPluginInterface plugin = hopServerPluginRegistry.get( servletPath );
    if ( plugin != null ) {
      try {
        plugin.doGet( req, resp );
      } catch ( ServletException e ) {
        throw e;
      } catch ( Exception e ) {
        throw new ServletException( e );
      }
    } else {
      if ( log.isDebug() ) {
        log.logDebug( "Unable to find Hop Server Plugin for key: /hop" + req.getPathInfo() );
      }
      resp.sendError( 404 );
    }
  }

  private String getServletKey( HopServerPluginInterface servlet ) {
    String key = servlet.getContextPath();
    if ( key.startsWith( "/hop" ) ) {
      key = key.substring( "/hop".length() );
    }
    return key;
  }

  @Override
  public void init( ServletConfig config ) throws ServletException {
    hopServerPluginRegistry = new ConcurrentHashMap<String, HopServerPluginInterface>();
    detections = Collections.synchronizedList( new ArrayList<SlaveServerDetection>() );

    PluginRegistry pluginRegistry = PluginRegistry.getInstance();
    List<PluginInterface> plugins = pluginRegistry.getPlugins( HopServerPluginType.class );

    // Initial Registry scan
    for ( PluginInterface plugin : plugins ) {
      try {
        registerServlet( loadServlet( plugin ) );
      } catch ( HopPluginException e ) {
        log.logError( "Unable to instantiate plugin for use with HopServerServlet " + plugin.getName() );
      }
    }

    // Servlets configured in web.xml take precedence to those discovered during plugin scan
    @SuppressWarnings( "unchecked" )
    Enumeration<String> initParameterNames = config.getInitParameterNames();
    while ( initParameterNames.hasMoreElements() ) {
      final String paramName = initParameterNames.nextElement();
      final String className = config.getInitParameter( paramName );
      final Class<?> clazz;
      try {
        clazz = Class.forName( className );
        registerServlet( (HopServerPluginInterface) clazz.newInstance() );
      } catch ( ClassNotFoundException e ) {
        log.logError( "Unable to find configured " + paramName + " of " + className, e );
      } catch ( InstantiationException e ) {
        log.logError( "Unable to instantiate configured " + paramName + " of " + className, e );
      } catch ( IllegalAccessException e ) {
        log.logError( "Unable to access configured " + paramName + " of " + className, e );
      } catch ( ClassCastException e ) {
        log.logError( "Unable to cast configured "
          + paramName + " of " + className + " to " + HopServerPluginInterface.class, e );
      }
    }

    // Catch servlets as they become available
    pluginRegistry.addPluginListener( HopServerPluginType.class, new PluginTypeListener() {
      @Override public void pluginAdded( Object serviceObject ) {
        try {
          registerServlet( loadServlet( (PluginInterface) serviceObject ) );
        } catch ( HopPluginException e ) {
          log.logError( MessageFormat.format( "Unable to load plugin: {0}", serviceObject ), e );
        }
      }

      @Override public void pluginRemoved( Object serviceObject ) {
        try {
          String key = getServletKey( loadServlet( (PluginInterface) serviceObject ) );
          hopServerPluginRegistry.remove( key );
        } catch ( HopPluginException e ) {
          log.logError( MessageFormat.format( "Unable to load plugin: {0}", serviceObject ), e );
        }
      }

      @Override public void pluginChanged( Object serviceObject ) {
        pluginAdded( serviceObject );
      }
    } );
  }

  private HopServerPluginInterface loadServlet(PluginInterface plugin ) throws HopPluginException {
    return PluginRegistry.getInstance().loadClass( plugin, HopServerPluginInterface.class );
  }

  private void registerServlet( HopServerPluginInterface servlet ) {
    TransformationMap transformationMap = HopServerSingleton.getInstance().getTransformationMap();
    JobMap jobMap = HopServerSingleton.getInstance().getJobMap();
    SocketRepository socketRepository = HopServerSingleton.getInstance().getSocketRepository();

    hopServerPluginRegistry.put( getServletKey( servlet ), servlet );
    servlet.setup( transformationMap, jobMap, socketRepository, detections );
    servlet.setJettyMode( false );
  }
}
