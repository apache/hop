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

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.HopServerPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginTypeListener;
import org.apache.hop.core.plugins.PluginRegistry;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HopServerServlet extends HttpServlet {

  private static final long serialVersionUID = 2434694833497859776L;

  public static final String STRING_HOP_SERVER_SERVLET = "HopServer Servlet";

  private Map<String, IHopServerPlugin> hopServerPluginRegistry;

  private final ILogChannel log;

  public HopServerServlet() {
    this.log = new LogChannel( STRING_HOP_SERVER_SERVLET );
  }

  public void doPost( HttpServletRequest req, HttpServletResponse resp ) throws ServletException, IOException {
    doGet( req, resp );
  }

  public void doGet( HttpServletRequest req, HttpServletResponse resp ) throws ServletException, IOException {
    String servletPath = req.getPathInfo();
    if ( servletPath.endsWith( "/" ) ) {
      servletPath = servletPath.substring( 0, servletPath.length() - 1 );
    }
    IHopServerPlugin plugin = hopServerPluginRegistry.get( servletPath );
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

  private String getServletKey( IHopServerPlugin servlet ) {
    String key = servlet.getContextPath();
    if ( key.startsWith( "/hop" ) ) {
      key = key.substring( "/hop".length() );
    }
    return key;
  }

  @Override
  public void init( ServletConfig config ) throws ServletException {
    hopServerPluginRegistry = new ConcurrentHashMap<>();

    PluginRegistry pluginRegistry = PluginRegistry.getInstance();
    List<IPlugin> plugins = pluginRegistry.getPlugins( HopServerPluginType.class );

    // Initial Registry scan
    for ( IPlugin plugin : plugins ) {
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
        registerServlet( (IHopServerPlugin) clazz.newInstance() );
      } catch ( ClassNotFoundException e ) {
        log.logError( "Unable to find configured " + paramName + " of " + className, e );
      } catch ( InstantiationException e ) {
        log.logError( "Unable to instantiate configured " + paramName + " of " + className, e );
      } catch ( IllegalAccessException e ) {
        log.logError( "Unable to access configured " + paramName + " of " + className, e );
      } catch ( ClassCastException e ) {
        log.logError( "Unable to cast configured "
          + paramName + " of " + className + " to " + IHopServerPlugin.class, e );
      }
    }

    // Catch servlets as they become available
    pluginRegistry.addPluginListener( HopServerPluginType.class, new IPluginTypeListener() {
      @Override public void pluginAdded( Object serviceObject ) {
        try {
          registerServlet( loadServlet( (IPlugin) serviceObject ) );
        } catch ( HopPluginException e ) {
          log.logError( MessageFormat.format( "Unable to load plugin: {0}", serviceObject ), e );
        }
      }

      @Override public void pluginRemoved( Object serviceObject ) {
        try {
          String key = getServletKey( loadServlet( (IPlugin) serviceObject ) );
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

  private IHopServerPlugin loadServlet( IPlugin plugin ) throws HopPluginException {
    return PluginRegistry.getInstance().loadClass( plugin, IHopServerPlugin.class );
  }

  private void registerServlet( IHopServerPlugin servlet ) {
    PipelineMap pipelineMap = HopServerSingleton.getInstance().getPipelineMap();
    WorkflowMap workflowMap = HopServerSingleton.getInstance().getWorkflowMap();

    hopServerPluginRegistry.put( getServletKey( servlet ), servlet );
    servlet.setup( pipelineMap, workflowMap );
    servlet.setJettyMode( false );
  }
}
