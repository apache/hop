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

import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.eclipse.jetty.jaas.JAASLoginService;
import org.eclipse.jetty.security.*;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.PathResource;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.security.Password;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.plugins.HopServerPluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.i18n.BaseMessages;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import javax.servlet.Servlet;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class WebServer {

  private static final int DEFAULT_DETECTION_TIMER = 20000;
  private static Class<?> PKG = WebServer.class; // for i18n purposes, needed by Translator2!!

  private LogChannelInterface log;

  public static final int PORT = 80;

  private Server server;

  private TransformationMap transformationMap;
  private JobMap jobMap;
  private List<SlaveServerDetection> detections;
  private SocketRepository socketRepository;

  private String hostname;
  private int port;

  private Timer slaveMonitoringTimer;

  private String passwordFile;
  private WebServerShutdownHook webServerShutdownHook;
  private IWebServerShutdownHandler webServerShutdownHandler = new DefaultWebServerShutdownHandler();

  private SslConfiguration sslConfig;

  public WebServer( LogChannelInterface log, TransformationMap transformationMap, JobMap jobMap,
      SocketRepository socketRepository, List<SlaveServerDetection> detections, String hostname, int port, boolean join,
      String passwordFile ) throws Exception {
    this( log, transformationMap, jobMap, socketRepository, detections, hostname, port, join, passwordFile, null );
  }

  public WebServer( LogChannelInterface log, TransformationMap transformationMap, JobMap jobMap,
      SocketRepository socketRepository, List<SlaveServerDetection> detections, String hostname, int port, boolean join,
      String passwordFile, SslConfiguration sslConfig ) throws Exception {
    this.log = log;
    this.transformationMap = transformationMap;
    this.jobMap = jobMap;
    this.socketRepository = socketRepository;
    this.detections = detections;
    this.hostname = hostname;
    this.port = port;
    this.passwordFile = passwordFile;
    this.sslConfig = sslConfig;

    startServer();

    // Start the monitoring of the registered slave servers...
    //
    startSlaveMonitoring();

    webServerShutdownHook = new WebServerShutdownHook( this );
    Runtime.getRuntime().addShutdownHook( webServerShutdownHook );

    try {
      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.CarteStartup.id, this );
    } catch ( HopException e ) {
      // Log error but continue regular operations to make sure HopServer continues to run properly
      //
      log.logError( "Error calling extension point CarteStartup", e );
    }

    if ( join ) {
      server.join();
    }
  }

  public WebServer( LogChannelInterface log, TransformationMap transformationMap, JobMap jobMap,
      SocketRepository socketRepository, List<SlaveServerDetection> slaveServers, String hostname, int port )
      throws Exception {
    this( log, transformationMap, jobMap, socketRepository, slaveServers, hostname, port, true );
  }

  public WebServer( LogChannelInterface log, TransformationMap transformationMap, JobMap jobMap,
      SocketRepository socketRepository, List<SlaveServerDetection> detections, String hostname, int port,
      boolean join ) throws Exception {
    this( log, transformationMap, jobMap, socketRepository, detections, hostname, port, join, null, null );
  }

  public Server getServer() {
    return server;
  }

  public void startServer() throws Exception {
    server = new Server();

    List<String> roles = new ArrayList<String>();
    roles.add( Constraint.ANY_ROLE );

    // Set up the security handler, optionally with JAAS
    //
    ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();

    if ( System.getProperty( "loginmodulename" ) != null
        && System.getProperty( "java.security.auth.login.config" ) != null ) {
      JAASLoginService jaasLoginService = new JAASLoginService( "Hop" );
      jaasLoginService.setLoginModuleName( System.getProperty( "loginmodulename" ) );
      securityHandler.setLoginService( jaasLoginService );
    } else {
      roles.add( "default" );
      HashLoginService hashLoginService;
      SlaveServer slaveServer = transformationMap.getSlaveServerConfig().getSlaveServer();
      if ( !Utils.isEmpty( slaveServer.getPassword() ) ) {
        hashLoginService = new HashLoginService( "Hop" );
        UserStore userStore = new UserStore();
        userStore.addUser(slaveServer.getUsername(), new Password(slaveServer.getPassword()), new String[]{"default"});
        hashLoginService.setUserStore(userStore);
      } else {
        // See if there is a kettle.pwd file in the HOP_HOME directory:
        if ( Utils.isEmpty( passwordFile ) ) {
          File homePwdFile = new File( Const.getHopCartePasswordFile() );
          if ( homePwdFile.exists() ) {
            passwordFile = Const.getHopCartePasswordFile();
          } else {
            passwordFile = Const.getHopLocalCartePasswordFile();
          }
        }
        hashLoginService = new HashLoginService("Hop");
        PropertyUserStore userStore = new PropertyUserStore();
        userStore.setConfig(passwordFile);
        hashLoginService.setUserStore(userStore);
      }
      securityHandler.setLoginService( hashLoginService );
    }

    Constraint constraint = new Constraint();
    constraint.setName( Constraint.__BASIC_AUTH );
    constraint.setRoles( roles.toArray( new String[roles.size()] ) );
    constraint.setAuthenticate( true );

    ConstraintMapping constraintMapping = new ConstraintMapping();
    constraintMapping.setConstraint( constraint );
    constraintMapping.setPathSpec( "/*" );

    securityHandler.setConstraintMappings( new ConstraintMapping[] { constraintMapping } );

    // Add all the servlets defined in kettle-servlets.xml ...
    //
    ContextHandlerCollection contexts = new ContextHandlerCollection();

    // Root
    //
    ServletContextHandler
        root =
        new ServletContextHandler( contexts, GetRootServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS );
    GetRootServlet rootServlet = new GetRootServlet();
    rootServlet.setJettyMode( true );
    root.addServlet( new ServletHolder( rootServlet ), "/*" );

    PluginRegistry pluginRegistry = PluginRegistry.getInstance();
    List<PluginInterface> plugins = pluginRegistry.getPlugins( HopServerPluginType.class );
    for ( PluginInterface plugin : plugins ) {

      HopServerPluginInterface servlet = pluginRegistry.loadClass( plugin, HopServerPluginInterface.class );
      servlet.setup( transformationMap, jobMap, socketRepository, detections );
      servlet.setJettyMode( true );

      ServletContextHandler servletContext =
        new ServletContextHandler( contexts, getContextPath( servlet ), ServletContextHandler.SESSIONS );
      ServletHolder servletHolder = new ServletHolder( (Servlet) servlet );
      servletContext.addServlet( servletHolder, "/*" );
    }

    // setup jersey (REST)
    ServletHolder jerseyServletHolder = new ServletHolder( ServletContainer.class );
    jerseyServletHolder.setInitParameter( "com.sun.jersey.config.property.resourceConfigClass",
        "com.sun.jersey.api.core.PackagesResourceConfig" );
    jerseyServletHolder.setInitParameter( "com.sun.jersey.config.property.packages", "org.apache.hop.www.jaxrs" );
    root.addServlet( jerseyServletHolder, "/api/*" );

    // setup static resource serving
    // ResourceHandler mobileResourceHandler = new ResourceHandler();
    // mobileResourceHandler.setWelcomeFiles(new String[]{"index.html"});
    // mobileResourceHandler.setResourceBase(getClass().getClassLoader().
    // getResource("org.apache.hop/www/mobile").toExternalForm());
    // Context mobileContext = new Context(contexts, "/mobile", Context.SESSIONS);
    // mobileContext.setHandler(mobileResourceHandler);

    // Allow png files to be shown for transformations and jobs...
    //
    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setResourceBase( "temp" );
    // add all handlers/contexts to server

    // set up static servlet
    ServletHolder staticHolder = new ServletHolder( "static", DefaultServlet.class );
    // resourceBase maps to the path relative to where carte is started
    staticHolder.setInitParameter( "resourceBase", "./static/" );
    staticHolder.setInitParameter( "dirAllowed", "true" );
    staticHolder.setInitParameter( "pathInfoOnly", "true" );
    root.addServlet( staticHolder, "/static/*" );

    HandlerList handlers = new HandlerList();
    handlers.setHandlers( new Handler[] { resourceHandler, contexts } );
    securityHandler.setHandler( handlers );

    server.setHandler( securityHandler );

    // Start execution
    createListeners();

    server.start();
  }

  public String getContextPath( HopServerPluginInterface servlet ) {
    String contextPath = servlet.getContextPath();
    if ( !contextPath.startsWith( "/hop" ) ) {
      contextPath = "/hop" + contextPath;
    }
    return contextPath;
  }

  public void join() throws InterruptedException {
    server.join();
  }

  public void stopServer() {

    webServerShutdownHook.setShuttingDown( true );

    try {
      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.CarteShutdown.id, this );
    } catch ( HopException e ) {
      // Log error but continue regular operations to make sure HopServer can be shut down properly.
      //
      log.logError( "Error calling extension point CarteStartup", e );
    }

    try {
      if ( server != null ) {

        // Stop the monitoring timer
        //
        if ( slaveMonitoringTimer != null ) {
          slaveMonitoringTimer.cancel();
          slaveMonitoringTimer = null;
        }

        // Clean up all the server sockets...
        //
        socketRepository.closeAll();

        // Stop the server...
        //
        server.stop();
        HopEnvironment.shutdown();
        if ( webServerShutdownHandler != null ) {
          webServerShutdownHandler.shutdownWebServer();
        }
      }
    } catch ( Exception e ) {
      log.logError( BaseMessages.getString( PKG, "WebServer.Error.FailedToStop.Title" ),
          BaseMessages.getString( PKG, "WebServer.Error.FailedToStop.Msg", "" + e ) );
    }
  }

  private void createListeners() {

    ServerConnector connector = getConnector();

    setupJettyOptions( connector );
    connector.setPort( port );
    connector.setHost( hostname );
    connector.setName( BaseMessages.getString( PKG, "WebServer.Log.HopHTTPListener", hostname ) );
    log.logBasic( BaseMessages.getString( PKG, "WebServer.Log.CreateListener", hostname, "" + port ) );

    server.setConnectors( new Connector[] { connector } );
  }

  private ServerConnector getConnector() {
    if ( sslConfig != null ) {
      log.logBasic( BaseMessages.getString( PKG, "WebServer.Log.SslModeUsing" ) );
      SslConnectionFactory connector = new SslConnectionFactory();

      SslContextFactory contextFactory = new SslContextFactory();
      contextFactory.setKeyStoreResource(new PathResource(new File(sslConfig.getKeyStore())));
      contextFactory.setKeyStorePassword(sslConfig.getKeyStorePassword());
      contextFactory.setKeyManagerPassword(sslConfig.getKeyPassword());
      contextFactory.setKeyStoreType( sslConfig.getKeyStoreType() );
      return new ServerConnector(server, connector);
    } else {
      return new ServerConnector(server);
    }

  }

  /**
   * Set up jetty options to the connector
   *
   * @param connector
   */
  protected void setupJettyOptions( ServerConnector connector ) {
    LowResourceMonitor lowResourceMonitor = new LowResourceMonitor(server);
    if ( validProperty( Const.HOP_CARTE_JETTY_ACCEPTORS ) ) {
      server.addBean(new ConnectionLimit(Integer.parseInt(System.getProperty(Const.HOP_CARTE_JETTY_ACCEPTORS))));
      log.logBasic(
          BaseMessages.getString( PKG, "WebServer.Log.ConfigOptions", "acceptors", connector.getAcceptors() ) );
    }

    if ( validProperty( Const.HOP_CARTE_JETTY_ACCEPT_QUEUE_SIZE ) ) {
      connector
          .setAcceptQueueSize( Integer.parseInt( System.getProperty( Const.HOP_CARTE_JETTY_ACCEPT_QUEUE_SIZE ) ) );
      log.logBasic( BaseMessages
          .getString( PKG, "WebServer.Log.ConfigOptions", "acceptQueueSize", connector.getAcceptQueueSize() ) );
    }

    if ( validProperty( Const.HOP_CARTE_JETTY_RES_MAX_IDLE_TIME ) ) {
      connector.setIdleTimeout(Integer.parseInt(System.getProperty(Const.HOP_CARTE_JETTY_RES_MAX_IDLE_TIME)));
      log.logBasic( BaseMessages.getString( PKG, "WebServer.Log.ConfigOptions", "lowResourcesMaxIdleTime",
          connector.getIdleTimeout() ) );
    }
  }

  /**
   * Checks if the property is not null or not empty String that can be parseable as int and returns true if it is,
   * otherwise false
   *
   * @param property the property to check
   * @return true if the property is not null or not empty String that can be parseable as int, false otherwise
   */
  private boolean validProperty( String property ) {
    boolean isValid = false;
    if ( System.getProperty( property ) != null && System.getProperty( property ).length() > 0 ) {
      try {
        Integer.parseInt( System.getProperty( property ) );
        isValid = true;
      } catch ( NumberFormatException nmbfExc ) {
        log.logBasic( BaseMessages
            .getString( PKG, "WebServer.Log.ConfigOptionsInvalid", property, System.getProperty( property ) ) );
      }
    }
    return isValid;
  }

  /**
   * @return the hostname
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * @param hostname the hostname to set
   */
  public void setHostname( String hostname ) {
    this.hostname = hostname;
  }

  /**
   * @return the slave server detections
   */
  public List<SlaveServerDetection> getDetections() {
    return detections;
  }

  /**
   * This method registers a timer to check up on all the registered slave servers every X seconds.<br>
   */
  private void startSlaveMonitoring() {
    slaveMonitoringTimer = new Timer( "WebServer Timer" );
    TimerTask timerTask = new TimerTask() {

      public void run() {
        for ( SlaveServerDetection slaveServerDetection : detections ) {
          SlaveServer slaveServer = slaveServerDetection.getSlaveServer();

          // See if we can get a status...
          //
          try {
            // TODO: consider making this lighter or retaining more information...
            slaveServer.getStatus(); // throws the exception
            slaveServerDetection.setActive( true );
            slaveServerDetection.setLastActiveDate( new Date() );
          } catch ( Exception e ) {
            slaveServerDetection.setActive( false );
            slaveServerDetection.setLastInactiveDate( new Date() );

            // TODO: kick it out after a configurable period of time...
          }
        }
      }
    };
    int detectionTime = defaultDetectionTimer();
    slaveMonitoringTimer.schedule( timerTask, detectionTime, detectionTime );
  }

  /**
   * @return the socketRepository
   */
  public SocketRepository getSocketRepository() {
    return socketRepository;
  }

  /**
   * @param socketRepository the socketRepository to set
   */
  public void setSocketRepository( SocketRepository socketRepository ) {
    this.socketRepository = socketRepository;
  }

  public String getPasswordFile() {
    return passwordFile;
  }

  public void setPasswordFile( String passwordFile ) {
    this.passwordFile = passwordFile;
  }

  public LogChannelInterface getLog() {
    return log;
  }

  public void setLog( LogChannelInterface log ) {
    this.log = log;
  }

  public TransformationMap getTransformationMap() {
    return transformationMap;
  }

  public void setTransformationMap( TransformationMap transformationMap ) {
    this.transformationMap = transformationMap;
  }

  public JobMap getJobMap() {
    return jobMap;
  }

  public void setJobMap( JobMap jobMap ) {
    this.jobMap = jobMap;
  }

  public int getPort() {
    return port;
  }

  public void setPort( int port ) {
    this.port = port;
  }

  public Timer getSlaveMonitoringTimer() {
    return slaveMonitoringTimer;
  }

  public void setSlaveMonitoringTimer( Timer slaveMonitoringTimer ) {
    this.slaveMonitoringTimer = slaveMonitoringTimer;
  }

  public void setServer( Server server ) {
    this.server = server;
  }

  public void setDetections( List<SlaveServerDetection> detections ) {
    this.detections = detections;
  }

  /**
   * Can be used to override the default shutdown behavior of performing a System.exit
   *
   * @param webServerShutdownHandler
   */
  public void setWebServerShutdownHandler( IWebServerShutdownHandler webServerShutdownHandler ) {
    this.webServerShutdownHandler = webServerShutdownHandler;
  }

  public int defaultDetectionTimer() {
    String sDetectionTimer = System.getProperty( Const.HOP_SLAVE_DETECTION_TIMER );

    if ( sDetectionTimer != null ) {
      return Integer.parseInt( sDetectionTimer );
    } else {
      return DEFAULT_DETECTION_TIMER;
    }
  }
}
