/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.www;

import jakarta.servlet.Servlet;
import java.awt.GraphicsEnvironment;
import java.io.File;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.HopServerPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.server.HopServerMeta;
import org.eclipse.jetty.ee11.servlet.DefaultServlet;
import org.eclipse.jetty.ee11.servlet.ServletContextHandler;
import org.eclipse.jetty.ee11.servlet.ServletHolder;
import org.eclipse.jetty.ee11.servlet.security.ConstraintMapping;
import org.eclipse.jetty.ee11.servlet.security.ConstraintSecurityHandler;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.jaas.JAASLoginService;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.Constraint;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.PropertyUserStore;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ConnectionLimit;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.LowResourceMonitor;
import org.eclipse.jetty.server.NetworkConnectionLimit;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceFactory;
import org.eclipse.jetty.util.security.Password;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.servlet.ServletContainer;

public class WebServer {
  private static final Class<?> PKG = WebServer.class;

  public static final int DEFAULT_PORT = 80;
  public static final int DEFAULT_SHUTDOWN_PORT = 8079;
  private static final int DEFAULT_DETECTION_TIMER = 20000;

  public static final String CONST_WEB_SERVER_LOG_CONFIG_OPTIONS = "WebServer.Log.ConfigOptions";
  @Getter @Setter private ILogChannel log;

  /** value of variables */
  @Setter @Getter private IVariables variables;

  @Getter @Setter private Server server;
  @Getter @Setter private PipelineMap pipelineMap;
  @Setter @Getter private WorkflowMap workflowMap;

  /** the hostname */
  @Setter @Getter private String hostname;

  @Setter @Getter private int port;
  private final int shutdownPort;
  private String passwordFile;
  private final WebServerShutdownHook webServerShutdownHook;

  /** Can be used to override the default shutdown behavior of performing a System.exit */
  @Setter
  private IWebServerShutdownHandler webServerShutdownHandler =
      new DefaultWebServerShutdownHandler();

  private final SslConfiguration sslConfig;

  public WebServer(
      ILogChannel log,
      PipelineMap pipelineMap,
      WorkflowMap workflowMap,
      String hostname,
      int port,
      int shutdownPort,
      String passwordFile)
      throws Exception {
    this(log, pipelineMap, workflowMap, hostname, port, shutdownPort, passwordFile, null);
  }

  public WebServer(
      ILogChannel log,
      PipelineMap pipelineMap,
      WorkflowMap workflowMap,
      String hostname,
      int port,
      int shutdownPort,
      String passwordFile,
      SslConfiguration sslConfig)
      throws Exception {
    this.log = log;
    this.pipelineMap = pipelineMap;
    this.workflowMap = workflowMap;
    if (pipelineMap != null) {
      variables = pipelineMap.getHopServerConfig().getVariables();
    } else if (workflowMap != null) {
      variables = workflowMap.getHopServerConfig().getVariables();
    } else {
      variables = Variables.getADefaultVariableSpace();
    }
    this.hostname = hostname;
    this.port = port;
    this.shutdownPort = shutdownPort;
    this.passwordFile = passwordFile;
    this.sslConfig = sslConfig;
    this.server = new Server();
  }

  public WebServer(
      ILogChannel log,
      PipelineMap pipelineMap,
      WorkflowMap workflowMap,
      String hostname,
      int port,
      int shutdownPort)
      throws Exception {
    this(log, pipelineMap, workflowMap, hostname, port, shutdownPort, null, null);
  }

  public Server getServer() {
    return server;
  }

  /** Start the web server... */
  public synchronized void start() throws Exception {

    if (server.isStarting() || server.isStarted()) {
      return;
    }

    log.logDetailed(BaseMessages.getString(PKG, "WebServer.Log.StartingServer"));
  public void startServer() throws Exception {
    server = new Server();
    HopServerMeta hopServer = pipelineMap.getHopServerConfig().getHopServer();

    Handler innerHandler;

    if (hopServer.isEnableAuth()) {
      Constraint.Builder constraintBuilder =
          new Constraint.Builder()
              .name(Authenticator.BASIC_AUTH)
              .authorization(Constraint.Authorization.SPECIFIC_ROLE)
              .transport(Constraint.Transport.ANY);

      ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();
      securityHandler.setAuthenticationType(Authenticator.BASIC_AUTH);

      // basic authentication
      if (System.getProperty("loginmodulename") != null
          && System.getProperty("java.security.auth.login.config") != null) {
        constraintBuilder.roles("*");
        JAASLoginService jaasLoginService = new JAASLoginService("Hop");
        jaasLoginService.setLoginModuleName(System.getProperty("loginmodulename"));
        securityHandler.setLoginService(jaasLoginService);
      } else {
        constraintBuilder.roles("default");
        HashLoginService hashLoginService;
        if (!Utils.isEmpty(hopServer.getPassword())) {
          hashLoginService = new HashLoginService("Hop");
          UserStore userStore = new UserStore();
          userStore.addUser(
              hopServer.getUsername(),
              new Password(hopServer.getPassword()),
              new String[] {"default"});
          hashLoginService.setUserStore(userStore);
        } else {
          if (Utils.isEmpty(passwordFile)) {
            passwordFile = Const.getHopLocalServerPasswordFile();
          }
          hashLoginService = new HashLoginService("Hop");
          PropertyUserStore userStore = new PropertyUserStore();
          userStore.setConfig(ResourceFactory.of(server).newResource(passwordFile));
          hashLoginService.setUserStore(userStore);
        }
        securityHandler.setLoginService(hashLoginService);
      }

      ConstraintMapping constraintMapping = new ConstraintMapping();
      constraintMapping.setPathSpec("/*");
      constraintMapping.setConstraint(constraintBuilder.build());
      securityHandler.setConstraintMappings(new ConstraintMapping[] {constraintMapping});

      // create context handler collection
      ContextHandlerCollection contexts = createContexts();
      ResourceHandler resourceHandler = new ResourceHandler();
      resourceHandler.setBaseResourceAsString("temp");

      securityHandler.setHandler(new Handler.Sequence(resourceHandler, contexts));
      innerHandler = securityHandler;
      log.logBasic("Hop Server: Basic authentication is ENABLED");
    } else {
      ContextHandlerCollection contexts = createContexts();
      ResourceHandler resourceHandler = new ResourceHandler();
      resourceHandler.setBaseResourceAsString("temp");

      innerHandler = new Handler.Sequence(resourceHandler, contexts);
      log.logBasic("Hop Server: Basic authentication is DISABLED (enableAuth=false)");
    }

    server.setHandler(innerHandler);

    // Start execution
    createListeners();
    server.start();
  }

  private ContextHandlerCollection createContexts() throws HopPluginException {
    // Configure the Servlet Context, to add all the server plugins
    //
    ContextHandlerCollection contexts = new ContextHandlerCollection();

    // Root
    ServletContextHandler root =
        new ServletContextHandler(GetRootServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
    contexts.addHandler(root);
    GetRootServlet rootServlet = new GetRootServlet();
    rootServlet.setJettyMode(true);

    boolean graphicsEnvironment = supportGraphicEnvironment();

    PluginRegistry pluginRegistry = PluginRegistry.getInstance();
    List<IPlugin> plugins = pluginRegistry.getPlugins(HopServerPluginType.class);
    for (IPlugin plugin : plugins) {
      IHopServerPlugin servlet = pluginRegistry.loadClass(plugin, IHopServerPlugin.class);
      servlet.setup(pipelineMap, workflowMap);
      servlet.setJettyMode(true);

      ServletContextHandler servletContext =
          new ServletContextHandler(getContextPath(servlet), ServletContextHandler.SESSIONS);
      contexts.addHandler(servletContext);
      ServletHolder servletHolder = new ServletHolder((Servlet) servlet);
      servletContext.addServlet(servletHolder, "/*");
          new ServletContextHandler(
              contexts, getContextPath(servlet), ServletContextHandler.SESSIONS);
      servletContext.setVirtualHosts(new String[] {"@webserver"});
      servletContext.setAttribute("GraphicsEnvironment", graphicsEnvironment);
      servletContext.addServlet(new ServletHolder((Servlet) servlet), "/*");
    }

    // setup jersey (REST)
    ServletHolder jerseyServletHolder = new ServletHolder(ServletContainer.class);
    jerseyServletHolder.setInitParameter(
        "com.sun.jersey.config.property.resourceConfigClass",
        "com.sun.jersey.api.core.PackagesResourceConfig");
    jerseyServletHolder.setInitParameter(
        "com.sun.jersey.config.property.packages", "org.apache.hop.www.jaxrs");
    root.addServlet(jerseyServletHolder, "/api/*");

    // Static resources
    ServletHolder staticHolder = new ServletHolder("static", DefaultServlet.class);
    // baseResource maps to the path relative to where hop-server is started
    Resource staticResource = ResourceFactory.of(server).newResource("static/");
    root.setInitParameter(DefaultServlet.CONTEXT_INIT + "baseResource", staticResource.toString());
    root.setInitParameter(DefaultServlet.CONTEXT_INIT + "dirAllowed", "true");
    root.setInitParameter(DefaultServlet.CONTEXT_INIT + "pathInfoOnly", "true");
    root.addServlet(staticHolder, "/static/*");

    root.addServlet(new ServletHolder(rootServlet), "/*");
    return contexts;
    HandlerList handlers = new HandlerList();
    handlers.setHandlers(new Handler[] {resourceHandler, contexts});
    securityHandler.setHandler(handlers);

    server.setHandler(securityHandler);

    // Public connector
    ServerConnector publicConnector = createConnector("webserver", hostname, port);
    setupJettyOptions(publicConnector);
    server.addConnector(publicConnector);

    // If shutdown port is enabled
    if (shutdownPort > 0) {
      // Shutdown context
      ServletContextHandler shutdownHandler =
          new ServletContextHandler(
              contexts, ShutdownServlet.CONTEXT_PATH, ServletContextHandler.NO_SESSIONS);
      ServletHolder shutdownHolder = new ServletHolder(new ShutdownServlet());
      shutdownHandler.addServlet(shutdownHolder, "/*");
      shutdownHandler.setVirtualHosts(new String[] {"@shutdown"});

      // Shutdown connector
      ServerConnector shutdownConnector = createConnector("shutdown", hostname, shutdownPort);
      shutdownConnector.setAcceptQueueSize(1);
      server.addConnector(shutdownConnector);
    }

    // Setup timeout to allow graceful timeout of server components
    server.setStopTimeout(1000L);

    // Start execution
    server.start();
  }

  public String getContextPath(IHopServerPlugin servlet) {
    return servlet.getContextPath();
  }

  public void join() throws InterruptedException {
    server.join();
  }

  /** Stop the web server... */
  public synchronized void stop() {
    if (server.isStopping() || server.isStopped()) {
      return;
    }

    log.logDetailed(BaseMessages.getString(PKG, "WebServer.Log.StoppingServer"));

    try {
      server.stop();
    } catch (Exception e) {
      log.logError(BaseMessages.getString(PKG, "WebServer.Error.FailedToStop.Msg", e.toString()));
    }
  }

  private ServerConnector createConnector(String name, String hostname, int securePort) {
    log.logBasic(
        BaseMessages.getString(
            PKG, "WebServer.Log.CreateListener", name, hostname, "" + securePort));

    ServerConnector connector = null;

    if (sslConfig != null) {
      log.logBasic(BaseMessages.getString(PKG, "WebServer.Log.SslModeUsing"));

      // SSL Context Factory for HTTPS
      String keyStorePassword =
          Encr.decryptPasswordOptionallyEncrypted(sslConfig.getKeyStorePassword());
      String keyPassword = Encr.decryptPasswordOptionallyEncrypted(sslConfig.getKeyPassword());

      SslContextFactory.Server factory = new SslContextFactory.Server();
      factory.setKeyStorePath(sslConfig.getKeyStore());
      factory.setKeyStorePassword(keyStorePassword);
      factory.setKeyManagerPassword(keyPassword);
      factory.setKeyStoreType(sslConfig.getKeyStoreType());
      factory.setTrustStorePath(sslConfig.getKeyStore());
      factory.setTrustStorePassword(keyStorePassword);

      // HTTPS connection factory
      HttpConfiguration httpsConfig = new HttpConfiguration();
      httpsConfig.setSecureScheme("https");
      httpsConfig.setSecurePort(securePort);
      httpsConfig.addCustomizer(new SecureRequestCustomizer());

      connector =
          new ServerConnector(
              server,
              new SslConnectionFactory(factory, HttpVersion.HTTP_1_1.asString()),
              new HttpConnectionFactory(httpsConfig));

    } else {
      connector = new ServerConnector(server);
    }
    connector.setName(name);
    connector.setHost(hostname);
    connector.setPort(securePort);

    return connector;
  }

  /**
   * Set up jetty options to the connector
   *
   * @param connector
   */
  protected void setupJettyOptions(ServerConnector connector) {
    LowResourceMonitor lowResourceMonitor = new LowResourceMonitor(server);
    if (validProperty(Const.HOP_SERVER_JETTY_ACCEPTORS)) {
      server.addBean(
          new NetworkConnectionLimit(
              Integer.parseInt(System.getProperty(Const.HOP_SERVER_JETTY_ACCEPTORS))));
      log.logBasic(
          BaseMessages.getString(
              PKG, CONST_WEB_SERVER_LOG_CONFIG_OPTIONS, "acceptors", connector.getAcceptors()));
    }

    if (validProperty(Const.HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE)) {
      connector.setAcceptQueueSize(
          Integer.parseInt(System.getProperty(Const.HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE)));
      log.logBasic(
          BaseMessages.getString(
              PKG,
              CONST_WEB_SERVER_LOG_CONFIG_OPTIONS,
              "acceptQueueSize",
              connector.getAcceptQueueSize()));
    }

    if (validProperty(Const.HOP_SERVER_JETTY_RES_MAX_IDLE_TIME)) {
      connector.setIdleTimeout(
          Integer.parseInt(System.getProperty(Const.HOP_SERVER_JETTY_RES_MAX_IDLE_TIME)));
      log.logBasic(
          BaseMessages.getString(
              PKG,
              CONST_WEB_SERVER_LOG_CONFIG_OPTIONS,
              "lowResourcesMaxIdleTime",
              connector.getIdleTimeout()));
    }
  }

  /**
   * Checks if the property is not null or not empty String that can be parseable as int and returns
   * true if it is, otherwise false
   *
   * @param property the property to check
   * @return true if the property is not null or not empty String that can be parseable as int,
   *     false otherwise
   */
  private boolean validProperty(String property) {
    boolean isValid = false;
    if (!Utils.isEmpty(System.getProperty(property))) {
      try {
        Integer.parseInt(System.getProperty(property));
        isValid = true;
      } catch (NumberFormatException nmbfExc) {
        log.logBasic(
            BaseMessages.getString(
                PKG, "WebServer.Log.ConfigOptionsInvalid", property, System.getProperty(property)));
      }
    }
    return isValid;
  }

  public String getPasswordFile() {
    return passwordFile;
  }

  public void setPasswordFile(String passwordFile) {
    this.passwordFile = passwordFile;
  }

  public int defaultDetectionTimer() {
    String sDetectionTimer = System.getProperty(Const.HOP_SERVER_DETECTION_TIMER);

    if (sDetectionTimer != null) {
      return Integer.parseInt(sDetectionTimer);
    } else {
      return DEFAULT_DETECTION_TIMER;
    }
  }

  private boolean supportGraphicEnvironment() {
    try {
      return GraphicsEnvironment.getLocalGraphicsEnvironment() != null;
    } catch (Error ignored) {
    }
    return false;
  }
}
