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

import java.awt.GraphicsEnvironment;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.Servlet;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.HopServerPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.server.HopServerMeta;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.jaas.JAASLoginService;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.PropertyUserStore;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.server.ConnectionLimit;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.LowResourceMonitor;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.PathResource;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.security.Password;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.servlet.ServletContainer;

public class WebServer {
  private static final Class<?> PKG = WebServer.class;

  public static final int DEFAULT_PORT = 80;
  public static final int DEFAULT_SHUTDOWN_PORT = 8079;
  private static final int DEFAULT_DETECTION_TIMER = 20000;

  public static final String CONST_WEB_SERVER_LOG_CONFIG_OPTIONS = "WebServer.Log.ConfigOptions";
  private ILogChannel log;
  private IVariables variables;
  private Server server;

  private PipelineMap pipelineMap;
  private WorkflowMap workflowMap;

  private String hostname;
  private int port;
  private final int shutdownPort;
  private String passwordFile;
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

    List<String> roles = new ArrayList<>();
    roles.add(Constraint.ANY_ROLE);

    // Set up the security handler, optionally with JAAS
    //
    ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();

    if (System.getProperty("loginmodulename") != null
        && System.getProperty("java.security.auth.login.config") != null) {
      JAASLoginService jaasLoginService = new JAASLoginService("Hop");
      jaasLoginService.setLoginModuleName(System.getProperty("loginmodulename"));
      securityHandler.setLoginService(jaasLoginService);
    } else {
      roles.add("default");
      HashLoginService hashLoginService;
      HopServerMeta hopServer = pipelineMap.getHopServerConfig().getHopServer();
      if (!Utils.isEmpty(hopServer.getPassword())) {
        hashLoginService = new HashLoginService("Hop");
        UserStore userStore = new UserStore();
        userStore.addUser(
            hopServer.getUsername(),
            new Password(hopServer.getPassword()),
            new String[] {"default"});
        hashLoginService.setUserStore(userStore);
      } else {
        // See if there is a hop.pwd file in the HOP_HOME directory:
        if (Utils.isEmpty(passwordFile)) {
          passwordFile = Const.getHopLocalServerPasswordFile();
        }
        hashLoginService = new HashLoginService("Hop");
        PropertyUserStore userStore = new PropertyUserStore();
        userStore.setConfig(passwordFile);
        hashLoginService.setUserStore(userStore);
      }
      securityHandler.setLoginService(hashLoginService);
    }

    Constraint constraint = new Constraint();
    constraint.setName(Constraint.__BASIC_AUTH);
    constraint.setRoles(roles.toArray(new String[roles.size()]));
    constraint.setAuthenticate(true);

    ConstraintMapping constraintMapping = new ConstraintMapping();
    constraintMapping.setConstraint(constraint);
    constraintMapping.setPathSpec("/*");

    securityHandler.setConstraintMappings(new ConstraintMapping[] {constraintMapping});

    // Configure the Servlet Context, to add all the server plugins
    //
    ContextHandlerCollection contexts = new ContextHandlerCollection();

    // Root
    //
    ServletContextHandler root =
        new ServletContextHandler(
            contexts, GetRootServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
    GetRootServlet rootServlet = new GetRootServlet();
    rootServlet.setJettyMode(true);
    root.addServlet(new ServletHolder(rootServlet), "/*");

    boolean graphicsEnvironment = supportGraphicEnvironment();

    PluginRegistry pluginRegistry = PluginRegistry.getInstance();
    List<IPlugin> plugins = pluginRegistry.getPlugins(HopServerPluginType.class);
    for (IPlugin plugin : plugins) {

      IHopServerPlugin servlet = pluginRegistry.loadClass(plugin, IHopServerPlugin.class);
      servlet.setup(pipelineMap, workflowMap);
      servlet.setJettyMode(true);

      ServletContextHandler servletContext =
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

    // Allow png files to be shown for pipelines and workflows...
    //
    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setResourceBase("temp");
    // add all handlers/contexts to server

    // set up static servlet
    ServletHolder staticHolder = new ServletHolder("static", DefaultServlet.class);
    // resourceBase maps to the path relative to where carte is started
    staticHolder.setInitParameter("resourceBase", "./static/");
    staticHolder.setInitParameter("dirAllowed", "true");
    staticHolder.setInitParameter("pathInfoOnly", "true");
    root.addServlet(staticHolder, "/static/*");

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

      SslContextFactory.Client factory = new SslContextFactory.Client();
      factory.setKeyStoreResource(new PathResource(new File(sslConfig.getKeyStore())));
      factory.setKeyStorePassword(keyStorePassword);
      factory.setKeyManagerPassword(keyPassword);
      factory.setKeyStoreType(sslConfig.getKeyStoreType());
      factory.setTrustStoreResource(new PathResource(new File(sslConfig.getKeyStore())));
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
          new ConnectionLimit(
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
    if (System.getProperty(property) != null && System.getProperty(property).length() > 0) {
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

  /**
   * @return the hostname
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * @param hostname the hostname to set
   */
  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getPasswordFile() {
    return passwordFile;
  }

  public void setPasswordFile(String passwordFile) {
    this.passwordFile = passwordFile;
  }

  public ILogChannel getLog() {
    return log;
  }

  public void setLog(ILogChannel log) {
    this.log = log;
  }

  public PipelineMap getPipelineMap() {
    return pipelineMap;
  }

  public void setPipelineMap(PipelineMap pipelineMap) {
    this.pipelineMap = pipelineMap;
  }

  public WorkflowMap getWorkflowMap() {
    return workflowMap;
  }

  public void setWorkflowMap(WorkflowMap workflowMap) {
    this.workflowMap = workflowMap;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getVariables() {
    return variables;
  }

  /**
   * @param variables The variables to set
   */
  public void setVariables(IVariables variables) {
    this.variables = variables;
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
