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
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.Servlet;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
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
import org.apache.hop.server.HopServer;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.jaas.JAASLoginService;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.PropertyUserStore;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.server.ConnectionLimit;
import org.eclipse.jetty.server.Connector;
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

  public static final int PORT = 80;
  public static final int SHUTDOWN_PORT = 8079;
  private static final int DEFAULT_DETECTION_TIMER = 20000;
  private static final Class<?> PKG = WebServer.class; // For Translator
  private ILogChannel log;
  private IVariables variables;
  private Server server;

  private PipelineMap pipelineMap;
  private WorkflowMap workflowMap;

  private String hostname;
  private int port;
  private int shutdownPort;

  private String passwordFile;
  private WebServerShutdownHook webServerShutdownHook;
  private IWebServerShutdownHandler webServerShutdownHandler =
      new DefaultWebServerShutdownHandler();

  private SslConfiguration sslConfig;

  public WebServer(
      ILogChannel log,
      PipelineMap pipelineMap,
      WorkflowMap workflowMap,
      String hostname,
      int port,
      int shutdownPort,
      boolean join,
      String passwordFile)
      throws Exception {
    this(log, pipelineMap, workflowMap, hostname, port, shutdownPort, join, passwordFile, null);
  }

  public WebServer(
      ILogChannel log,
      PipelineMap pipelineMap,
      WorkflowMap workflowMap,
      String hostname,
      int port,
      int shutdownPort,
      boolean join,
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

    startServer();

    webServerShutdownHook = new WebServerShutdownHook(this);
    Runtime.getRuntime().addShutdownHook(webServerShutdownHook);

    try {
      ExtensionPointHandler.callExtensionPoint(
          log, variables, HopExtensionPoint.HopServerStartup.id, this);
    } catch (HopException e) {
      // Log error but continue regular operations to make sure HopServer continues to run properly
      //
      log.logError("Error calling extension point HopServerStartup", e);
    }

    if (join) {
      server.join();
    }
  }

  public WebServer(
      ILogChannel log,
      PipelineMap pipelineMap,
      WorkflowMap workflowMap,
      String hostname,
      int port,
      int shutdownPort)
      throws Exception {
    this(log, pipelineMap, workflowMap, hostname, port, shutdownPort, true);
  }

  public WebServer(
      ILogChannel log,
      PipelineMap pipelineMap,
      WorkflowMap workflowMap,
      String hostname,
      int port,
      int shutdownPort,
      boolean join)
      throws Exception {
    this(log, pipelineMap, workflowMap, hostname, port, shutdownPort, join, null, null);
  }

  public Server getServer() {
    return server;
  }

  public void setServer(Server server) {
    this.server = server;
  }

  public void startServer() throws Exception {
    server = new Server();

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
      HopServer hopServer = pipelineMap.getHopServerConfig().getHopServer();
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

    // Add all the servlets defined in hop-servlets.xml ...
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
      ServletHolder servletHolder = new ServletHolder((Servlet) servlet);
      servletContext.addServlet(servletHolder, "/*");
      servletContext.setAttribute("GraphicsEnvironment", graphicsEnvironment);
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

    // Start execution
    createListeners();
    // Temp disable shutdown listener #3367
//    Thread monitor = new MonitorThread(server, hostname, shutdownPort);
//    monitor.start();
    server.start();
  }

  public String getContextPath(IHopServerPlugin servlet) {
    String contextPath = servlet.getContextPath();
    return contextPath;
  }

  public void join() throws InterruptedException {
    server.join();
  }

  public void stopServer() {

    webServerShutdownHook.setShuttingDown(true);
    log.logBasic(BaseMessages.getString(PKG, "WebServer.Log.ShuttingDown"));
    try {
      ExtensionPointHandler.callExtensionPoint(
          log, variables, HopExtensionPoint.HopServerShutdown.id, this);
    } catch (HopException e) {
      // Log error but continue regular operations to make sure HopServer can be shut down properly.
      //
      log.logError("Error calling extension point HopServerStartup", e);
    }

    try {
      if (server != null) {

        // Stop the server...
        //
        server.stop();
        HopEnvironment.shutdown();
        if (webServerShutdownHandler != null) {
          webServerShutdownHandler.shutdownWebServer();
        }
      }
    } catch (Exception e) {
      log.logError(
          BaseMessages.getString(PKG, "WebServer.Error.FailedToStop.Title"),
          BaseMessages.getString(PKG, "WebServer.Error.FailedToStop.Msg", "" + e));
    }
  }

  private void createListeners() {

    ServerConnector connector = getConnector();

    setupJettyOptions(connector);
    connector.setPort(port);
    connector.setHost(hostname);
    connector.setName(BaseMessages.getString(PKG, "WebServer.Log.HopHTTPListener", hostname));
    log.logBasic(BaseMessages.getString(PKG, "WebServer.Log.CreateListener", hostname, "" + port));

    // Temp disable shutdown listener #3367
    //    log.logBasic(
//        BaseMessages.getString(
//            PKG, "WebServer.Log.CreateShutDownListener", hostname, "" + shutdownPort));

    server.setConnectors(new Connector[] {connector});
  }

  private ServerConnector getConnector() {
    if (sslConfig != null) {
      log.logBasic(BaseMessages.getString(PKG, "WebServer.Log.SslModeUsing"));

      HttpConfiguration httpConfig = new HttpConfiguration();
      httpConfig.setSecureScheme("https");
      httpConfig.setSecurePort(port);

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

      HttpConfiguration httpsConfig = new HttpConfiguration(httpConfig);
      httpsConfig.addCustomizer(new SecureRequestCustomizer());

      ServerConnector sslConnector =
          new ServerConnector(
              server,
              new SslConnectionFactory(factory, HttpVersion.HTTP_1_1.asString()),
              new HttpConnectionFactory(httpsConfig));
      sslConnector.setPort(port);

      return sslConnector;
    } else {
      return new ServerConnector(server);
    }
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
              PKG, "WebServer.Log.ConfigOptions", "acceptors", connector.getAcceptors()));
    }

    if (validProperty(Const.HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE)) {
      connector.setAcceptQueueSize(
          Integer.parseInt(System.getProperty(Const.HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE)));
      log.logBasic(
          BaseMessages.getString(
              PKG,
              "WebServer.Log.ConfigOptions",
              "acceptQueueSize",
              connector.getAcceptQueueSize()));
    }

    if (validProperty(Const.HOP_SERVER_JETTY_RES_MAX_IDLE_TIME)) {
      connector.setIdleTimeout(
          Integer.parseInt(System.getProperty(Const.HOP_SERVER_JETTY_RES_MAX_IDLE_TIME)));
      log.logBasic(
          BaseMessages.getString(
              PKG,
              "WebServer.Log.ConfigOptions",
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

  /**
   * Can be used to override the default shutdown behavior of performing a System.exit
   *
   * @param webServerShutdownHandler
   */
  public void setWebServerShutdownHandler(IWebServerShutdownHandler webServerShutdownHandler) {
    this.webServerShutdownHandler = webServerShutdownHandler;
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

  private static class MonitorThread extends Thread {

    private ServerSocket socket;
    private Server server;

    public MonitorThread(Server server, String hostname, int shutdownPort) {
      this.server = server;
      setDaemon(true);
      setName("StopMonitor");
      try {
        socket = new ServerSocket(shutdownPort, 1, InetAddress.getByName(hostname));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void run() {
      Socket accept;
      try {
        accept = socket.accept();
        BufferedReader reader = new BufferedReader(new InputStreamReader(accept.getInputStream()));
        reader.readLine();
        server.stop();
        accept.close();
        socket.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
