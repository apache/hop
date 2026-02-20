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
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
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

@Getter
@Setter
public class WebServer {
  private static final Class<?> PKG = WebServer.class;

  public static final int DEFAULT_PORT = 80;
  public static final int DEFAULT_SHUTDOWN_PORT = 8079;
  private static final int DEFAULT_DETECTION_TIMER = 20000;

  public static final String CONST_WEB_SERVER_LOG_CONFIG_OPTIONS = "WebServer.Log.ConfigOptions";
  private ILogChannel log;

  /** value of variables */
  private IVariables variables;

  private Server server;
  private PipelineMap pipelineMap;
  private WorkflowMap workflowMap;

  /** the hostname */
  private String hostname;

  private int port;
  private final int shutdownPort;
  private String passwordFile;
  private final WebServerShutdownHook webServerShutdownHook;

  /** Can be used to override the default shutdown behavior of performing a System.exit */
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
      int shutdownPort,
      boolean join)
      throws Exception {
    this(log, pipelineMap, workflowMap, hostname, port, shutdownPort, join, null, null);
  }

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
  }

  public String getContextPath(IHopServerPlugin servlet) {
    return servlet.getContextPath();
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

      SslContextFactory.Server factory = new SslContextFactory.Server();
      factory.setKeyStorePath(sslConfig.getKeyStore());
      factory.setKeyStorePassword(keyStorePassword);
      factory.setKeyManagerPassword(keyPassword);
      factory.setKeyStoreType(sslConfig.getKeyStoreType());
      factory.setTrustStorePath(sslConfig.getKeyStore());
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
   * @param connector The Jetty connector to set up
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
