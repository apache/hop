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


package org.apache.hop.server;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.IXml;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.www.GetPipelineStatusServlet;
import org.apache.hop.www.GetStatusServlet;
import org.apache.hop.www.GetWorkflowStatusServlet;
import org.apache.hop.www.NextSequenceValueServlet;
import org.apache.hop.www.PausePipelineServlet;
import org.apache.hop.www.RegisterPackageServlet;
import org.apache.hop.www.RemovePipelineServlet;
import org.apache.hop.www.RemoveWorkflowServlet;
import org.apache.hop.www.HopServerPipelineStatus;
import org.apache.hop.www.HopServerStatus;
import org.apache.hop.www.HopServerWorkflowStatus;
import org.apache.hop.www.SniffTransformServlet;
import org.apache.hop.www.SslConfiguration;
import org.apache.hop.www.StartPipelineServlet;
import org.apache.hop.www.StartWorkflowServlet;
import org.apache.hop.www.StopPipelineServlet;
import org.apache.hop.www.StopWorkflowServlet;
import org.apache.hop.www.WebResult;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

@HopMetadata(
  key = "server",
  name = "Hop Server",
  description = "Defines a Hop Hop Server",
  image = "ui/images/server.svg"
)
public class HopServer extends HopMetadataBase implements Cloneable, IXml, IHopMetadata {
  private static final Class<?> PKG = HopServer.class; // For Translator

  public static final String STRING_HOP_SERVER = "Hop Server";

  private static final Random RANDOM = new Random();

  public static final String XML_TAG = "hop-server";

  private static final String HTTP = "http";
  private static final String HTTPS = "https";

  public static final String SSL_MODE_TAG = "sslMode";

  public static final int HOP_SERVER_RETRIES = getNumberOfHopServerRetries();

  public static final int HOP_SERVER_RETRY_BACKOFF_INCREMENTS = getBackoffIncrements();

  private static int getNumberOfHopServerRetries() {
    try {
      return Integer.parseInt( Const.NVL( System.getProperty( "HOP_SERVER_RETRIES" ), "0" ) );
    } catch ( Exception e ) {
      return 0;
    }
  }

  public static int getBackoffIncrements() {
    try {
      return Integer.parseInt( Const.NVL( System.getProperty( "HOP_SERVER_RETRY_BACKOFF_INCREMENTS" ), "1000" ) );
    } catch ( Exception e ) {
      return 1000;
    }
  }

  private ILogChannel log;

  @HopMetadataProperty
  private String hostname;

  @HopMetadataProperty
  private String port;

  @HopMetadataProperty
  private String webAppName;

  @HopMetadataProperty
  private String username;

  @HopMetadataProperty( password = true )
  private String password;

  @HopMetadataProperty
  private String proxyHostname;

  @HopMetadataProperty
  private String proxyPort;

  @HopMetadataProperty
  private String nonProxyHosts;

  @HopMetadataProperty
  private String propertiesMasterName;

  @HopMetadataProperty
  private boolean overrideExistingProperties;

  private Date changedDate;

  @HopMetadataProperty
  private boolean sslMode;

  @HopMetadataProperty
  private SslConfiguration sslConfig;

  public HopServer() {
    this.log = new LogChannel( STRING_HOP_SERVER );
    this.changedDate = new Date();
  }

  public HopServer( String name, String hostname, String port, String username, String password ) {
    this( name, hostname, port, username, password, null, null, null, false );
  }

  public HopServer( String name, String hostname, String port, String username, String password,
                    String proxyHostname, String proxyPort, String nonProxyHosts ) {
    this( name, hostname, port, username, password, proxyHostname, proxyPort, nonProxyHosts, false );
  }

  public HopServer( String name, String hostname, String port, String username, String password,
                    String proxyHostname, String proxyPort, String nonProxyHosts, boolean sslMode ) {
    this();
    this.name = name;
    this.hostname = hostname;
    this.port = port;
    this.username = username;
    this.password = password;

    this.proxyHostname = proxyHostname;
    this.proxyPort = proxyPort;
    this.nonProxyHosts = nonProxyHosts;
    this.sslMode = sslMode;
    this.log = new LogChannel( this );
  }

  public HopServer( Node node ) {
    this();
    this.name = XmlHandler.getTagValue( node, "name" );
    this.hostname = XmlHandler.getTagValue( node, "hostname" );
    this.port = XmlHandler.getTagValue( node, "port" );
    this.webAppName = XmlHandler.getTagValue( node, "webAppName" );
    this.username = XmlHandler.getTagValue( node, "username" );
    this.password = Encr.decryptPasswordOptionallyEncrypted( XmlHandler.getTagValue( node, "password" ) );
    this.proxyHostname = XmlHandler.getTagValue( node, "proxy_hostname" );
    this.proxyPort = XmlHandler.getTagValue( node, "proxy_port" );
    this.nonProxyHosts = XmlHandler.getTagValue( node, "non_proxy_hosts" );
    this.propertiesMasterName = XmlHandler.getTagValue( node, "get_properties_from_master" );
    this.overrideExistingProperties =
      "Y".equalsIgnoreCase( XmlHandler.getTagValue( node, "override_existing_properties" ) );
    this.log = new LogChannel( this );

    setSslMode( "Y".equalsIgnoreCase( XmlHandler.getTagValue( node, SSL_MODE_TAG ) ) );
    Node sslConfig = XmlHandler.getSubNode( node, SslConfiguration.XML_TAG );
    if ( sslConfig != null ) {
      setSslMode( true );
      this.sslConfig = new SslConfiguration( sslConfig );
    }
  }

  public ILogChannel getLogChannel() {
    return log;
  }

  public String getXml() {
    StringBuilder xml = new StringBuilder();

    xml.append( "      " ).append( XmlHandler.openTag( XML_TAG ) ).append( Const.CR );

    xml.append( "        " ).append( XmlHandler.addTagValue( "name", name ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "hostname", hostname ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "port", port ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "webAppName", webAppName ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "username", username ) );
    xml.append( XmlHandler.addTagValue( "password", Encr.encryptPasswordIfNotUsingVariables( password ), false ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "proxy_hostname", proxyHostname ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "proxy_port", proxyPort ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "non_proxy_hosts", nonProxyHosts ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( SSL_MODE_TAG, isSslMode(), false ) );
    if ( sslConfig != null ) {
      xml.append( sslConfig.getXml() );
    }

    xml.append( "      " ).append( XmlHandler.closeTag( XML_TAG ) ).append( Const.CR );

    return xml.toString();
  }

  public Object clone() {
    HopServer hopServer = new HopServer();
    hopServer.replaceMeta( this );
    return hopServer;
  }

  public void replaceMeta( HopServer hopServer ) {
    this.name = hopServer.name;
    this.hostname = hopServer.hostname;
    this.port = hopServer.port;
    this.webAppName = hopServer.webAppName;
    this.username = hopServer.username;
    this.password = hopServer.password;
    this.proxyHostname = hopServer.proxyHostname;
    this.proxyPort = hopServer.proxyPort;
    this.nonProxyHosts = hopServer.nonProxyHosts;
    this.sslMode = hopServer.sslMode;
  }

  public String toString() {
    return name;
  }

  public String getServerAndPort(IVariables variables) {
    String realHostname;

    realHostname = variables.resolve( hostname );

    if ( !Utils.isEmpty( realHostname ) ) {
      return realHostname + getPortSpecification(variables);
    }
    return "Hop Server";
  }

  public boolean equals( Object obj ) {
    if ( !( obj instanceof HopServer ) ) {
      return false;
    }
    HopServer server = (HopServer) obj;
    return name.equalsIgnoreCase( server.getName() );
  }

  public int hashCode() {
    return name.toLowerCase().hashCode();
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname( String urlString ) {
    this.hostname = urlString;
  }

  /**
   * @return the password
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password the password to set
   */
  public void setPassword( String password ) {
    this.password = password;
  }

  /**
   * @return the username
   */
  public String getUsername() {
    return username;
  }

  /**
   * @param username the username to set
   */
  public void setUsername( String username ) {
    this.username = username;
  }

  /**
   * @return the username
   */
  public String getWebAppName() {
    return webAppName;
  }

  /**
   * @param webAppName the web application name to set
   */
  public void setWebAppName( String webAppName ) {
    this.webAppName = webAppName;
  }

  /**
   * @return the nonProxyHosts
   */
  public String getNonProxyHosts() {
    return nonProxyHosts;
  }

  /**
   * @param nonProxyHosts the nonProxyHosts to set
   */
  public void setNonProxyHosts( String nonProxyHosts ) {
    this.nonProxyHosts = nonProxyHosts;
  }

  /**
   * @return the proxyHostname
   */
  public String getProxyHostname() {
    return proxyHostname;
  }

  /**
   * @param proxyHostname the proxyHostname to set
   */
  public void setProxyHostname( String proxyHostname ) {
    this.proxyHostname = proxyHostname;
  }

  /**
   * @return the proxyPort
   */
  public String getProxyPort() {
    return proxyPort;
  }

  /**
   * @param proxyPort the proxyPort to set
   */
  public void setProxyPort( String proxyPort ) {
    this.proxyPort = proxyPort;
  }

  /**
   * @return the Master name for read properties
   */
  public String getPropertiesMasterName() {
    return propertiesMasterName;
  }

  /**
   * @return flag for read properties from Master
   */
  public boolean isOverrideExistingProperties() {
    return overrideExistingProperties;
  }

  /**
   * @return the port
   */
  public String getPort() {
    return port;
  }

  /**
   * @param port the port to set
   */
  public void setPort( String port ) {
    this.port = port;
  }

  public String getPortSpecification(IVariables variables) {
    String realPort = variables.resolve( port );
    String portSpec = ":" + realPort;
    if ( Utils.isEmpty( realPort ) || port.equals( "80" ) ) {
      portSpec = "";
    }
    return portSpec;

  }

  public String constructUrl( IVariables variables, String serviceAndArguments ) throws UnsupportedEncodingException {
    String realHostname = null;
    String proxyHostname = null;
    realHostname = variables.resolve( hostname );
    proxyHostname = variables.resolve( getProxyHostname() );
    if ( !Utils.isEmpty( proxyHostname ) && realHostname.equals( "localhost" ) ) {
      realHostname = "127.0.0.1";
    }

    if ( !StringUtils.isBlank( webAppName ) ) {
      serviceAndArguments = "/" + variables.resolve( getWebAppName() ) + serviceAndArguments;
    }

    String result =
      ( isSslMode() ? HTTPS : HTTP ) + "://" + realHostname + getPortSpecification(variables) + serviceAndArguments;
    result = Const.replace( result, " ", "%20" );
    return result;

  }

  // Method is defined as package-protected in order to be accessible by unit tests
  HttpPost buildSendXmlMethod( IVariables variables, byte[] content, String service ) throws Exception {
    // Prepare HTTP put
    //
    String urlString = constructUrl( variables, service );
    if ( log.isDebug() ) {
      log.logDebug( BaseMessages.getString( PKG, "HopServer.DEBUG_ConnectingTo", urlString ) );
    }
    HttpPost postMethod = new HttpPost( urlString );

    // Request content will be retrieved directly from the input stream
    //
    HttpEntity entity = new ByteArrayEntity( content );

    postMethod.setEntity( entity );
    postMethod.addHeader( new BasicHeader( "Content-Type", "text/xml;charset=" + Const.XML_ENCODING ) );

    return postMethod;
  }

  public String sendXml( IVariables variables, String xml, String service ) throws Exception {
    HttpPost method = buildSendXmlMethod( variables, xml.getBytes( Const.XML_ENCODING ), service );
    try {
      return executeAuth( variables, method );
    } finally {
      // Release current connection to the connection pool once you are done
      method.releaseConnection();
      if ( log.isDetailed() ) {
        log.logDetailed( BaseMessages.getString( PKG, "HopServer.DETAILED_SentXmlToService", service,
          variables.resolve( hostname ) ) );
      }
    }
  }

  /**
   * Throws if not ok
   */
  private void handleStatus( IVariables variables, HttpUriRequest method, StatusLine statusLine, int status ) throws HopException {
    if ( status >= 300 ) {
      String message;
      if ( status == HttpStatus.SC_NOT_FOUND ) {
        message = String.format( "%s%s%s%s",
          BaseMessages.getString( PKG, "HopServer.Error.404.Title" ),
          Const.CR, Const.CR,
          BaseMessages.getString( PKG, "HopServer.Error.404.Message" )
        );
      } else {
        message = String.format( "HTTP Status %d - %s - %s",
          status,
          method.getURI().toString(),
          statusLine.getReasonPhrase() );
      }
      throw new HopException( message );
    }
  }

  // Method is defined as package-protected in order to be accessible by unit tests
  HttpPost buildSendExportMethod( IVariables variables, String type, String load, InputStream is ) throws UnsupportedEncodingException {
    String serviceUrl = RegisterPackageServlet.CONTEXT_PATH;
    if ( type != null && load != null ) {
      serviceUrl +=
        "/?" + RegisterPackageServlet.PARAMETER_TYPE + "=" + type
          + "&" + RegisterPackageServlet.PARAMETER_LOAD + "=" + URLEncoder.encode( load, "UTF-8" );
    }

    String urlString = constructUrl( variables, serviceUrl );
    if ( log.isDebug() ) {
      log.logDebug( BaseMessages.getString( PKG, "HopServer.DEBUG_ConnectingTo", urlString ) );
    }

    HttpPost method = new HttpPost( urlString );
    method.setEntity( new InputStreamEntity( is ) );
    method.addHeader( new BasicHeader( "Content-Type", "binary/zip" ) );

    return method;
  }

  /**
   * Send an exported archive over to this hop server
   *
   * @param filename The archive to send
   * @param type     The type of file to add to the hop server (AddExportServlet.TYPE_*)
   * @param load     The filename to load in the archive (the .hwf or .hpl)
   * @return the XML of the web result
   * @throws Exception in case something goes awry
   */
  public String sendExport( IVariables variables, String filename, String type, String load ) throws Exception {
    // Request content will be retrieved directly from the input stream
    try ( InputStream is = HopVfs.getInputStream( HopVfs.getFileObject( filename ) ) ) {
      // Execute request
      HttpPost method = buildSendExportMethod( variables, type, load, is );
      try {
        return executeAuth( variables, method );
      } finally {
        // Release current connection to the connection pool once you are done
        method.releaseConnection();
        if ( log.isDetailed() ) {
          log.logDetailed( BaseMessages.getString( PKG, "HopServer.DETAILED_SentExportToService",
            RegisterPackageServlet.CONTEXT_PATH, variables.resolve( hostname ) ) );
        }
      }
    }
  }

  /**
   * Executes method with authentication.
   *
   * @param method
   * @return
   * @throws IOException
   * @throws ClientProtocolException
   * @throws HopException            if response not ok
   */
  private String executeAuth( IVariables variables, HttpUriRequest method ) throws IOException, ClientProtocolException, HopException {
    HttpResponse httpResponse = getHttpClient().execute( method, getAuthContext(variables) );
    return getResponse( variables, method, httpResponse );
  }

  private String getResponse( IVariables variables, HttpUriRequest method, HttpResponse httpResponse ) throws IOException, HopException {
    StatusLine statusLine = httpResponse.getStatusLine();
    int statusCode = statusLine.getStatusCode();
    // The status code
    if ( log.isDebug() ) {
      log.logDebug( BaseMessages.getString( PKG, "HopServer.DEBUG_ResponseStatus", Integer.toString( statusCode ) ) );
    }

    String responseBody = getResponseBodyAsString( httpResponse.getEntity().getContent() );
    if ( log.isDebug() ) {
      log.logDebug( BaseMessages.getString( PKG, "HopServer.DEBUG_ResponseBody", responseBody ) );
    }

    // throw if not ok
    handleStatus( variables, method, statusLine, statusCode );

    return responseBody;
  }

  private void addCredentials( IVariables variables, HttpClientContext context ) {

    String host = variables.resolve( hostname );
    int port = Const.toInt( variables.resolve( this.port ), 80 );
    String userName = variables.resolve( username );
    String password = Encr.decryptPasswordOptionallyEncrypted( variables.resolve( this.password ) );
    String proxyHost = variables.resolve( proxyHostname );

    CredentialsProvider provider = new BasicCredentialsProvider();
    UsernamePasswordCredentials credentials = new UsernamePasswordCredentials( userName, password );
    if ( !Utils.isEmpty( proxyHost ) && host.equals( "localhost" ) ) {
      host = "127.0.0.1";
    }
    provider.setCredentials( new AuthScope( host, port ), credentials );
    context.setCredentialsProvider( provider );
    // Generate BASIC scheme object and add it to the local auth cache
    HttpHost target = new HttpHost( host, port, isSslMode() ? HTTPS : HTTP );
    AuthCache authCache = new BasicAuthCache();
    BasicScheme basicAuth = new BasicScheme();
    authCache.put( target, basicAuth );
    context.setAuthCache( authCache );
  }

  private void addProxy( IVariables variables, HttpClientContext context ) {
    String proxyHost = variables.resolve( this.proxyHostname );
    String proxyPort = variables.resolve( this.proxyPort );
    String nonProxyHosts = variables.resolve( this.nonProxyHosts );

    String hostName = variables.resolve( this.hostname );
    if ( Utils.isEmpty( proxyHost ) || Utils.isEmpty( proxyPort ) ) {
      return;
    }
    // skip applying proxy if non-proxy host matches
    if ( !Utils.isEmpty( nonProxyHosts ) && hostName.matches( nonProxyHosts ) ) {
      return;
    }
    HttpHost httpHost = new HttpHost( proxyHost, Integer.valueOf( proxyPort ) );

    RequestConfig requestConfig = RequestConfig.custom()
      .setProxy( httpHost )
      .build();

    context.setRequestConfig( requestConfig );
  }

  /**
   * @return HttpClientContext with authorization credentials
   */
  protected HttpClientContext getAuthContext(IVariables variables) {
    HttpClientContext context = HttpClientContext.create();
    addCredentials( variables, context );
    addProxy( variables, context );
    return context;
  }

  public String execService( IVariables variables, String service, boolean retry ) throws Exception {
    int tries = 0;
    int maxRetries = 0;
    if ( retry ) {
      maxRetries = HOP_SERVER_RETRIES;
    }
    while ( true ) {
      try {
        return execService( variables, service );
      } catch ( Exception e ) {
        if ( tries >= maxRetries ) {
          throw e;
        } else {
          try {
            Thread.sleep( getDelay( tries ) );
          } catch ( InterruptedException e2 ) {
            //ignore
          }
        }
      }
      tries++;
    }
  }

  public static long getDelay( int trial ) {
    long current = HOP_SERVER_RETRY_BACKOFF_INCREMENTS;
    long previous = 0;
    for ( int i = 0; i < trial; i++ ) {
      long tmp = current;
      current = current + previous;
      previous = tmp;
    }
    return current + RANDOM.nextInt( (int) Math.min( Integer.MAX_VALUE, current / 4L ) );
  }

  public String execService( IVariables variables, String service ) throws Exception {
    return execService( variables, service, new HashMap<>() );
  }

  // Method is defined as package-protected in order to be accessible by unit tests
  String getResponseBodyAsString( InputStream is ) throws IOException {
    BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( is ) );
    StringBuilder bodyBuffer = new StringBuilder();
    String line;

    try {
      while ( ( line = bufferedReader.readLine() ) != null ) {
        bodyBuffer.append( line );
      }
    } finally {
      bufferedReader.close();
    }

    return bodyBuffer.toString();
  }

  // Method is defined as package-protected in order to be accessible by unit tests
  HttpGet buildExecuteServiceMethod( IVariables variables, String service, Map<String, String> headerValues )
    throws UnsupportedEncodingException {
    HttpGet method = new HttpGet( constructUrl( variables, service ) );

    for ( String key : headerValues.keySet() ) {
      method.setHeader( key, headerValues.get( key ) );
    }
    return method;
  }

  public String execService( IVariables variables, String service, Map<String, String> headerValues ) throws Exception {
    // Prepare HTTP get
    HttpGet method = buildExecuteServiceMethod( variables, service, headerValues );
    // Execute request
    try {
      HttpResponse httpResponse = getHttpClient().execute( method, getAuthContext(variables) );
      StatusLine statusLine = httpResponse.getStatusLine();
      int statusCode = statusLine.getStatusCode();

      // The status code
      if ( log.isDebug() ) {
        log.logDebug(
          BaseMessages.getString( PKG, "HopServer.DEBUG_ResponseStatus", Integer.toString( statusCode ) ) );
      }

      String responseBody = getResponseBodyAsString( httpResponse.getEntity().getContent() );

      if ( log.isDetailed() ) {
        log.logDetailed( BaseMessages.getString( PKG, "HopServer.DETAILED_FinishedReading", Integer
          .toString( responseBody.getBytes().length ) ) );
      }
      if ( log.isDebug() ) {
        log.logDebug( BaseMessages.getString( PKG, "HopServer.DEBUG_ResponseBody", responseBody ) );
      }

      if ( statusCode >= 400 ) {
        throw new HopException( String.format( "HTTP Status %d - %s - %s", statusCode, method.getURI().toString(),
          statusLine.getReasonPhrase() ) );
      }

      return responseBody;
    } finally {
      // Release current connection to the connection pool once you are done
      method.releaseConnection();
      if ( log.isDetailed() ) {
        log.logDetailed( BaseMessages.getString( PKG, "HopServer.DETAILED_ExecutedService", service, hostname ) );
      }
    }
  }

  // Method is defined as package-protected in order to be accessible by unit tests
  HttpClient getHttpClient() {
    ServerConnectionManager connectionManager = ServerConnectionManager.getInstance();
    return connectionManager.createHttpClient();
  }

  public HopServerStatus getStatus(IVariables variables) throws Exception {
    String xml = execService( variables, GetStatusServlet.CONTEXT_PATH + "/?xml=Y" );
    return HopServerStatus.fromXml( xml );
  }

  public HopServerPipelineStatus getPipelineStatus( IVariables variables, String pipelineName, String serverObjectId, int startLogLineNr )
    throws Exception {
    return getPipelineStatus( variables, pipelineName, serverObjectId, startLogLineNr, false );
  }

  public HopServerPipelineStatus getPipelineStatus( IVariables variables, String pipelineName, String serverObjectId, int startLogLineNr,
                                                      boolean sendResultXmlWithStatus )
    throws Exception {
    String query = GetPipelineStatusServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id="
      + Const.NVL( serverObjectId, "" ) + "&xml=Y&from=" + startLogLineNr;
    if ( sendResultXmlWithStatus ) {
      query = query + "&" + GetPipelineStatusServlet.SEND_RESULT + "=Y";
    }
    String xml = execService( variables, query, true );
    return HopServerPipelineStatus.fromXml( xml );
  }

  public HopServerWorkflowStatus getWorkflowStatus( IVariables variables, String workflowName, String serverObjectId, int startLogLineNr )
    throws Exception {
    String xml =
      execService( variables, GetWorkflowStatusServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( workflowName, "UTF-8" ) + "&id="
        + Const.NVL( serverObjectId, "" ) + "&xml=Y&from=" + startLogLineNr, true );
    return HopServerWorkflowStatus.fromXml( xml );
  }

  public WebResult stopPipeline( IVariables variables, String pipelineName, String serverObjectId ) throws Exception {
    String xml =
      execService( variables, StopPipelineServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id="
        + Const.NVL( serverObjectId, "" ) + "&xml=Y" );
    return WebResult.fromXmlString( xml );
  }

  public WebResult pauseResumePipeline( IVariables variables, String pipelineName, String serverObjectId ) throws Exception {
    String xml =
      execService( variables, PausePipelineServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id="
        + Const.NVL( serverObjectId, "" ) + "&xml=Y" );
    return WebResult.fromXmlString( xml );
  }

  public WebResult removePipeline( IVariables variables, String pipelineName, String serverObjectId ) throws Exception {
    String xml =
      execService( variables, RemovePipelineServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id="
        + Const.NVL( serverObjectId, "" ) + "&xml=Y" );
    return WebResult.fromXmlString( xml );
  }

  public WebResult removeWorkflow( IVariables variables, String workflowName, String serverObjectId ) throws Exception {
    String xml =
      execService( variables, RemoveWorkflowServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( workflowName, "UTF-8" ) + "&id="
        + Const.NVL( serverObjectId, "" ) + "&xml=Y" );
    return WebResult.fromXmlString( xml );
  }

  public WebResult stopWorkflow( IVariables variables, String pipelineName, String serverObjectId ) throws Exception {
    String xml =
      execService( variables, StopWorkflowServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( pipelineName, "UTF-8" ) + "&xml=Y&id="
        + Const.NVL( serverObjectId, "" ) );
    return WebResult.fromXmlString( xml );
  }

  public WebResult startPipeline( IVariables variables, String pipelineName, String serverObjectId ) throws Exception {
    String xml =
      execService( variables, StartPipelineServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id="
        + Const.NVL( serverObjectId, "" ) + "&xml=Y" );
    return WebResult.fromXmlString( xml );
  }

  public WebResult startWorkflow( IVariables variables, String workflowName, String serverObjectId ) throws Exception {
    String xml =
      execService( variables, StartWorkflowServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( workflowName, "UTF-8" ) + "&xml=Y&id="
        + Const.NVL( serverObjectId, "" ) );
    return WebResult.fromXmlString( xml );
  }

  public static HopServer findHopServer( List<HopServer> hopServers, String name ) {
    for ( HopServer hopServer : hopServers ) {
      if ( hopServer.getName() != null && hopServer.getName().equalsIgnoreCase( name ) ) {
        return hopServer;
      }
    }
    return null;
  }

  public static String[] getHopServerNames( IHopMetadataProvider metadataProvider ) throws HopException {
    List<String> names = metadataProvider.getSerializer( HopServer.class ).listObjectNames();
    return names.toArray( new String[ 0 ] );
  }

  public String getDescription() {
    // NOT USED
    return null;
  }

  public void setDescription( String description ) {
    // NOT USED
  }

  /**
   * Verify the name of the hop server and if required, change it if it already exists in the list of hop servers.
   *
   * @param hopServers the hop servers to check against.
   * @param oldname      the old name of the hop server
   * @return the new hop server name
   */
  public String verifyAndModifyHopServerName( List<HopServer> hopServers, String oldname ) {
    String name = getName();
    if ( name.equalsIgnoreCase( oldname ) ) {
      return name; // nothing to see here: move along!
    }

    int nr = 2;
    while ( HopServer.findHopServer( hopServers, getName() ) != null ) {
      setName( name + " " + nr );
      nr++;
    }
    return getName();
  }

  /**
   * Sniff rows on a the hop server, return xml containing the row metadata and data.
   *
   * @param pipelineName  pipeline name
   * @param id            the id on the server
   * @param transformName transform name
   * @param copyNr        transform copy number
   * @param lines         lines number
   * @param type          transform type
   * @return xml with row metadata and data
   * @throws Exception
   */
  public String sniffTransform( IVariables variables, String pipelineName, String transformName, String id, String copyNr, int lines, String type ) throws Exception {
    return execService( variables, SniffTransformServlet.CONTEXT_PATH +
      "/?pipeline=" + URLEncoder.encode( pipelineName, "UTF-8" ) +
      "&id=" + URLEncoder.encode( id, "UTF-8" ) +
      "&transform=" + URLEncoder.encode( transformName, "UTF-8" ) +
      "&copynr=" + copyNr + "&type=" + type + "&lines=" + lines + "&xml=Y" );
  }

  public long getNextServerSequenceValue( IVariables variables, String serverSequenceName, long incrementValue ) throws HopException {
    try {
      String xml =
        execService( variables, NextSequenceValueServlet.CONTEXT_PATH + "/" + "?" + NextSequenceValueServlet.PARAM_NAME + "="
          + URLEncoder.encode( serverSequenceName, "UTF-8" ) + "&" + NextSequenceValueServlet.PARAM_INCREMENT + "="
          + Long.toString( incrementValue ) );

      Document doc = XmlHandler.loadXmlString( xml );
      Node seqNode = XmlHandler.getSubNode( doc, NextSequenceValueServlet.XML_TAG );
      String nextValueString = XmlHandler.getTagValue( seqNode, NextSequenceValueServlet.XML_TAG_VALUE );
      String errorString = XmlHandler.getTagValue( seqNode, NextSequenceValueServlet.XML_TAG_ERROR );

      if ( !Utils.isEmpty( errorString ) ) {
        throw new HopException( errorString );
      }
      if ( Utils.isEmpty( nextValueString ) ) {
        throw new HopException( "No value retrieved from server sequence '" + serverSequenceName + "' on server "
          + toString() );
      }
      long nextValue = Const.toLong( nextValueString, Long.MIN_VALUE );
      if ( nextValue == Long.MIN_VALUE ) {
        throw new HopException( "Incorrect value '" + nextValueString + "' retrieved from server sequence '"
          + serverSequenceName + "' on server " + toString() );
      }

      return nextValue;
    } catch ( Exception e ) {
      throw new HopException( "There was a problem retrieving a next sequence value from server sequence '"
        + serverSequenceName + "' on server " + toString(), e );
    }
  }

  public HopServer getClient() {
    String pHostName = getHostname();
    String pPort = getPort();
    String name = MessageFormat.format( "Dynamic server [{0}:{1}]", pHostName, pPort );
    HopServer client = new HopServer( name, pHostName, pPort, getUsername(), getPassword() );
    client.setSslMode( isSslMode() );
    return client;
  }

  /**
   * Monitors a remote pipeline every 5 seconds.
   *
   * @param log            the log channel interface
   * @param serverObjectId the HopServer object ID
   * @param pipelineName   the pipeline name
   */
  public void monitorRemotePipeline( IVariables variables, ILogChannel log, String serverObjectId, String pipelineName ) {
    monitorRemotePipeline( variables, log, serverObjectId, pipelineName, 5 );
  }

  /**
   * Monitors a remote pipeline at the specified interval.
   *
   * @param log              the log channel interface
   * @param serverObjectId   the HopServer object ID
   * @param pipelineName     the pipeline name
   * @param sleepTimeSeconds the sleep time (in seconds)
   */
  public void monitorRemotePipeline( IVariables variables, ILogChannel log, String serverObjectId, String pipelineName, int sleepTimeSeconds ) {
    long errors = 0;
    boolean allFinished = false;
    while ( !allFinished && errors == 0 ) {
      allFinished = true;
      errors = 0L;

      // Check the remote server
      if ( allFinished && errors == 0 ) {
        try {
          HopServerPipelineStatus pipelineStatus = getPipelineStatus( variables, pipelineName, serverObjectId, 0 );
          if ( pipelineStatus.isRunning() ) {
            if ( log.isDetailed() ) {
              log.logDetailed( pipelineName, "Remote pipeline is still running." );
            }
            allFinished = false;
          } else {
            if ( log.isDetailed() ) {
              log.logDetailed( pipelineName, "Remote pipeline has finished." );
            }
          }
          Result result = pipelineStatus.getResult();
          errors += result.getNrErrors();
        } catch ( Exception e ) {
          errors += 1;
          log.logError( pipelineName, "Unable to contact remote hop server '" + this.getName() + "' to check pipeline status : " + e.toString() );
        }
      }

      //
      // Keep waiting until all pipelines have finished
      // If needed, we stop them again and again until they yield.
      //
      if ( !allFinished ) {
        // Not finished or error: wait a bit longer
        if ( log.isDetailed() ) {
          log.logDetailed( pipelineName, "The remote pipeline is still running, waiting a few seconds..." );
        }
        try {
          Thread.sleep( sleepTimeSeconds * 1000 );
        } catch ( Exception e ) {
          // Ignore errors
        }
      }
    }

    log.logBasic( pipelineName, "The remote pipeline has finished." );
  }


  /**
   * Monitors a remote workflow every 5 seconds.
   *
   * @param log            the log channel interface
   * @param serverObjectId the HopServer object ID
   * @param workflowName   the workflow name
   */
  public void monitorRemoteWorkflow( IVariables variables, ILogChannel log, String serverObjectId, String workflowName ) {
    monitorRemoteWorkflow( variables, log, serverObjectId, workflowName, 5 );
  }

  /**
   * Monitors a remote workflow at the specified interval.
   *
   * @param log              the log channel interface
   * @param serverObjectId   the HopServer object ID
   * @param workflowName     the workflow name
   * @param sleepTimeSeconds the sleep time (in seconds)
   */
  public void monitorRemoteWorkflow( IVariables variables, ILogChannel log, String serverObjectId, String workflowName, int sleepTimeSeconds ) {
    long errors = 0;
    boolean allFinished = false;
    while ( !allFinished && errors == 0 ) {
      allFinished = true;
      errors = 0L;

      // Check the remote server
      if ( allFinished && errors == 0 ) {
        try {
          HopServerWorkflowStatus jobStatus = getWorkflowStatus( variables, workflowName, serverObjectId, 0 );
          if ( jobStatus.isRunning() ) {
            if ( log.isDetailed() ) {
              log.logDetailed( workflowName, "Remote workflow is still running." );
            }
            allFinished = false;
          } else {
            if ( log.isDetailed() ) {
              log.logDetailed( workflowName, "Remote workflow has finished." );
            }
          }
          Result result = jobStatus.getResult();
          errors += result.getNrErrors();
        } catch ( Exception e ) {
          errors += 1;
          log.logError( workflowName, "Unable to contact remote hop server '" + this.getName() + "' to check workflow status : " + e.toString() );
        }
      }

      //
      // Keep waiting until all pipelines have finished
      // If needed, we stop them again and again until they yield.
      //
      if ( !allFinished ) {
        // Not finished or error: wait a bit longer
        if ( log.isDetailed() ) {
          log.logDetailed( workflowName, "The remote workflow is still running, waiting a few seconds..." );
        }
        try {
          Thread.sleep( sleepTimeSeconds * 1000 );
        } catch ( Exception e ) {
          // Ignore errors
        }
      }
    }

    log.logBasic( workflowName, "The remote workflow has finished." );
  }


  /**
   * @return the changedDate
   */
  public Date getChangedDate() {
    return changedDate;
  }

  /**
   * @param sslMode
   */
  public void setSslMode( boolean sslMode ) {
    this.sslMode = sslMode;
  }

  /**
   * @return the sslMode
   */
  public boolean isSslMode() {
    return sslMode;
  }

  /**
   * @return the sslConfig
   */
  public SslConfiguration getSslConfig() {
    return sslConfig;
  }

  /**
   * @param sslConfig The sslConfig to set
   */
  public void setSslConfig( SslConfiguration sslConfig ) {
    this.sslConfig = sslConfig;
  }

  /**
   * @param overrideExistingProperties The overrideExistingProperties to set
   */
  public void setOverrideExistingProperties( boolean overrideExistingProperties ) {
    this.overrideExistingProperties = overrideExistingProperties;
  }

  /**
   * @param propertiesMasterName The propertiesMasterName to set
   */
  public void setPropertiesMasterName( String propertiesMasterName ) {
    this.propertiesMasterName = propertiesMasterName;
  }
}
