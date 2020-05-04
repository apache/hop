/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.cluster;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.changed.ChangedFlag;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.IXml;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.IHopMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.metastore.persist.MetaStoreElementType;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.metastore.util.HopDefaults;
import org.apache.hop.www.GetPipelineStatusServlet;
import org.apache.hop.www.GetStatusServlet;
import org.apache.hop.www.GetWorkflowStatusServlet;
import org.apache.hop.www.NextSequenceValueServlet;
import org.apache.hop.www.PausePipelineServlet;
import org.apache.hop.www.RegisterPackageServlet;
import org.apache.hop.www.RemovePipelineServlet;
import org.apache.hop.www.RemoveWorkflowServlet;
import org.apache.hop.www.SlaveServerPipelineStatus;
import org.apache.hop.www.SlaveServerStatus;
import org.apache.hop.www.SlaveServerWorkflowStatus;
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

@MetaStoreElementType(
  name = "Slave Server",
  description = "Defines a Hop Slave Server"
)
public class SlaveServer extends ChangedFlag implements Cloneable, IVariables, IXml, IHopMetaStoreElement<SlaveServer> {
  private static Class<?> PKG = SlaveServer.class; // for i18n purposes, needed by Translator!!

  public static final String STRING_SLAVESERVER = "Slave Server";

  private static final Random RANDOM = new Random();

  public static final String XML_TAG = "slaveserver";

  private static final String HTTP = "http";
  private static final String HTTPS = "https";

  public static final String SSL_MODE_TAG = "sslMode";

  public static final int HOP_CARTE_RETRIES = getNumberOfSlaveServerRetries();

  public static final int HOP_CARTE_RETRY_BACKOFF_INCREMENTS = getBackoffIncrements();

  private static int getNumberOfSlaveServerRetries() {
    try {
      return Integer.parseInt( Const.NVL( System.getProperty( "HOP_CARTE_RETRIES" ), "0" ) );
    } catch ( Exception e ) {
      return 0;
    }
  }

  public static int getBackoffIncrements() {
    try {
      return Integer.parseInt( Const.NVL( System.getProperty( "HOP_CARTE_RETRY_BACKOFF_INCREMENTS" ), "1000" ) );
    } catch ( Exception e ) {
      return 1000;
    }
  }

  private ILogChannel log;

  private String name;

  @MetaStoreAttribute
  private String hostname;

  @MetaStoreAttribute
  private String port;

  @MetaStoreAttribute
  private String webAppName;

  @MetaStoreAttribute
  private String username;

  @MetaStoreAttribute( password = true )
  private String password;

  @MetaStoreAttribute
  private String proxyHostname;

  @MetaStoreAttribute
  private String proxyPort;

  @MetaStoreAttribute
  private String nonProxyHosts;

  @MetaStoreAttribute
  private String propertiesMasterName;

  @MetaStoreAttribute
  private boolean overrideExistingProperties;

  @MetaStoreAttribute
  private boolean master;

  private IVariables variables = new Variables();

  private Date changedDate;

  @MetaStoreAttribute
  private boolean sslMode;

  @MetaStoreAttribute
  private SslConfiguration sslConfig;

  public SlaveServer() {
    initializeVariablesFrom( null );
    this.log = new LogChannel( STRING_SLAVESERVER );
    this.changedDate = new Date();
  }

  public SlaveServer( String name, String hostname, String port, String username, String password ) {
    this( name, hostname, port, username, password, null, null, null, false, false );
  }

  public SlaveServer( String name, String hostname, String port, String username, String password,
                      String proxyHostname, String proxyPort, String nonProxyHosts, boolean master ) {
    this( name, hostname, port, username, password, proxyHostname, proxyPort, nonProxyHosts, master, false );
  }

  public SlaveServer( String name, String hostname, String port, String username, String password,
                      String proxyHostname, String proxyPort, String nonProxyHosts, boolean master, boolean ssl ) {
    this();
    this.name = name;
    this.hostname = hostname;
    this.port = port;
    this.username = username;
    this.password = password;

    this.proxyHostname = proxyHostname;
    this.proxyPort = proxyPort;
    this.nonProxyHosts = nonProxyHosts;

    this.master = master;
    initializeVariablesFrom( null );
    this.log = new LogChannel( this );
  }

  public SlaveServer( Node slaveNode ) {
    this();
    this.name = XmlHandler.getTagValue( slaveNode, "name" );
    this.hostname = XmlHandler.getTagValue( slaveNode, "hostname" );
    this.port = XmlHandler.getTagValue( slaveNode, "port" );
    this.webAppName = XmlHandler.getTagValue( slaveNode, "webAppName" );
    this.username = XmlHandler.getTagValue( slaveNode, "username" );
    this.password = Encr.decryptPasswordOptionallyEncrypted( XmlHandler.getTagValue( slaveNode, "password" ) );
    this.proxyHostname = XmlHandler.getTagValue( slaveNode, "proxy_hostname" );
    this.proxyPort = XmlHandler.getTagValue( slaveNode, "proxy_port" );
    this.nonProxyHosts = XmlHandler.getTagValue( slaveNode, "non_proxy_hosts" );
    this.propertiesMasterName = XmlHandler.getTagValue( slaveNode, "get_properties_from_master" );
    this.overrideExistingProperties =
      "Y".equalsIgnoreCase( XmlHandler.getTagValue( slaveNode, "override_existing_properties" ) );
    this.master = "Y".equalsIgnoreCase( XmlHandler.getTagValue( slaveNode, "master" ) );
    initializeVariablesFrom( null );
    this.log = new LogChannel( this );

    setSslMode( "Y".equalsIgnoreCase( XmlHandler.getTagValue( slaveNode, SSL_MODE_TAG ) ) );
    Node sslConfig = XmlHandler.getSubNode( slaveNode, SslConfiguration.XML_TAG );
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
    xml.append( "        " ).append( XmlHandler.addTagValue( "master", master ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( SSL_MODE_TAG, isSslMode(), false ) );
    if ( sslConfig != null ) {
      xml.append( sslConfig.getXml() );
    }

    xml.append( "      " ).append( XmlHandler.closeTag( XML_TAG ) ).append( Const.CR );

    return xml.toString();
  }

  public Object clone() {
    SlaveServer slaveServer = new SlaveServer();
    slaveServer.replaceMeta( this );
    return slaveServer;
  }

  public void replaceMeta( SlaveServer slaveServer ) {
    this.name = slaveServer.name;
    this.hostname = slaveServer.hostname;
    this.port = slaveServer.port;
    this.webAppName = slaveServer.webAppName;
    this.username = slaveServer.username;
    this.password = slaveServer.password;
    this.proxyHostname = slaveServer.proxyHostname;
    this.proxyPort = slaveServer.proxyPort;
    this.nonProxyHosts = slaveServer.nonProxyHosts;
    this.master = slaveServer.master;

    this.sslMode = slaveServer.sslMode;

    this.setChanged( true );

  }

  public String toString() {
    return name;
  }

  public String getServerAndPort() {
    String realHostname;

    realHostname = environmentSubstitute( hostname );

    if ( !Utils.isEmpty( realHostname ) ) {
      return realHostname + getPortSpecification();
    }
    return "Slave Server";
  }

  public boolean equals( Object obj ) {
    if ( !( obj instanceof SlaveServer ) ) {
      return false;
    }
    SlaveServer slave = (SlaveServer) obj;
    return name.equalsIgnoreCase( slave.getName() );
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

  public String getPortSpecification() {
    String realPort = environmentSubstitute( port );
    String portSpec = ":" + realPort;
    if ( Utils.isEmpty( realPort ) || port.equals( "80" ) ) {
      portSpec = "";
    }
    return portSpec;

  }

  public String constructUrl( String serviceAndArguments ) throws UnsupportedEncodingException {
    String realHostname = null;
    String proxyHostname = null;
    realHostname = environmentSubstitute( hostname );
    proxyHostname = environmentSubstitute( getProxyHostname() );
    if ( !Utils.isEmpty( proxyHostname ) && realHostname.equals( "localhost" ) ) {
      realHostname = "127.0.0.1";
    }

    if ( !StringUtils.isBlank( webAppName ) ) {
      serviceAndArguments = "/" + environmentSubstitute( getWebAppName() ) + serviceAndArguments;
    }

    String result =
      ( isSslMode() ? HTTPS : HTTP ) + "://" + realHostname + getPortSpecification() + serviceAndArguments;
    result = Const.replace( result, " ", "%20" );
    return result;

  }

  // Method is defined as package-protected in order to be accessible by unit tests
  HttpPost buildSendXmlMethod(byte[] content, String service ) throws Exception {
    // Prepare HTTP put
    //
    String urlString = constructUrl( service );
    if ( log.isDebug() ) {
      log.logDebug( BaseMessages.getString( PKG, "SlaveServer.DEBUG_ConnectingTo", urlString ) );
    }
    HttpPost postMethod = new HttpPost( urlString );

    // Request content will be retrieved directly from the input stream
    //
    HttpEntity entity = new ByteArrayEntity( content );

    postMethod.setEntity( entity );
    postMethod.addHeader( new BasicHeader( "Content-Type", "text/xml;charset=" + Const.XML_ENCODING ) );

    return postMethod;
  }

  public String sendXml(String xml, String service ) throws Exception {
    HttpPost method = buildSendXmlMethod( xml.getBytes( Const.XML_ENCODING ), service );
    try {
      return executeAuth( method );
    } finally {
      // Release current connection to the connection pool once you are done
      method.releaseConnection();
      if ( log.isDetailed() ) {
        log.logDetailed( BaseMessages.getString( PKG, "SlaveServer.DETAILED_SentXmlToService", service,
          environmentSubstitute( hostname ) ) );
      }
    }
  }

  /**
   * Throws if not ok
   */
  private void handleStatus( HttpUriRequest method, StatusLine statusLine, int status ) throws HopException {
    if ( status >= 300 ) {
      String message;
      if ( status == HttpStatus.SC_NOT_FOUND ) {
        message = String.format( "%s%s%s%s",
          BaseMessages.getString( PKG, "SlaveServer.Error.404.Title" ),
          Const.CR, Const.CR,
          BaseMessages.getString( PKG, "SlaveServer.Error.404.Message" )
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
  HttpPost buildSendExportMethod( String type, String load, InputStream is ) throws UnsupportedEncodingException {
    String serviceUrl = RegisterPackageServlet.CONTEXT_PATH;
    if ( type != null && load != null ) {
      serviceUrl +=
        "/?" + RegisterPackageServlet.PARAMETER_TYPE + "=" + type
          + "&" + RegisterPackageServlet.PARAMETER_LOAD + "=" + URLEncoder.encode( load, "UTF-8" );
    }

    String urlString = constructUrl( serviceUrl );
    if ( log.isDebug() ) {
      log.logDebug( BaseMessages.getString( PKG, "SlaveServer.DEBUG_ConnectingTo", urlString ) );
    }

    HttpPost method = new HttpPost( urlString );
    method.setEntity( new InputStreamEntity( is ) );
    method.addHeader( new BasicHeader( "Content-Type", "binary/zip" ) );

    return method;
  }

  /**
   * Send an exported archive over to this slave server
   *
   * @param filename The archive to send
   * @param type     The type of file to add to the slave server (AddExportServlet.TYPE_*)
   * @param load     The filename to load in the archive (the .hwf or .hpl)
   * @return the XML of the web result
   * @throws Exception in case something goes awry
   */
  public String sendExport( String filename, String type, String load ) throws Exception {
    // Request content will be retrieved directly from the input stream
    try ( InputStream is = HopVfs.getInputStream( HopVfs.getFileObject( filename ) ) ) {
      // Execute request
      HttpPost method = buildSendExportMethod( type, load, is );
      try {
        return executeAuth( method );
      } finally {
        // Release current connection to the connection pool once you are done
        method.releaseConnection();
        if ( log.isDetailed() ) {
          log.logDetailed( BaseMessages.getString( PKG, "SlaveServer.DETAILED_SentExportToService",
            RegisterPackageServlet.CONTEXT_PATH, environmentSubstitute( hostname ) ) );
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
  private String executeAuth( HttpUriRequest method ) throws IOException, ClientProtocolException, HopException {
    HttpResponse httpResponse = getHttpClient().execute( method, getAuthContext() );
    return getResponse( method, httpResponse );
  }

  private String getResponse( HttpUriRequest method, HttpResponse httpResponse ) throws IOException, HopException {
    StatusLine statusLine = httpResponse.getStatusLine();
    int statusCode = statusLine.getStatusCode();
    // The status code
    if ( log.isDebug() ) {
      log.logDebug( BaseMessages.getString( PKG, "SlaveServer.DEBUG_ResponseStatus", Integer.toString( statusCode ) ) );
    }

    String responseBody = getResponseBodyAsString( httpResponse.getEntity().getContent() );
    if ( log.isDebug() ) {
      log.logDebug( BaseMessages.getString( PKG, "SlaveServer.DEBUG_ResponseBody", responseBody ) );
    }

    // throw if not ok
    handleStatus( method, statusLine, statusCode );

    return responseBody;
  }

  private void addCredentials( HttpClientContext context ) {

    String host = environmentSubstitute( hostname );
    int port = Const.toInt( environmentSubstitute( this.port ), 80 );
    String userName = environmentSubstitute( username );
    String password = Encr.decryptPasswordOptionallyEncrypted( environmentSubstitute( this.password ) );
    String proxyHost = environmentSubstitute( proxyHostname );

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

  private void addProxy( HttpClientContext context ) {
    String proxyHost = environmentSubstitute( this.proxyHostname );
    String proxyPort = environmentSubstitute( this.proxyPort );
    String nonProxyHosts = environmentSubstitute( this.nonProxyHosts );

    String hostName = environmentSubstitute( this.hostname );
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
  protected HttpClientContext getAuthContext() {
    HttpClientContext context = HttpClientContext.create();
    addCredentials( context );
    addProxy( context );
    return context;
  }

  /**
   * @return the master
   */
  public boolean isMaster() {
    return master;
  }

  /**
   * @param master the master to set
   */
  public void setMaster( boolean master ) {
    this.master = master;
  }

  public String execService( String service, boolean retry ) throws Exception {
    int tries = 0;
    int maxRetries = 0;
    if ( retry ) {
      maxRetries = HOP_CARTE_RETRIES;
    }
    while ( true ) {
      try {
        return execService( service );
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
    long current = HOP_CARTE_RETRY_BACKOFF_INCREMENTS;
    long previous = 0;
    for ( int i = 0; i < trial; i++ ) {
      long tmp = current;
      current = current + previous;
      previous = tmp;
    }
    return current + RANDOM.nextInt( (int) Math.min( Integer.MAX_VALUE, current / 4L ) );
  }

  public String execService( String service ) throws Exception {
    return execService( service, new HashMap<>() );
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
  HttpGet buildExecuteServiceMethod( String service, Map<String, String> headerValues )
    throws UnsupportedEncodingException {
    HttpGet method = new HttpGet( constructUrl( service ) );

    for ( String key : headerValues.keySet() ) {
      method.setHeader( key, headerValues.get( key ) );
    }
    return method;
  }

  public String execService( String service, Map<String, String> headerValues ) throws Exception {
    // Prepare HTTP get
    HttpGet method = buildExecuteServiceMethod( service, headerValues );
    // Execute request
    try {
      HttpResponse httpResponse = getHttpClient().execute( method, getAuthContext() );
      StatusLine statusLine = httpResponse.getStatusLine();
      int statusCode = statusLine.getStatusCode();

      // The status code
      if ( log.isDebug() ) {
        log.logDebug(
          BaseMessages.getString( PKG, "SlaveServer.DEBUG_ResponseStatus", Integer.toString( statusCode ) ) );
      }

      String responseBody = getResponseBodyAsString( httpResponse.getEntity().getContent() );

      if ( log.isDetailed() ) {
        log.logDetailed( BaseMessages.getString( PKG, "SlaveServer.DETAILED_FinishedReading", Integer
          .toString( responseBody.getBytes().length ) ) );
      }
      if ( log.isDebug() ) {
        log.logDebug( BaseMessages.getString( PKG, "SlaveServer.DEBUG_ResponseBody", responseBody ) );
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
        log.logDetailed( BaseMessages.getString( PKG, "SlaveServer.DETAILED_ExecutedService", service, hostname ) );
      }
    }
  }

  // Method is defined as package-protected in order to be accessible by unit tests
  HttpClient getHttpClient() {
    SlaveConnectionManager connectionManager = SlaveConnectionManager.getInstance();
    return connectionManager.createHttpClient();
  }

  public SlaveServerStatus getStatus() throws Exception {
    String xml = execService( GetStatusServlet.CONTEXT_PATH + "/?xml=Y" );
    return SlaveServerStatus.fromXml( xml );
  }

  public SlaveServerPipelineStatus getPipelineStatus( String pipelineName, String serverObjectId, int startLogLineNr )
    throws Exception {
    return getPipelineStatus( pipelineName, serverObjectId, startLogLineNr, false );
  }

  public SlaveServerPipelineStatus getPipelineStatus( String pipelineName, String serverObjectId, int startLogLineNr,
                                                      boolean sendResultXmlWithStatus )
    throws Exception {
    String query = GetPipelineStatusServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id="
      + Const.NVL( serverObjectId, "" ) + "&xml=Y&from=" + startLogLineNr;
    if ( sendResultXmlWithStatus ) {
      query = query + "&" + GetPipelineStatusServlet.SEND_RESULT + "=Y";
    }
    String xml = execService( query, true );
    return SlaveServerPipelineStatus.fromXml( xml );
  }

  public SlaveServerWorkflowStatus getWorkflowStatus( String workflowName, String serverObjectId, int startLogLineNr )
    throws Exception {
    String xml =
      execService( GetWorkflowStatusServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( workflowName, "UTF-8" ) + "&id="
        + Const.NVL( serverObjectId, "" ) + "&xml=Y&from=" + startLogLineNr, true );
    return SlaveServerWorkflowStatus.fromXml( xml );
  }

  public WebResult stopPipeline( String pipelineName, String serverObjectId ) throws Exception {
    String xml =
      execService( StopPipelineServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id="
        + Const.NVL( serverObjectId, "" ) + "&xml=Y" );
    return WebResult.fromXmlString( xml );
  }

  public WebResult pauseResumePipeline( String pipelineName, String serverObjectId ) throws Exception {
    String xml =
      execService( PausePipelineServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id="
        + Const.NVL( serverObjectId, "" ) + "&xml=Y" );
    return WebResult.fromXmlString( xml );
  }

  public WebResult removePipeline( String pipelineName, String serverObjectId ) throws Exception {
    String xml =
      execService( RemovePipelineServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id="
        + Const.NVL( serverObjectId, "" ) + "&xml=Y" );
    return WebResult.fromXmlString( xml );
  }

  public WebResult removeWorkflow( String workflowName, String serverObjectId ) throws Exception {
    String xml =
      execService( RemoveWorkflowServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( workflowName, "UTF-8" ) + "&id="
        + Const.NVL( serverObjectId, "" ) + "&xml=Y" );
    return WebResult.fromXmlString( xml );
  }

  public WebResult stopWorkflow( String pipelineName, String serverObjectId ) throws Exception {
    String xml =
      execService( StopWorkflowServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( pipelineName, "UTF-8" ) + "&xml=Y&id="
        + Const.NVL( serverObjectId, "" ) );
    return WebResult.fromXmlString( xml );
  }

  public WebResult startPipeline( String pipelineName, String serverObjectId ) throws Exception {
    String xml =
      execService( StartPipelineServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id="
        + Const.NVL( serverObjectId, "" ) + "&xml=Y" );
    return WebResult.fromXmlString( xml );
  }

  public WebResult startWorkflow( String workflowName, String serverObjectId ) throws Exception {
    String xml =
      execService( StartWorkflowServlet.CONTEXT_PATH + "/?name=" + URLEncoder.encode( workflowName, "UTF-8" ) + "&xml=Y&id="
        + Const.NVL( serverObjectId, "" ) );
    return WebResult.fromXmlString( xml );
  }

  public static SlaveServer findSlaveServer( List<SlaveServer> slaveServers, String name ) {
    for ( SlaveServer slaveServer : slaveServers ) {
      if ( slaveServer.getName() != null && slaveServer.getName().equalsIgnoreCase( name ) ) {
        return slaveServer;
      }
    }
    return null;
  }

  public static String[] getSlaveServerNames( IMetaStore metaStore ) throws MetaStoreException {
    MetaStoreFactory<SlaveServer> factory = createFactory( metaStore );
    List<String> names = factory.getElementNames();
    return names.toArray( new String[ 0 ] );
  }


  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public void copyVariablesFrom( IVariables variables ) {
    this.variables.copyVariablesFrom( variables );
  }

  public String environmentSubstitute( String aString ) {
    return variables.environmentSubstitute( aString );
  }

  public String[] environmentSubstitute( String[] aString ) {
    return variables.environmentSubstitute( aString );
  }

  public String fieldSubstitute( String aString, IRowMeta rowMeta, Object[] rowData )
    throws HopValueException {
    return variables.fieldSubstitute( aString, rowMeta, rowData );
  }

  public IVariables getParentVariableSpace() {
    return variables.getParentVariableSpace();
  }

  public void setParentVariableSpace( IVariables parent ) {
    variables.setParentVariableSpace( parent );
  }

  public String getVariable( String variableName, String defaultValue ) {
    return variables.getVariable( variableName, defaultValue );
  }

  public String getVariable( String variableName ) {
    return variables.getVariable( variableName );
  }

  public boolean getBooleanValueOfVariable( String variableName, boolean defaultValue ) {
    if ( !Utils.isEmpty( variableName ) ) {
      String value = environmentSubstitute( variableName );
      if ( !Utils.isEmpty( value ) ) {
        return ValueMetaString.convertStringToBoolean( value );
      }
    }
    return defaultValue;
  }

  public void initializeVariablesFrom( IVariables parent ) {
    variables.initializeVariablesFrom( parent );
  }

  public String[] listVariables() {
    return variables.listVariables();
  }

  public void setVariable( String variableName, String variableValue ) {
    variables.setVariable( variableName, variableValue );
  }

  public void shareVariablesWith( IVariables variables ) {
    this.variables = variables;
  }

  public void injectVariables( Map<String, String> prop ) {
    variables.injectVariables( prop );
  }

  public String getDescription() {
    // NOT USED
    return null;
  }

  public void setDescription( String description ) {
    // NOT USED
  }

  /**
   * Verify the name of the slave server and if required, change it if it already exists in the list of slave servers.
   *
   * @param slaveServers the slave servers to check against.
   * @param oldname      the old name of the slave server
   * @return the new slave server name
   */
  public String verifyAndModifySlaveServerName( List<SlaveServer> slaveServers, String oldname ) {
    String name = getName();
    if ( name.equalsIgnoreCase( oldname ) ) {
      return name; // nothing to see here: move along!
    }

    int nr = 2;
    while ( SlaveServer.findSlaveServer( slaveServers, getName() ) != null ) {
      setName( name + " " + nr );
      nr++;
    }
    return getName();
  }

  /**
   * Sniff rows on a the slave server, return xml containing the row metadata and data.
   *
   * @param pipelineName pipeline name
   * @param id the id on the server
   * @param transformName  transform name
   * @param copyNr    transform copy number
   * @param lines     lines number
   * @param type      transform type
   * @return xml with row metadata and data
   * @throws Exception
   */
  public String sniffTransform( String pipelineName, String transformName, String id, String copyNr, int lines, String type ) throws Exception {
    return execService( SniffTransformServlet.CONTEXT_PATH +
      "/?pipeline=" + URLEncoder.encode( pipelineName, "UTF-8" ) +
      "&id=" + URLEncoder.encode( id, "UTF-8" ) +
      "&transform=" + URLEncoder.encode( transformName, "UTF-8" ) +
      "&copynr=" + copyNr + "&type=" + type + "&lines=" + lines + "&xml=Y" );
  }

  public long getNextSlaveSequenceValue( String slaveSequenceName, long incrementValue ) throws HopException {
    try {
      String xml =
        execService( NextSequenceValueServlet.CONTEXT_PATH + "/" + "?" + NextSequenceValueServlet.PARAM_NAME + "="
          + URLEncoder.encode( slaveSequenceName, "UTF-8" ) + "&" + NextSequenceValueServlet.PARAM_INCREMENT + "="
          + Long.toString( incrementValue ) );

      Document doc = XmlHandler.loadXmlString( xml );
      Node seqNode = XmlHandler.getSubNode( doc, NextSequenceValueServlet.XML_TAG );
      String nextValueString = XmlHandler.getTagValue( seqNode, NextSequenceValueServlet.XML_TAG_VALUE );
      String errorString = XmlHandler.getTagValue( seqNode, NextSequenceValueServlet.XML_TAG_ERROR );

      if ( !Utils.isEmpty( errorString ) ) {
        throw new HopException( errorString );
      }
      if ( Utils.isEmpty( nextValueString ) ) {
        throw new HopException( "No value retrieved from slave sequence '" + slaveSequenceName + "' on slave "
          + toString() );
      }
      long nextValue = Const.toLong( nextValueString, Long.MIN_VALUE );
      if ( nextValue == Long.MIN_VALUE ) {
        throw new HopException( "Incorrect value '" + nextValueString + "' retrieved from slave sequence '"
          + slaveSequenceName + "' on slave " + toString() );
      }

      return nextValue;
    } catch ( Exception e ) {
      throw new HopException( "There was a problem retrieving a next sequence value from slave sequence '"
        + slaveSequenceName + "' on slave " + toString(), e );
    }
  }

  public SlaveServer getClient() {
    String pHostName = getHostname();
    String pPort = getPort();
    String name = MessageFormat.format( "Dynamic slave [{0}:{1}]", pHostName, pPort );
    SlaveServer client = new SlaveServer( name, pHostName, pPort, getUsername(), getPassword() );
    client.setSslMode( isSslMode() );
    return client;
  }

  /**
   * Monitors a remote pipeline every 5 seconds.
   *
   * @param log           the log channel interface
   * @param serverObjectId the HopServer object ID
   * @param pipelineName     the pipeline name
   */
  public void monitorRemotePipeline( ILogChannel log, String serverObjectId, String pipelineName ) {
    monitorRemotePipeline( log, serverObjectId, pipelineName, 5 );
  }

  /**
   * Monitors a remote pipeline at the specified interval.
   *
   * @param log              the log channel interface
   * @param serverObjectId    the HopServer object ID
   * @param pipelineName        the pipeline name
   * @param sleepTimeSeconds the sleep time (in seconds)
   */
  public void monitorRemotePipeline( ILogChannel log, String serverObjectId, String pipelineName, int sleepTimeSeconds ) {
    long errors = 0;
    boolean allFinished = false;
    while ( !allFinished && errors == 0 ) {
      allFinished = true;
      errors = 0L;

      // Check the remote server
      if ( allFinished && errors == 0 ) {
        try {
          SlaveServerPipelineStatus pipelineStatus = getPipelineStatus( pipelineName, serverObjectId, 0 );
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
          log.logError( pipelineName, "Unable to contact remote slave server '" + this.getName() + "' to check pipeline status : " + e.toString() );
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
        } // Check all slaves every x seconds.
      }
    }

    log.logMinimal( pipelineName, "The remote pipeline has finished." );
  }


  /**
   * Monitors a remote workflow every 5 seconds.
   *
   * @param log           the log channel interface
   * @param serverObjectId the HopServer object ID
   * @param workflowName       the workflow name
   */
  public void monitorRemoteJob( ILogChannel log, String serverObjectId, String workflowName ) {
    monitorRemoteJob( log, serverObjectId, workflowName, 5 );
  }

  /**
   * Monitors a remote workflow at the specified interval.
   *
   * @param log              the log channel interface
   * @param serverObjectId    the HopServer object ID
   * @param workflowName          the workflow name
   * @param sleepTimeSeconds the sleep time (in seconds)
   */
  public void monitorRemoteJob( ILogChannel log, String serverObjectId, String workflowName, int sleepTimeSeconds ) {
    long errors = 0;
    boolean allFinished = false;
    while ( !allFinished && errors == 0 ) {
      allFinished = true;
      errors = 0L;

      // Check the remote server
      if ( allFinished && errors == 0 ) {
        try {
          SlaveServerWorkflowStatus jobStatus = getWorkflowStatus( workflowName, serverObjectId, 0 );
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
          log.logError( workflowName, "Unable to contact remote slave server '" + this.getName() + "' to check workflow status : " + e.toString() );
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
        } // Check all slaves every x seconds.
      }
    }

    log.logMinimal( workflowName, "The remote workflow has finished." );
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
   * @param overrideExistingProperties The overrideExistingProperties to set
   */
  public void setOverrideExistingProperties( boolean overrideExistingProperties ) {
    this.overrideExistingProperties = overrideExistingProperties;
  }

  /**
   * No plugin, object factory or anything, just return the factory
   *
   * @param metaStore The metastore to use
   * @return
   */
  @Override public MetaStoreFactory<SlaveServer> getFactory( IMetaStore metaStore ) {
    return createFactory( metaStore );
  }

  public static final MetaStoreFactory<SlaveServer> createFactory( IMetaStore metaStore ) {
    return new MetaStoreFactory<>( SlaveServer.class, metaStore, HopDefaults.NAMESPACE );
  }
}
