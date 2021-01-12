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

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.json.JSONConfiguration;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.PrintWriter;
import java.io.StringWriter;

public class HopServer {
  private static final Class<?> PKG = HopServer.class; // For Translator

  private WebServer webServer;
  private HopServerConfig config;
  private boolean allOK;
  private static Options options;

  public HopServer( final HopServerConfig config ) throws Exception {
    this( config, null );
  }

  public HopServer( final HopServerConfig config, Boolean joinOverride ) throws Exception {
    this.config = config;

    allOK = true;

    HopServerSingleton.setHopServerConfig( config );
    ILogChannel log = HopServerSingleton.getInstance().getLog();

    final PipelineMap pipelineMap = HopServerSingleton.getInstance().getPipelineMap();
    pipelineMap.setHopServerConfig( config );
    final WorkflowMap workflowMap = HopServerSingleton.getInstance().getWorkflowMap();
    workflowMap.setHopServerConfig( config );

    org.apache.hop.server.HopServer hopServer = config.getHopServer();

    String hostname = hopServer.getHostname();
    int port = WebServer.PORT;
    if ( !Utils.isEmpty( hopServer.getPort() ) ) {
      try {
        port = Integer.parseInt( hopServer.getPort() );
      } catch ( Exception e ) {
        log.logError( BaseMessages.getString( PKG, "HopServer.Error.CanNotPartPort", hopServer.getHostname(), "" + port ),
          e );
        allOK = false;
      }
    }

    // If we need to time out finished or idle objects, we should create a timer in the background to clean
    // this is done automatically now
    // HopServerSingleton.installPurgeTimer(config, log, pipelineMap, workflowMap);

    if ( allOK ) {
      boolean shouldJoin = config.isJoining();
      if ( joinOverride != null ) {
        shouldJoin = joinOverride;
      }

      this.webServer = new WebServer( log, pipelineMap, workflowMap, hostname, port, shouldJoin, config.getPasswordFile(), hopServer.getSslConfig() );
    }
  }

  public static void main( String[] args ) {
    try {
      parseAndRunCommand( args );
    } catch ( Exception e ) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings( "static-access" )
  private static void parseAndRunCommand( String[] args ) throws Exception {
    options = new Options();
    options.addOption( OptionBuilder.withLongOpt( "stop" ).withDescription( BaseMessages.getString( PKG,
      "HopServer.ParamDescription.stop" ) ).hasArg( false ).isRequired( false ).create( 's' ) );
    options.addOption( OptionBuilder.withLongOpt( "userName" ).withDescription( BaseMessages.getString( PKG,
      "HopServer.ParamDescription.userName" ) ).hasArg( true ).isRequired( false ).create( 'u' ) );
    options.addOption( OptionBuilder.withLongOpt( "password" ).withDescription( BaseMessages.getString( PKG,
      "HopServer.ParamDescription.password" ) ).hasArg( true ).isRequired( false ).create( 'p' ) );
    options.addOption( OptionBuilder.withLongOpt( "help" ).withDescription( BaseMessages.getString( PKG,
      "HopServer.ParamDescription.help" ) ).create( 'h' ) );

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse( options, args );

    if ( cmd.hasOption( 'h' ) ) {
      displayHelpAndAbort();
    }

    String[] arguments = cmd.getArgs();
    boolean usingConfigFile = false;

    // Load from an xml file that describes the complete configuration...
    //
    HopServerConfig config = null;
    if ( arguments.length == 1 && !Utils.isEmpty( arguments[ 0 ] ) ) {
      if ( cmd.hasOption( 's' ) ) {
        throw new HopServerCommandException( BaseMessages.getString( PKG, "HopServer.Error.illegalStop" ) );
      }
      usingConfigFile = true;
      FileObject file = HopVfs.getFileObject( arguments[ 0 ] );
      Document document = XmlHandler.loadXmlFile( file );
      setHopEnvironment(); // Must stand up server now to allow decryption of password
      Node configNode = XmlHandler.getSubNode( document, HopServerConfig.XML_TAG );
      config = new HopServerConfig( new LogChannel( "Hop server config" ), configNode );
      if ( config.getAutoSequence() != null ) {
        config.readAutoSequences();
      }
      config.setFilename( arguments[ 0 ] );
    }
    if ( arguments.length == 2 && !Utils.isEmpty( arguments[ 0 ] ) && !Utils.isEmpty( arguments[ 1 ] ) ) {
      String hostname = arguments[ 0 ];
      String port = arguments[ 1 ];

      if ( cmd.hasOption( 's' ) ) {
        String user = cmd.getOptionValue( 'u' );
        String password = cmd.getOptionValue( 'p' );
        shutdown( hostname, port, user, password );
        System.exit( 0 );
      }

      org.apache.hop.server.HopServer hopServer = new org.apache.hop.server.HopServer( hostname + ":" + port, hostname, port, null, null );

      // TODO: configure the variables in HopServerConfig()
      //
      config = new HopServerConfig();
      config.setHopServer( hopServer );
    }

    // Nothing configured: show the usage
    //
    if ( config == null ) {
      displayHelpAndAbort();
    }

    if ( !usingConfigFile ) {
      setHopEnvironment();
    }
    runHopServer( config );
  }

  private static void setHopEnvironment() throws Exception {
    HopClientEnvironment.getInstance().setClient( HopClientEnvironment.ClientType.SERVER );
    HopEnvironment.init();
  }

  public static void runHopServer( HopServerConfig config ) throws Exception {
    HopLogStore.init();

    config.setJoining( true );

    HopServer hopServer = new HopServer( config, false );
    HopServerSingleton.setHopServer( hopServer );

    hopServer.getWebServer().join();
  }

  /**
   * @return the webServer
   */
  public WebServer getWebServer() {
    return webServer;
  }

  /**
   * @param webServer the webServer to set
   */
  public void setWebServer( WebServer webServer ) {
    this.webServer = webServer;
  }

  /**
   * @return the hop server (HopServer) configuration
   */
  public HopServerConfig getConfig() {
    return config;
  }

  /**
   * @param config the hop server (HopServer) configuration
   */
  public void setConfig( HopServerConfig config ) {
    this.config = config;
  }

  private static void displayHelpAndAbort() {
    HelpFormatter formatter = new HelpFormatter();
    String optionsHelp = getOptionsHelpForUsage();
    String header =
      BaseMessages.getString( PKG, "HopServer.Usage.Text" ) + optionsHelp + "\nor\n" + BaseMessages.getString( PKG,
        "HopServer.Usage.Text2" ) + "\n\n" + BaseMessages.getString( PKG, "HopServer.MainDescription" );

    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter( stringWriter );
    formatter.printHelp( printWriter, 80, "HopServerDummy", header, options, 5, 5, "", false );
    System.err.println( stripOff( stringWriter.toString(), "usage: HopServerDummy" ) );

    System.err.println( BaseMessages.getString( PKG, "HopServer.Usage.Example" ) + ": hop-server.sh 127.0.0.1 8080" );
    System.err.println( BaseMessages.getString( PKG, "HopServer.Usage.Example" ) + ": hop-server.sh 192.168.1.221 8081" );
    System.err.println();
    System.err.println( BaseMessages.getString( PKG, "HopServer.Usage.Example" ) + ": hop-server.sh /foo/bar/hop-server-config.xml" );
    System.err.println( BaseMessages.getString( PKG, "HopServer.Usage.Example" )
      + ": hop-server.sh http://www.example.com/hop-server-config.xml" );
    System.err.println( BaseMessages.getString( PKG, "HopServer.Usage.Example" )
      + ": hop-server.sh 127.0.0.1 8080 -s -u cluster -p cluster" );

    System.exit( 1 );
  }

  private static String getOptionsHelpForUsage() {
    HelpFormatter formatter = new HelpFormatter();
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter( stringWriter );
    formatter.printUsage( printWriter, 999, "", options );
    return stripOff( stringWriter.toString(), "usage: " ); // Strip off the "usage:" so it can be localized
  }

  private static String stripOff( String target, String strip ) {
    return target.substring( target.indexOf( strip ) + strip.length() );
  }

  private static void shutdown( String hostname, String port, String username, String password ) {
    try {
      callStopHopServerRestService( hostname, port, username, password );
    } catch ( Exception e ) {
      e.printStackTrace();
    }
  }

  /**
   * Checks that HopServer is running and if so, shuts down the HopServer server
   *
   * @param hostname
   * @param port
   * @param username
   * @param password
   * @throws ParseException
   * @throws HopServerCommandException
   */
  @VisibleForTesting
  static void callStopHopServerRestService( String hostname, String port, String username, String password )
    throws ParseException, HopServerCommandException {
    // get information about the remote connection
    try {
      HopClientEnvironment.init();

      ClientConfig clientConfig = new DefaultClientConfig();
      clientConfig.getFeatures().put( JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE );
      Client client = Client.create( clientConfig );

      client.addFilter( new HTTPBasicAuthFilter( username, Encr.decryptPasswordOptionallyEncrypted( password ) ) );

      // check if the user can access the hop server. Don't really need this call but may want to check it's output at
      // some point
      String contextURL = "http://" + hostname + ":" + port + "/hop";
      WebResource resource = client.resource( contextURL + "/status/?xml=Y" );
      String response = resource.get( String.class );
      if ( response == null || !response.contains( "<serverstatus>" ) ) {
        throw new HopServerCommandException( BaseMessages.getString( PKG, "HopServer.Error.NoServerFound", hostname, ""
          + port ) );
      }

      // This is the call that matters
      resource = client.resource( contextURL + "/stopHopServer" );
      response = resource.get( String.class );
      if ( response == null || !response.contains( "Shutting Down" ) ) {
        throw new HopServerCommandException( BaseMessages.getString( PKG, "HopServer.Error.NoShutdown", hostname, ""
          + port ) );
      }
    } catch ( Exception e ) {
      throw new HopServerCommandException( BaseMessages.getString( PKG, "HopServer.Error.NoServerFound", hostname, ""
        + port ), e );
    }
  }

  /**
   * Exception generated when command line fails
   */
  public static class HopServerCommandException extends Exception {
    private static final long serialVersionUID = 1L;

    public HopServerCommandException() {
    }

    public HopServerCommandException( final String message ) {
      super( message );
    }

    public HopServerCommandException( final String message, final Throwable cause ) {
      super( message, cause );
    }

    public HopServerCommandException( final Throwable cause ) {
      super( cause );
    }
  }
}
