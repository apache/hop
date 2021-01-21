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

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.server.HopServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class HopServerConfig {
  public static final String XML_TAG = "hop-server-config";

  public static final String XML_TAG_SEQUENCES = "sequences";
  public static final String XML_TAG_AUTOSEQUENCE = "autosequence";
  public static final String XML_TAG_AUTO_CREATE = "autocreate";
  public static final String XML_TAG_JETTY_OPTIONS = "jetty_options";
  public static final String XML_TAG_ACCEPTORS = "acceptors";
  public static final String XML_TAG_ACCEPT_QUEUE_SIZE = "acceptQueueSize";
  public static final String XML_TAG_LOW_RES_MAX_IDLE_TIME = "lowResourcesMaxIdleTime";

  private HopServer hopServer;

  private boolean joining;

  private int maxLogLines;

  private int maxLogTimeoutMinutes;

  private int objectTimeoutMinutes;

  private String filename;

  private List<DatabaseMeta> databases;
  private List<HopServerSequence> hopServerSequences;

  private HopServerSequence autoSequence;

  private boolean automaticCreationAllowed;

  private String passwordFile;

  private IVariables variables;

  public HopServerConfig() {
    databases = new ArrayList<>();
    hopServerSequences = new ArrayList<>();
    automaticCreationAllowed = false;
    passwordFile = null; // force lookup by server in ~/.hop or local folder
    variables = Variables.getADefaultVariableSpace();
  }

  public HopServerConfig( HopServer hopServer ) {
    this();
    this.hopServer = hopServer;
  }

  public String getXml() {

    StringBuilder xml = new StringBuilder();

    xml.append( XmlHandler.openTag( XML_TAG ) );

    if ( hopServer != null ) {
      xml.append( hopServer.getXml() );
    }

    XmlHandler.addTagValue( "joining", joining );
    XmlHandler.addTagValue( "max_log_lines", maxLogLines );
    XmlHandler.addTagValue( "max_log_timeout_minutes", maxLogTimeoutMinutes );
    XmlHandler.addTagValue( "object_timeout_minutes", objectTimeoutMinutes );

    xml.append( XmlHandler.openTag( XML_TAG_SEQUENCES ) );
    for ( HopServerSequence hopServerSequence : hopServerSequences ) {
      xml.append( XmlHandler.openTag( HopServerSequence.XML_TAG ) );
      xml.append( hopServerSequence.getXml() );
      xml.append( XmlHandler.closeTag( HopServerSequence.XML_TAG ) );
    }
    xml.append( XmlHandler.closeTag( XML_TAG_SEQUENCES ) );

    if ( autoSequence != null ) {
      xml.append( XmlHandler.openTag( XML_TAG_AUTOSEQUENCE ) );
      xml.append( autoSequence.getXml() );
      xml.append( XmlHandler.addTagValue( XML_TAG_AUTO_CREATE, automaticCreationAllowed ) );
      xml.append( XmlHandler.closeTag( XML_TAG_AUTOSEQUENCE ) );
    }

    xml.append( XmlHandler.closeTag( XML_TAG ) );

    return xml.toString();
  }

  public HopServerConfig( ILogChannel log, Node node ) throws HopXmlException {
    this();
    Node hopServerNode = XmlHandler.getSubNode( node, HopServer.XML_TAG );
    if ( hopServerNode != null ) {
      hopServer = new HopServer( hopServerNode );
      checkNetworkInterfaceSetting( log, hopServerNode, hopServer );
    }

    joining = "Y".equalsIgnoreCase( XmlHandler.getTagValue( node, "joining" ) );
    maxLogLines = Const.toInt( XmlHandler.getTagValue( node, "max_log_lines" ), 0 );
    maxLogTimeoutMinutes = Const.toInt( XmlHandler.getTagValue( node, "max_log_timeout_minutes" ), 0 );
    objectTimeoutMinutes = Const.toInt( XmlHandler.getTagValue( node, "object_timeout_minutes" ), 0 );

    // Read sequence information
    //

    // TODO : read databases back in
    //
    Node sequencesNode = XmlHandler.getSubNode( node, "sequences" );
    List<Node> seqNodes = XmlHandler.getNodes( sequencesNode, HopServerSequence.XML_TAG );
    for ( Node seqNode : seqNodes ) {
      hopServerSequences.add( new HopServerSequence( seqNode, databases ) );
    }

    Node autoSequenceNode = XmlHandler.getSubNode( node, XML_TAG_AUTOSEQUENCE );
    if ( autoSequenceNode != null ) {
      autoSequence = new HopServerSequence( autoSequenceNode, databases );
      automaticCreationAllowed =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( autoSequenceNode, XML_TAG_AUTO_CREATE ) );
    }

    // Set Jetty Options
    setUpJettyOptions( node );
  }

  /**
   * Set up jetty options to the system properties
   *
   * @param node
   */
  protected void setUpJettyOptions( Node node ) {
    Map<String, String> jettyOptions = parseJettyOptions( node );

    if ( jettyOptions != null && jettyOptions.size() > 0 ) {
      for ( Entry<String, String> jettyOption : jettyOptions.entrySet() ) {
        System.setProperty( jettyOption.getKey(), jettyOption.getValue() );
      }
    }
  }

  /**
   * Read and parse jetty options
   *
   * @param node that contains jetty options nodes
   * @return map of not empty jetty options
   */
  protected Map<String, String> parseJettyOptions( Node node ) {

    Map<String, String> jettyOptions = null;

    Node jettyOptionsNode = XmlHandler.getSubNode( node, XML_TAG_JETTY_OPTIONS );

    if ( jettyOptionsNode != null ) {

      jettyOptions = new HashMap<>();
      if ( XmlHandler.getTagValue( jettyOptionsNode, XML_TAG_ACCEPTORS ) != null ) {
        jettyOptions.put( Const.HOP_SERVER_JETTY_ACCEPTORS, XmlHandler.getTagValue( jettyOptionsNode, XML_TAG_ACCEPTORS ) );
      }
      if ( XmlHandler.getTagValue( jettyOptionsNode, XML_TAG_ACCEPT_QUEUE_SIZE ) != null ) {
        jettyOptions.put( Const.HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE, XmlHandler.getTagValue( jettyOptionsNode,
          XML_TAG_ACCEPT_QUEUE_SIZE ) );
      }
      if ( XmlHandler.getTagValue( jettyOptionsNode, XML_TAG_LOW_RES_MAX_IDLE_TIME ) != null ) {
        jettyOptions.put( Const.HOP_SERVER_JETTY_RES_MAX_IDLE_TIME, XmlHandler.getTagValue( jettyOptionsNode,
          XML_TAG_LOW_RES_MAX_IDLE_TIME ) );
      }
    }
    return jettyOptions;
  }

  public void readAutoSequences() throws HopException {
    if ( autoSequence == null ) {
      return;
    }

    Database database = null;

    try {
      DatabaseMeta databaseMeta = autoSequence.getDatabaseMeta();
      ILoggingObject loggingInterface =
        new SimpleLoggingObject( "auto-sequence", LoggingObjectType.GENERAL, null );
      database = new Database( loggingInterface, variables, databaseMeta );
      database.connect();
      String schemaTable =
        databaseMeta.getQuotedSchemaTableCombination( variables, autoSequence.getSchemaName(), autoSequence.getTableName() );
      String seqField = databaseMeta.quoteField( autoSequence.getSequenceNameField() );
      String valueField = databaseMeta.quoteField( autoSequence.getValueField() );

      String sql = "SELECT " + seqField + ", " + valueField + " FROM " + schemaTable;
      List<Object[]> rows = database.getRows( sql, 0 );
      IRowMeta rowMeta = database.getReturnRowMeta();
      for ( Object[] row : rows ) {
        // Automatically create a new sequence for each sequence found...
        //
        String sequenceName = rowMeta.getString( row, seqField, null );
        if ( !Utils.isEmpty( sequenceName ) ) {
          Long value = rowMeta.getInteger( row, valueField, null );
          if ( value != null ) {
            HopServerSequence hopServerSequence =
              new HopServerSequence( sequenceName, value, databaseMeta, autoSequence.getSchemaName(), autoSequence
                .getTableName(), autoSequence.getSequenceNameField(), autoSequence.getValueField() );

            hopServerSequences.add( hopServerSequence );

            LogChannel.GENERAL.logBasic( "Automatically created server sequence '"
              + hopServerSequence.getName() + "' with start value " + hopServerSequence.getStartValue() );
          }
        }
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to automatically configure server sequences", e );
    } finally {
      if ( database != null ) {
        database.disconnect();
      }
    }
  }

  private void checkNetworkInterfaceSetting( ILogChannel log, Node serverNode, HopServer hopServer ) {
    // See if we need to grab the network interface to use and then override the host name
    //
    String networkInterfaceName = XmlHandler.getTagValue( serverNode, "network_interface" );
    if ( !Utils.isEmpty( networkInterfaceName ) ) {
      // OK, so let's try to get the IP address for this network interface...
      //
      try {
        String newHostname = Const.getIPAddress( networkInterfaceName );
        if ( newHostname != null ) {
          hopServer.setHostname( newHostname );
          // Also change the name of the server...
          //
          hopServer.setName( hopServer.getName() + "-" + newHostname );
          log.logBasic( "Hostname for hop server ["
            + hopServer.getName() + "] is set to [" + newHostname + "], information derived from network "
            + networkInterfaceName );
        }
      } catch ( SocketException e ) {
        log.logError( "Unable to get the IP address for network interface "
          + networkInterfaceName + " for hop server [" + hopServer.getName() + "]", e );
      }
    }

  }

  public HopServerConfig( String hostname, int port, boolean joining ) {
    this();
    this.joining = joining;
    this.hopServer = new HopServer( hostname + ":" + port, hostname, "" + port, null, null );
  }


  /**
   * @return the hop server.<br>
   * The user name and password defined in here are used to contact this server by the masters.
   */
  public HopServer getHopServer() {
    return hopServer;
  }

  /**
   * @param hopServer the hop server details to set.<br>
   *                    The user name and password defined in here are used to contact this server by the masters.
   */
  public void setHopServer( HopServer hopServer ) {
    this.hopServer = hopServer;
  }

  /**
   * @return true if the webserver needs to join with the webserver threads (wait/block until finished)
   */
  public boolean isJoining() {
    return joining;
  }

  /**
   * @param joining Set to true if the webserver needs to join with the webserver threads (wait/block until finished)
   */
  public void setJoining( boolean joining ) {
    this.joining = joining;
  }

  /**
   * @return the maxLogLines
   */
  public int getMaxLogLines() {
    return maxLogLines;
  }

  /**
   * @param maxLogLines the maxLogLines to set
   */
  public void setMaxLogLines( int maxLogLines ) {
    this.maxLogLines = maxLogLines;
  }

  /**
   * @return the maxLogTimeoutMinutes
   */
  public int getMaxLogTimeoutMinutes() {
    return maxLogTimeoutMinutes;
  }

  /**
   * @param maxLogTimeoutMinutes the maxLogTimeoutMinutes to set
   */
  public void setMaxLogTimeoutMinutes( int maxLogTimeoutMinutes ) {
    this.maxLogTimeoutMinutes = maxLogTimeoutMinutes;
  }

  /**
   * @return the objectTimeoutMinutes
   */
  public int getObjectTimeoutMinutes() {
    return objectTimeoutMinutes;
  }

  /**
   * @param objectTimeoutMinutes the objectTimeoutMinutes to set
   */
  public void setObjectTimeoutMinutes( int objectTimeoutMinutes ) {
    this.objectTimeoutMinutes = objectTimeoutMinutes;
  }

  /**
   * @return the filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename the filename to set
   */
  public void setFilename( String filename ) {
    this.filename = filename;
  }

  /**
   * @return the databases
   */
  public List<DatabaseMeta> getDatabases() {
    return databases;
  }

  /**
   * @param databases the databases to set
   */
  public void setDatabases( List<DatabaseMeta> databases ) {
    this.databases = databases;
  }

  /**
   * @return the hopServerSequences
   */
  public List<HopServerSequence> getHopServerSequences() {
    return hopServerSequences;
  }

  /**
   * @param hopServerSequences the hopServerSequences to set
   */
  public void setHopServerSequences( List<HopServerSequence> hopServerSequences ) {
    this.hopServerSequences = hopServerSequences;
  }

  /**
   * @return the autoSequence
   */
  public HopServerSequence getAutoSequence() {
    return autoSequence;
  }

  /**
   * @param autoSequence the autoSequence to set
   */
  public void setAutoSequence( HopServerSequence autoSequence ) {
    this.autoSequence = autoSequence;
  }

  /**
   * @return the automaticCreationAllowed
   */
  public boolean isAutomaticCreationAllowed() {
    return automaticCreationAllowed;
  }

  /**
   * @param automaticCreationAllowed the automaticCreationAllowed to set
   */
  public void setAutomaticCreationAllowed( boolean automaticCreationAllowed ) {
    this.automaticCreationAllowed = automaticCreationAllowed;
  }

  public String getPasswordFile() {
    return passwordFile;
  }

  public void setPasswordFile( String passwordFile ) {
    this.passwordFile = passwordFile;
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
  public void setVariables( IVariables variables ) {
    this.variables = variables;
  }
}
