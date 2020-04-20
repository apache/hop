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

package org.apache.hop.www;

import org.apache.hop.cluster.SlaveServer;
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
import org.apache.hop.metastore.MetaStoreConst;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.stores.delegate.DelegatingMetaStore;
import org.apache.hop.metastore.stores.memory.MemoryMetaStore;
import org.apache.hop.metastore.stores.xml.XmlMetaStore;
import org.w3c.dom.Node;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SlaveServerConfig {
  public static final String XML_TAG = "slave_config";
  public static final String XML_TAG_MASTERS = "masters";

  public static final String XML_TAG_SEQUENCES = "sequences";
  public static final String XML_TAG_AUTOSEQUENCE = "autosequence";
  public static final String XML_TAG_AUTO_CREATE = "autocreate";
  public static final String XML_TAG_JETTY_OPTIONS = "jetty_options";
  public static final String XML_TAG_ACCEPTORS = "acceptors";
  public static final String XML_TAG_ACCEPT_QUEUE_SIZE = "acceptQueueSize";
  public static final String XML_TAG_LOW_RES_MAX_IDLE_TIME = "lowResourcesMaxIdleTime";

  private List<SlaveServer> masters;

  private SlaveServer slaveServer;

  private boolean reportingToMasters;

  private boolean joining;

  private int maxLogLines;

  private int maxLogTimeoutMinutes;

  private int objectTimeoutMinutes;

  private String filename;

  private List<DatabaseMeta> databases;
  private List<SlaveSequence> slaveSequences;

  private SlaveSequence autoSequence;

  private boolean automaticCreationAllowed;

  private DelegatingMetaStore metaStore;

  private String passwordFile;

  public SlaveServerConfig() {
    masters = new ArrayList<SlaveServer>();
    databases = new ArrayList<DatabaseMeta>();
    slaveSequences = new ArrayList<SlaveSequence>();
    automaticCreationAllowed = false;
    metaStore = new DelegatingMetaStore();
    // Add the local MetaStore to the delegation.
    // This sets it as the active one.
    //
    try {
      XmlMetaStore localStore = new XmlMetaStore( MetaStoreConst.getDefaultHopMetaStoreLocation() );
      metaStore.addMetaStore( localStore );
      metaStore.setActiveMetaStoreName( localStore.getName() );
    } catch ( MetaStoreException e ) {
      LogChannel.GENERAL.logError( "Unable to open local hop metastore from [" + MetaStoreConst.getDefaultHopMetaStoreLocation() + "]", e );
      // now replace this with an in memory metastore.
      //
      try {
        MemoryMetaStore memoryStore = new MemoryMetaStore();
        memoryStore.setName( "Memory metastore" );
        metaStore.addMetaStore( memoryStore );
        metaStore.setActiveMetaStoreName( memoryStore.getName() );
      } catch ( MetaStoreException e2 ) {
        throw new RuntimeException( "Unable to add a default memory metastore to the delegating store", e );
      }
    }
    passwordFile = null; // force lookup by server in ~/.kettle or local folder
  }

  public SlaveServerConfig( SlaveServer slaveServer ) {
    this();
    this.slaveServer = slaveServer;
  }

  public SlaveServerConfig( List<SlaveServer> masters, boolean reportingToMasters, SlaveServer slaveServer ) {
    this.masters = masters;
    this.reportingToMasters = reportingToMasters;
    this.slaveServer = slaveServer;
  }

  public String getXml() {

    StringBuilder xml = new StringBuilder();

    xml.append( XmlHandler.openTag( XML_TAG ) );

    for ( SlaveServer slaveServer : masters ) {
      xml.append( slaveServer.getXml() );
    }

    XmlHandler.addTagValue( "report_to_masters", reportingToMasters );

    if ( slaveServer != null ) {
      xml.append( slaveServer.getXml() );
    }

    XmlHandler.addTagValue( "joining", joining );
    XmlHandler.addTagValue( "max_log_lines", maxLogLines );
    XmlHandler.addTagValue( "max_log_timeout_minutes", maxLogTimeoutMinutes );
    XmlHandler.addTagValue( "object_timeout_minutes", objectTimeoutMinutes );

    xml.append( XmlHandler.openTag( XML_TAG_SEQUENCES ) );
    for ( SlaveSequence slaveSequence : slaveSequences ) {
      xml.append( XmlHandler.openTag( SlaveSequence.XML_TAG ) );
      xml.append( slaveSequence.getXml() );
      xml.append( XmlHandler.closeTag( SlaveSequence.XML_TAG ) );
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

  public SlaveServerConfig( ILogChannel log, Node node ) throws HopXmlException {
    this();
    Node slaveNode = XmlHandler.getSubNode( node, SlaveServer.XML_TAG );
    if ( slaveNode != null ) {
      slaveServer = new SlaveServer( slaveNode );
      checkNetworkInterfaceSetting( log, slaveNode, slaveServer );
    }

    Node mastersNode = XmlHandler.getSubNode( node, XML_TAG_MASTERS );
    int nrMasters = XmlHandler.countNodes( mastersNode, SlaveServer.XML_TAG );
    for ( int i = 0; i < nrMasters; i++ ) {
      Node masterSlaveNode = XmlHandler.getSubNodeByNr( mastersNode, SlaveServer.XML_TAG, i );
      SlaveServer masterSlaveServer = new SlaveServer( masterSlaveNode );
      checkNetworkInterfaceSetting( log, masterSlaveNode, masterSlaveServer );
      masterSlaveServer.setSslMode( slaveServer.isSslMode() );
      masters.add( masterSlaveServer );
    }

    reportingToMasters = "Y".equalsIgnoreCase( XmlHandler.getTagValue( node, "report_to_masters" ) );

    joining = "Y".equalsIgnoreCase( XmlHandler.getTagValue( node, "joining" ) );
    maxLogLines = Const.toInt( XmlHandler.getTagValue( node, "max_log_lines" ), 0 );
    maxLogTimeoutMinutes = Const.toInt( XmlHandler.getTagValue( node, "max_log_timeout_minutes" ), 0 );
    objectTimeoutMinutes = Const.toInt( XmlHandler.getTagValue( node, "object_timeout_minutes" ), 0 );

    // Read sequence information
    //

    // TODO : read databases back in
    //
    Node sequencesNode = XmlHandler.getSubNode( node, "sequences" );
    List<Node> seqNodes = XmlHandler.getNodes( sequencesNode, SlaveSequence.XML_TAG );
    for ( Node seqNode : seqNodes ) {
      slaveSequences.add( new SlaveSequence( seqNode, databases ) );
    }

    Node autoSequenceNode = XmlHandler.getSubNode( node, XML_TAG_AUTOSEQUENCE );
    if ( autoSequenceNode != null ) {
      autoSequence = new SlaveSequence( autoSequenceNode, databases );
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
        jettyOptions.put( Const.HOP_CARTE_JETTY_ACCEPTORS, XmlHandler.getTagValue( jettyOptionsNode, XML_TAG_ACCEPTORS ) );
      }
      if ( XmlHandler.getTagValue( jettyOptionsNode, XML_TAG_ACCEPT_QUEUE_SIZE ) != null ) {
        jettyOptions.put( Const.HOP_CARTE_JETTY_ACCEPT_QUEUE_SIZE, XmlHandler.getTagValue( jettyOptionsNode,
          XML_TAG_ACCEPT_QUEUE_SIZE ) );
      }
      if ( XmlHandler.getTagValue( jettyOptionsNode, XML_TAG_LOW_RES_MAX_IDLE_TIME ) != null ) {
        jettyOptions.put( Const.HOP_CARTE_JETTY_RES_MAX_IDLE_TIME, XmlHandler.getTagValue( jettyOptionsNode,
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
      database = new Database( loggingInterface, databaseMeta );
      database.connect();
      String schemaTable =
        databaseMeta.getQuotedSchemaTableCombination( autoSequence.getSchemaName(), autoSequence.getTableName() );
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
            SlaveSequence slaveSequence =
              new SlaveSequence( sequenceName, value, databaseMeta, autoSequence.getSchemaName(), autoSequence
                .getTableName(), autoSequence.getSequenceNameField(), autoSequence.getValueField() );

            slaveSequences.add( slaveSequence );

            LogChannel.GENERAL.logBasic( "Automatically created slave sequence '"
              + slaveSequence.getName() + "' with start value " + slaveSequence.getStartValue() );
          }
        }
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to automatically configure slave sequences", e );
    } finally {
      if ( database != null ) {
        database.disconnect();
      }
    }
  }

  private void checkNetworkInterfaceSetting( ILogChannel log, Node slaveNode, SlaveServer slaveServer ) {
    // See if we need to grab the network interface to use and then override the host name
    //
    String networkInterfaceName = XmlHandler.getTagValue( slaveNode, "network_interface" );
    if ( !Utils.isEmpty( networkInterfaceName ) ) {
      // OK, so let's try to get the IP address for this network interface...
      //
      try {
        String newHostname = Const.getIPAddress( networkInterfaceName );
        if ( newHostname != null ) {
          slaveServer.setHostname( newHostname );
          // Also change the name of the slave...
          //
          slaveServer.setName( slaveServer.getName() + "-" + newHostname );
          log.logBasic( "Hostname for slave server ["
            + slaveServer.getName() + "] is set to [" + newHostname + "], information derived from network "
            + networkInterfaceName );
        }
      } catch ( SocketException e ) {
        log.logError( "Unable to get the IP address for network interface "
          + networkInterfaceName + " for slave server [" + slaveServer.getName() + "]", e );
      }
    }

  }

  public SlaveServerConfig( String hostname, int port, boolean joining ) {
    this();
    this.joining = joining;
    this.slaveServer = new SlaveServer( hostname + ":" + port, hostname, "" + port, null, null );
  }

  /**
   * @return the list of masters to report back to if the report to masters flag is enabled.
   */
  public List<SlaveServer> getMasters() {
    return masters;
  }

  /**
   * @param masters the list of masters to set. It is the list of masters to report back to if the report to masters flag is
   *                enabled.
   */
  public void setMasters( List<SlaveServer> masters ) {
    this.masters = masters;
  }

  /**
   * @return the slave server.<br>
   * The user name and password defined in here are used to contact this slave by the masters.
   */
  public SlaveServer getSlaveServer() {
    return slaveServer;
  }

  /**
   * @param slaveServer the slave server details to set.<br>
   *                    The user name and password defined in here are used to contact this slave by the masters.
   */
  public void setSlaveServer( SlaveServer slaveServer ) {
    this.slaveServer = slaveServer;
  }

  /**
   * @return true if this slave reports to the masters
   */
  public boolean isReportingToMasters() {
    return reportingToMasters;
  }

  /**
   * @param reportingToMaster set to true if this slave should report to the masters
   */
  public void setReportingToMasters( boolean reportingToMaster ) {
    this.reportingToMasters = reportingToMaster;
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
   * @return the slaveSequences
   */
  public List<SlaveSequence> getSlaveSequences() {
    return slaveSequences;
  }

  /**
   * @param slaveSequences the slaveSequences to set
   */
  public void setSlaveSequences( List<SlaveSequence> slaveSequences ) {
    this.slaveSequences = slaveSequences;
  }

  /**
   * @return the autoSequence
   */
  public SlaveSequence getAutoSequence() {
    return autoSequence;
  }

  /**
   * @param autoSequence the autoSequence to set
   */
  public void setAutoSequence( SlaveSequence autoSequence ) {
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

  public DelegatingMetaStore getMetaStore() {
    return metaStore;
  }

  public void setMetaStore( DelegatingMetaStore metaStore ) {
    this.metaStore = metaStore;
  }

  public String getPasswordFile() {
    return passwordFile;
  }

  public void setPasswordFile( String passwordFile ) {
    this.passwordFile = passwordFile;
  }

}
