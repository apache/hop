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

package org.apache.hop.workflow.actions.snmptrap;

import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.snmp4j.CommunityTarget;
import org.snmp4j.PDU;
import org.snmp4j.PDUv1;
import org.snmp4j.ScopedPDU;
import org.snmp4j.Snmp;
import org.snmp4j.TransportMapping;
import org.snmp4j.UserTarget;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.MPv3;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.AuthMD5;
import org.snmp4j.security.PrivDES;
import org.snmp4j.security.SecurityLevel;
import org.snmp4j.security.SecurityProtocols;
import org.snmp4j.security.USM;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;
import org.w3c.dom.Node;

import java.net.InetAddress;

/**
 * This defines an SNMPTrap action.
 *
 * @author Matt
 * @since 05-11-2003
 */

@Action(
  id = "SNMP_TRAP",
  name = "i18n::ActionSNMPTrap.Name",
  description = "i18n::ActionSNMPTrap.Description",
  image = "SNMP.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/snmptrap.html"
)
public class ActionSNMPTrap extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionSNMPTrap.class; // For Translator

  private String serverName;
  private String port;
  private String timeout;
  private String nrretry;
  private String comString;
  private String message;
  private String oid;
  private String targettype;
  private String user;
  private String passphrase;
  private String engineid;

  /**
   * Default retries
   */
  private static int DEFAULT_RETRIES = 1;

  /**
   * Default timeout to 500 milliseconds
   */
  private static int DEFAULT_TIME_OUT = 5000;

  /**
   * Default port
   */
  public static int DEFAULT_PORT = 162;

  public static final String[] targetTypeDesc = new String[] {
    BaseMessages.getString( PKG, "JobSNMPTrap.TargetType.Community" ),
    BaseMessages.getString( PKG, "JobSNMPTrap.TargetType.User" ) };
  public static final String[] targetTypeCode = new String[] { "community", "user" };

  public ActionSNMPTrap( String n ) {
    super( n, "" );
    port = "" + DEFAULT_PORT;
    serverName = null;
    comString = "public";
    nrretry = "" + DEFAULT_RETRIES;
    timeout = "" + DEFAULT_TIME_OUT;
    message = null;
    oid = null;
    targettype = targetTypeCode[ 0 ];
    user = null;
    passphrase = null;
    engineid = null;
  }

  public ActionSNMPTrap() {
    this( "" );
  }

  public Object clone() {
    ActionSNMPTrap je = (ActionSNMPTrap) super.clone();
    return je;
  }

  public String getTargetTypeDesc( String tt ) {
    if ( Utils.isEmpty( tt ) ) {
      return targetTypeDesc[ 0 ];
    }
    if ( tt.equalsIgnoreCase( targetTypeCode[ 1 ] ) ) {
      return targetTypeDesc[ 1 ];
    } else {
      return targetTypeDesc[ 0 ];
    }
  }

  public String getTargetTypeCode( String tt ) {
    if ( tt == null ) {
      return targetTypeCode[ 0 ];
    }
    if ( tt.equals( targetTypeDesc[ 1 ] ) ) {
      return targetTypeCode[ 1 ];
    } else {
      return targetTypeCode[ 0 ];
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "port", port ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "servername", serverName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "oid", oid ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "comstring", comString ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "message", message ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "timeout", timeout ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "nrretry", nrretry ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "targettype", targettype ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "user", user ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "passphrase", Encr.encryptPasswordIfNotUsingVariables( passphrase ) ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "engineid", engineid ) );
    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      port = XmlHandler.getTagValue( entrynode, "port" );
      serverName = XmlHandler.getTagValue( entrynode, "servername" );
      oid = XmlHandler.getTagValue( entrynode, "oid" );
      message = XmlHandler.getTagValue( entrynode, "message" );
      comString = XmlHandler.getTagValue( entrynode, "comstring" );
      timeout = XmlHandler.getTagValue( entrynode, "timeout" );
      nrretry = XmlHandler.getTagValue( entrynode, "nrretry" );
      targettype = XmlHandler.getTagValue( entrynode, "targettype" );
      user = XmlHandler.getTagValue( entrynode, "user" );
      passphrase = Encr.decryptPasswordOptionallyEncrypted( XmlHandler.getTagValue( entrynode, "passphrase" ) );
      engineid = XmlHandler.getTagValue( entrynode, "engineid" );

    } catch ( HopXmlException xe ) {
      throw new HopXmlException( "Unable to load action of type 'SNMPTrap' from XML node", xe );
    }
  }


  /**
   * @return Returns the serverName.
   */
  public String getServerName() {
    return serverName;
  }

  /**
   * @param serverName The serverName to set.
   */
  public void setServerName( String serverName ) {
    this.serverName = serverName;
  }

  /**
   * @return Returns the OID.
   */
  public String getOID() {
    return oid;
  }

  /**
   * @param oid The oid to set.
   */
  public void setOID( String oid ) {
    this.oid = oid;
  }

  /**
   * @return Returns the comString.
   */
  public String getComString() {
    return comString;
  }

  /**
   * @param comString The comString to set.
   */
  public void setComString( String comString ) {
    this.comString = comString;
  }

  /**
   * @param user The user to set.
   */
  public void setUser( String user ) {
    this.user = user;
  }

  /**
   * @return Returns the user.
   */
  public String getUser() {
    return user;
  }

  /**
   * @param user The passphrase to set.
   */
  public void setPassPhrase( String passphrase ) {
    this.passphrase = passphrase;
  }

  /**
   * @return Returns the passphrase.
   */
  public String getPassPhrase() {
    return passphrase;
  }

  /**
   * @param user The engineid to set.
   */
  public void setEngineID( String engineid ) {
    this.engineid = engineid;
  }

  /**
   * @return Returns the engineid.
   */
  public String getEngineID() {
    return engineid;
  }

  public String getTargetType() {
    return targettype;
  }

  public void setTargetType( String targettypein ) {
    this.targettype = getTargetTypeCode( targettypein );
  }

  /**
   * @param message The message to set.
   */
  public void setMessage( String message ) {
    this.message = message;
  }

  /**
   * @return Returns the comString.
   */
  public String getMessage() {
    return message;
  }

  /**
   * @return Returns the port.
   */
  public String getPort() {
    return port;
  }

  /**
   * @param port The port to set.
   */
  public void setPort( String port ) {
    this.port = port;
  }

  /**
   * @param timeout The timeout to set.
   */
  public void setTimeout( String timeout ) {
    this.timeout = timeout;
  }

  /**
   * @return Returns the timeout.
   */
  public String getTimeout() {
    return timeout;
  }

  /**
   * @param nrretry The nrretry to set.
   */
  public void setRetry( String nrretry ) {
    this.nrretry = nrretry;
  }

  /**
   * @return Returns the nrretry.
   */
  public String getRetry() {
    return nrretry;
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setNrErrors( 1 );
    result.setResult( false );

    String servername = resolve( serverName );
    int nrPort = Const.toInt( resolve( "" + port ), DEFAULT_PORT );
    String Oid = resolve( oid );
    int timeOut = Const.toInt( resolve( "" + timeout ), DEFAULT_TIME_OUT );
    int retry = Const.toInt( resolve( "" + nrretry ), 1 );
    String messageString = resolve( message );

    Snmp snmp = null;

    try {
      TransportMapping transMap = new DefaultUdpTransportMapping();
      snmp = new Snmp( transMap );

      UdpAddress udpAddress = new UdpAddress( InetAddress.getByName( servername ), nrPort );
      ResponseEvent response = null;
      if ( targettype.equals( targetTypeCode[ 0 ] ) ) {
        // Community target
        String community = resolve( comString );

        CommunityTarget target = new CommunityTarget();
        PDUv1 pdu1 = new PDUv1();
        transMap.listen();

        target.setCommunity( new OctetString( community ) );
        target.setVersion( SnmpConstants.version1 );
        target.setAddress( udpAddress );
        if ( target.getAddress().isValid() ) {
          if ( log.isDebug() ) {
            logDebug( "Valid IP address" );
          }
        } else {
          throw new HopException( "Invalid IP address" );
        }
        target.setRetries( retry );
        target.setTimeout( timeOut );

        // create the PDU
        pdu1.setGenericTrap( 6 );
        pdu1.setSpecificTrap( PDUv1.ENTERPRISE_SPECIFIC );
        pdu1.setEnterprise( new OID( Oid ) );
        pdu1.add( new VariableBinding( new OID( Oid ), new OctetString( messageString ) ) );

        response = snmp.send( pdu1, target );

      } else {
        // User target
        String userName = resolve( user );
        String passPhrase = resolve( passphrase );
        String engineID = resolve( engineid );

        UserTarget usertarget = new UserTarget();
        transMap.listen();
        usertarget.setAddress( udpAddress );
        if ( usertarget.getAddress().isValid() ) {
          if ( log.isDebug() ) {
            logDebug( "Valid IP address" );
          }
        } else {
          throw new HopException( "Invalid IP address" );
        }

        usertarget.setRetries( retry );
        usertarget.setTimeout( timeOut );
        usertarget.setVersion( SnmpConstants.version3 );
        usertarget.setSecurityLevel( SecurityLevel.AUTH_PRIV );
        usertarget.setSecurityName( new OctetString( "MD5DES" ) );

        // Since we are using SNMPv3 we use authenticated users
        // this is handled by the UsmUser and USM class

        UsmUser uu =
          new UsmUser(
            new OctetString( userName ), AuthMD5.ID, new OctetString( passPhrase ), PrivDES.ID,
            new OctetString( passPhrase ) );

        USM usm = snmp.getUSM();

        if ( usm == null ) {
          throw new HopException( "Null Usm" );
        } else {
          usm = new USM( SecurityProtocols.getInstance(), new OctetString( MPv3.createLocalEngineID() ), 0 );
          usm.addUser( new OctetString( userName ), uu );
          if ( log.isDebug() ) {
            logDebug( "Valid Usm" );
          }
        }

        // create the PDU
        ScopedPDU pdu = new ScopedPDU();
        pdu.add( new VariableBinding( new OID( Oid ), new OctetString( messageString ) ) );
        pdu.setType( PDU.TRAP );
        if ( !Utils.isEmpty( engineID ) ) {
          pdu.setContextEngineID( new OctetString( engineID ) );
        }

        // send the PDU
        response = snmp.send( pdu, usertarget );
      }

      if ( response != null ) {
        if ( log.isDebug() ) {
          logDebug( "Received response from: " + response.getPeerAddress() + response.toString() );
        }
      }

      result.setNrErrors( 0 );
      result.setResult( true );
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "ActionSNMPTrap.ErrorGetting", e.getMessage() ) );
    } finally {
      try {
        if ( snmp != null ) {
          snmp.close();
        }
      } catch ( Exception e ) { /* Ignore */
      }
    }

    return result;
  }

  @Override public boolean isEvaluation() {
    return true;
  }

}
