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

package org.apache.hop.workflow.actions.sendnagiospassivecheck;

import com.googlecode.jsendnsca.Level;
import com.googlecode.jsendnsca.MessagePayload;
import com.googlecode.jsendnsca.NagiosPassiveCheckSender;
import com.googlecode.jsendnsca.NagiosSettings;
import com.googlecode.jsendnsca.builders.MessagePayloadBuilder;
import com.googlecode.jsendnsca.builders.NagiosSettingsBuilder;
import com.googlecode.jsendnsca.encryption.Encryption;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.util.List;

/**
 * This defines an SendNagiosPassiveCheck action.
 *
 * @author Samatar
 * @since 01-10-2011
 */

@Action(
  id = "SEND_NAGIOS_PASSIVE_CHECK",
  name = "i18n::ActionSendNagiosPassiveCheck.Name",
  description = "i18n::ActionSendNagiosPassiveCheck.Description",
  image = "SendNagiosPassiveCheck.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/sendnagiospassivecheck.html"
)
public class ActionSendNagiosPassiveCheck extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionSendNagiosPassiveCheck.class; // For Translator

  private String serverName;
  private String port;
  private String responseTimeOut;
  private String connectionTimeOut;

  private String message;
  private String senderServerName;
  private String senderServiceName;
  private int encryptionMode;
  private int level;

  private String password;

  /**
   * Default responseTimeOut to 1000 milliseconds
   */
  private static int DEFAULT_RESPONSE_TIME_OUT = 10000; // ms

  /**
   * Default connection responseTimeOut to 5000 milliseconds
   */
  public static int DEFAULT_CONNECTION_TIME_OUT = 5000; // ms

  /**
   * Default port
   */
  public static int DEFAULT_PORT = 5667;

  public static final String[] encryptionModeDesc = new String[] {
    BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.EncryptionMode.None" ),
    BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.EncryptionMode.TripleDES" ),
    BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.EncryptionMode.XOR" ) };
  public static final String[] encryptionModeCode = new String[] { "none", "tripledes", "xor" };

  public static final int ENCRYPTION_MODE_NONE = 0;
  public static final int ENCRYPTION_MODE_TRIPLEDES = 1;
  public static final int ENCRYPTION_MODE_XOR = 2;

  public static final String[] levelTypeDesc = new String[] {
    BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.LevelType.Unknown" ),
    BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.EncryptionMode.OK" ),
    BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.EncryptionMode.Warning" ),
    BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.EncryptionMode.Critical" ) };
  public static final String[] levelTypeCode = new String[] { "unknown", "ok", "warning", "critical" };

  public static final int LEVEL_TYPE_UNKNOWN = 0;
  public static final int LEVEL_TYPE_OK = 1;
  public static final int LEVEL_TYPE_WARNING = 2;
  public static final int LEVEL_TYPE_CRITICAL = 3;

  public ActionSendNagiosPassiveCheck(String n ) {
    super( n, "" );
    port = "" + DEFAULT_PORT;
    serverName = null;
    connectionTimeOut = String.valueOf( DEFAULT_CONNECTION_TIME_OUT );
    responseTimeOut = String.valueOf( DEFAULT_RESPONSE_TIME_OUT );
    message = null;
    senderServerName = null;
    senderServiceName = null;
    encryptionMode = ENCRYPTION_MODE_NONE;
    level = LEVEL_TYPE_UNKNOWN;
    password = null;
  }

  public ActionSendNagiosPassiveCheck() {
    this( "" );
  }

  public Object clone() {
    ActionSendNagiosPassiveCheck je = (ActionSendNagiosPassiveCheck) super.clone();
    return je;
  }

  public static int getEncryptionModeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < encryptionModeDesc.length; i++ ) {
      if ( encryptionModeDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getEncryptionModeByCode( tt );
  }

  public static String getEncryptionModeDesc( int i ) {
    if ( i < 0 || i >= encryptionModeDesc.length ) {
      return encryptionModeDesc[ 0 ];
    }
    return encryptionModeDesc[ i ];
  }

  public static String getLevelDesc( int i ) {
    if ( i < 0 || i >= levelTypeDesc.length ) {
      return levelTypeDesc[ 0 ];
    }
    return levelTypeDesc[ i ];
  }

  public static int getLevelByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < levelTypeDesc.length; i++ ) {
      if ( levelTypeDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getEncryptionModeByCode( tt );
  }

  private static String getEncryptionModeCode( int i ) {
    if ( i < 0 || i >= encryptionModeCode.length ) {
      return encryptionModeCode[ 0 ];
    }
    return encryptionModeCode[ i ];
  }

  private String getLevelCode( int i ) {
    if ( i < 0 || i >= levelTypeCode.length ) {
      return levelTypeCode[ 0 ];
    }
    return levelTypeCode[ i ];
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "port", port ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "servername", serverName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "password", password ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "responseTimeOut", responseTimeOut ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "connectionTimeOut", connectionTimeOut ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "senderServerName", senderServerName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "senderServiceName", senderServiceName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "message", message ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "encryptionMode", getEncryptionModeCode( encryptionMode ) ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "level", getLevelCode( level ) ) );

    return retval.toString();
  }

  private static int getEncryptionModeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < encryptionModeCode.length; i++ ) {
      if ( encryptionModeCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  private static int getLevelByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < levelTypeCode.length; i++ ) {
      if ( levelTypeCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      port = XmlHandler.getTagValue( entrynode, "port" );
      serverName = XmlHandler.getTagValue( entrynode, "servername" );
      responseTimeOut = XmlHandler.getTagValue( entrynode, "responseTimeOut" );
      connectionTimeOut = XmlHandler.getTagValue( entrynode, "connectionTimeOut" );
      password = XmlHandler.getTagValue( entrynode, "password" );

      senderServerName = XmlHandler.getTagValue( entrynode, "senderServerName" );
      senderServiceName = XmlHandler.getTagValue( entrynode, "senderServiceName" );
      message = XmlHandler.getTagValue( entrynode, "message" );

      encryptionMode = getEncryptionModeByCode( XmlHandler.getTagValue( entrynode, "encryptionMode" ) );
      level = getLevelByCode( XmlHandler.getTagValue( entrynode, "level" ) );

    } catch ( HopXmlException xe ) {
      throw new HopXmlException( "Unable to load action of type 'SendNagiosPassiveCheck' from XML node", xe );
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
   * @return Returns the senderServerName.
   */
  public String getSenderServerName() {
    return senderServerName;
  }

  /**
   * @param senderServerName The senderServerName to set.
   */
  public void setSenderServerName( String senderServerName ) {
    this.senderServerName = senderServerName;
  }

  /**
   * @return Returns the senderServiceName.
   */
  public String getSenderServiceName() {
    return senderServiceName;
  }

  /**
   * @param senderServiceName The senderServiceName to set.
   */
  public void setSenderServiceName( String senderServiceName ) {
    this.senderServiceName = senderServiceName;
  }

  /**
   * @param password The password to set.
   */
  public void setPassword( String password ) {
    this.password = password;
  }

  /**
   * @return Returns the password.
   */
  public String getPassword() {
    return password;
  }

  public int getEncryptionMode() {
    return encryptionMode;
  }

  public void setEncryptionMode( int encryptionModein ) {
    this.encryptionMode = encryptionModein;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel( int levelMode ) {
    this.level = levelMode;
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
   * @param responseTimeOut The responseTimeOut to set.
   */
  public void setResponseTimeOut( String responseTimeOut ) {
    this.responseTimeOut = responseTimeOut;
  }

  /**
   * @return Returns the responseTimeOut.
   */
  public String getResponseTimeOut() {
    return responseTimeOut;
  }

  /**
   * @param connectionTimeOut The connectionTimeOut to set.
   */
  public void setConnectionTimeOut( String connectionTimeOut ) {
    this.connectionTimeOut = connectionTimeOut;
  }

  /**
   * @return Returns the connectionTimeOut.
   */
  public String getConnectionTimeOut() {
    return connectionTimeOut;
  }

  public Result execute( Result previousResult, int nr ) {
    log.logBasic( BaseMessages.getString( PKG, "ActionSendNagiosPassiveCheck.Started", serverName ) );

    Result result = previousResult;
    result.setNrErrors( 1 );
    result.setResult( false );

    // Target
    String realServername = resolve( serverName );
    String realPassword = Utils.resolvePassword( getVariables(), password );
    int realPort = Const.toInt( resolve( port ), DEFAULT_PORT );
    int realResponseTimeOut = Const.toInt( resolve( responseTimeOut ), DEFAULT_RESPONSE_TIME_OUT );
    int realConnectionTimeOut =
      Const.toInt( resolve( connectionTimeOut ), DEFAULT_CONNECTION_TIME_OUT );

    // Sender
    String realSenderServerName = resolve( senderServerName );
    String realSenderServiceName = resolve( senderServiceName );

    try {
      if ( Utils.isEmpty( realServername ) ) {
        throw new HopException( BaseMessages.getString(
          PKG, "JobSendNagiosPassiveCheck.Error.TargetServerMissing" ) );
      }

      String realMessageString = resolve( message );

      if ( Utils.isEmpty( realMessageString ) ) {
        throw new HopException( BaseMessages.getString( PKG, "JobSendNagiosPassiveCheck.Error.MessageMissing" ) );
      }

      Level level = Level.UNKNOWN;
      switch ( getLevel() ) {
        case LEVEL_TYPE_OK:
          level = Level.OK;
          break;
        case LEVEL_TYPE_CRITICAL:
          level = Level.CRITICAL;
          break;
        case LEVEL_TYPE_WARNING:
          level = Level.WARNING;
          break;
        default:
          break;
      }
      Encryption encr = Encryption.NONE;
      switch ( getEncryptionMode() ) {
        case ENCRYPTION_MODE_TRIPLEDES:
          encr = Encryption.TRIPLE_DES;
          break;
        case ENCRYPTION_MODE_XOR:
          encr = Encryption.XOR;
          break;
        default:
          break;
      }

      // settings
      NagiosSettingsBuilder ns = new NagiosSettingsBuilder();
      ns.withNagiosHost( realServername );
      ns.withPort( realPort );
      ns.withConnectionTimeout( realConnectionTimeOut );
      ns.withResponseTimeout( realResponseTimeOut );
      ns.withEncryption( encr );
      if ( !Utils.isEmpty( realPassword ) ) {
        ns.withPassword( realPassword );
      } else {
        ns.withNoPassword();
      }

      // target nagios host
      NagiosSettings settings = ns.create();

      // sender
      MessagePayloadBuilder pb = new MessagePayloadBuilder();
      if ( !Utils.isEmpty( realSenderServerName ) ) {
        pb.withHostname( realSenderServerName );
      }
      pb.withLevel( level );
      if ( !Utils.isEmpty( realSenderServiceName ) ) {
        pb.withServiceName( realSenderServiceName );
      }
      pb.withMessage( realMessageString );
      MessagePayload payload = pb.create();

      NagiosPassiveCheckSender sender = new NagiosPassiveCheckSender( settings );

      sender.send( payload );

      result.setNrErrors( 0 );
      result.setResult( true );

    } catch ( Exception e ) {
      log.logError( BaseMessages.getString( PKG, "ActionSendNagiosPassiveCheck.ErrorGetting", e.toString() ) );
    }

    return result;
  }

  @Override public boolean isEvaluation() {
    return true;
  }

  public List<ResourceReference> getResourceDependencies( IVariables variables, WorkflowMeta workflowMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( variables, workflowMeta );
    if ( !Utils.isEmpty( serverName ) ) {
      String realServername = resolve( serverName );
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( realServername, ResourceType.SERVER ) );
      references.add( reference );
    }
    return references;
  }

  @Override
  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    ActionValidatorUtils.andValidator().validate( this, "serverName", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
  }

}
