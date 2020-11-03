/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.workflow.actions.ftpsput;

import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.actions.ftpsget.FtpsConnection;
import org.w3c.dom.Node;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines an FTPS put action.
 *
 * @author Samatar
 * @since 15-03-2010
 */

@Action(
  id = "FTPS_PUT",
  i18nPackageName = "org.apache.hop.workflow.actions.ftpsput",
  name = "ActionFTPSPut.Name",
  description = "ActionFTPSPut.Description",
  image = "FTPSPut.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileTransfer",
  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/ftpsput.html"
)
public class ActionFtpsPut extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionFtpsPut.class; // for i18n purposes, needed by Translator!!

  private String serverName;
  private String serverPort;
  private String userName;
  private String password;
  private String remoteDirectory;
  private String localDirectory;
  private String wildcard;
  private boolean binaryMode;
  private int timeout;
  private boolean remove;
  private boolean onlyPuttingNewFiles; /* Don't overwrite files */
  private boolean activeConnection;
  private String proxyHost;

  private String proxyPort; /* string to allow variable substitution */

  private String proxyUsername;

  private String proxyPassword;

  private int connectionType;

  public ActionFtpsPut( String n ) {
    super( n, "" );
    serverName = null;
    serverPort = "21";
    remoteDirectory = null;
    localDirectory = null;
    connectionType = FtpsConnection.CONNECTION_TYPE_FTP;
  }

  public ActionFtpsPut() {
    this( "" );
  }

  public Object clone() {
    ActionFtpsPut je = (ActionFtpsPut) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 400 );

    retval.append( super.getXml() );

    retval.append( "      " ).append( XmlHandler.addTagValue( "servername", serverName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "serverport", serverPort ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "username", userName ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "password", Encr.encryptPasswordIfNotUsingVariables( getPassword() ) ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "remoteDirectory", remoteDirectory ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "localDirectory", localDirectory ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "wildcard", wildcard ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "binary", binaryMode ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "timeout", timeout ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "remove", remove ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "only_new", onlyPuttingNewFiles ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "active", activeConnection ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "proxy_host", proxyHost ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "proxy_port", proxyPort ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "proxy_username", proxyUsername ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "proxy_password", proxyPassword ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "connection_type", FtpsConnection.getConnectionTypeCode( connectionType ) ) );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      serverName = XmlHandler.getTagValue( entrynode, "servername" );
      serverPort = XmlHandler.getTagValue( entrynode, "serverport" );
      userName = XmlHandler.getTagValue( entrynode, "username" );
      password = Encr.decryptPasswordOptionallyEncrypted( XmlHandler.getTagValue( entrynode, "password" ) );
      remoteDirectory = XmlHandler.getTagValue( entrynode, "remoteDirectory" );
      localDirectory = XmlHandler.getTagValue( entrynode, "localDirectory" );
      wildcard = XmlHandler.getTagValue( entrynode, "wildcard" );
      binaryMode = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "binary" ) );
      timeout = Const.toInt( XmlHandler.getTagValue( entrynode, "timeout" ), 10000 );
      remove = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "remove" ) );
      onlyPuttingNewFiles = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "only_new" ) );
      activeConnection = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "active" ) );

      proxyHost = XmlHandler.getTagValue( entrynode, "proxy_host" );
      proxyPort = XmlHandler.getTagValue( entrynode, "proxy_port" );
      proxyUsername = XmlHandler.getTagValue( entrynode, "proxy_username" );
      proxyPassword = XmlHandler.getTagValue( entrynode, "proxy_password" );
      connectionType =
        FtpsConnection.getConnectionTypeByCode( Const.NVL(
          XmlHandler.getTagValue( entrynode, "connection_type" ), "" ) );
    } catch ( HopXmlException xe ) {
      throw new HopXmlException( BaseMessages.getString( PKG, "JobFTPSPUT.Log.UnableToLoadFromXml" ), xe );
    }
  }

  /**
   * @return Returns the binaryMode.
   */
  public boolean isBinaryMode() {
    return binaryMode;
  }

  /**
   * @param binaryMode The binaryMode to set.
   */
  public void setBinaryMode( boolean binaryMode ) {
    this.binaryMode = binaryMode;
  }

  /**
   * @param timeout The timeout to set.
   */
  public void setTimeout( int timeout ) {
    this.timeout = timeout;
  }

  /**
   * @return Returns the timeout.
   */
  public int getTimeout() {
    return timeout;
  }

  /**
   * @return Returns the onlyGettingNewFiles.
   */
  public boolean isOnlyPuttingNewFiles() {
    return onlyPuttingNewFiles;
  }

  /**
   * @param onlyPuttingNewFiles Only transfer new files to the remote host
   */
  public void setOnlyPuttingNewFiles( boolean onlyPuttingNewFiles ) {
    this.onlyPuttingNewFiles = onlyPuttingNewFiles;
  }

  /**
   * @return Returns the remoteDirectory.
   */
  public String getRemoteDirectory() {
    return remoteDirectory;
  }

  /**
   * @param directory The remoteDirectory to set.
   */
  public void setRemoteDirectory( String directory ) {
    this.remoteDirectory = directory;
  }

  /**
   * @return Returns the password.
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password The password to set.
   */
  public void setPassword( String password ) {
    this.password = password;
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
   * @return Returns the userName.
   */
  public String getUserName() {
    return userName;
  }

  /**
   * @param userName The userName to set.
   */
  public void setUserName( String userName ) {
    this.userName = userName;
  }

  /**
   * @return Returns the wildcard.
   */
  public String getWildcard() {
    return wildcard;
  }

  /**
   * @param wildcard The wildcard to set.
   */
  public void setWildcard( String wildcard ) {
    this.wildcard = wildcard;
  }

  /**
   * @return Returns the localDirectory.
   */
  public String getLocalDirectory() {
    return localDirectory;
  }

  /**
   * @param directory The localDirectory to set.
   */
  public void setLocalDirectory( String directory ) {
    this.localDirectory = directory;
  }

  /**
   * @param remove The remove to set.
   */
  public void setRemove( boolean remove ) {
    this.remove = remove;
  }

  /**
   * @return Returns the remove.
   */
  public boolean getRemove() {
    return remove;
  }

  public String getServerPort() {
    return serverPort;
  }

  public void setServerPort( String serverPort ) {
    this.serverPort = serverPort;
  }

  /**
   * @return the activeConnection
   */
  public boolean isActiveConnection() {
    return activeConnection;
  }

  /**
   * @param activeConnection set to true to get an active FTP connection
   */
  public void setActiveConnection( boolean activeConnection ) {
    this.activeConnection = activeConnection;
  }

  /**
   * @return Returns the hostname of the ftp-proxy.
   */
  public String getProxyHost() {
    return proxyHost;
  }

  /**
   * @param proxyHost The hostname of the proxy.
   */
  public void setProxyHost( String proxyHost ) {
    this.proxyHost = proxyHost;
  }

  /**
   * @return Returns the password which is used to authenticate at the proxy.
   */
  public String getProxyPassword() {
    return proxyPassword;
  }

  /**
   * @param proxyPassword The password which is used to authenticate at the proxy.
   */
  public void setProxyPassword( String proxyPassword ) {
    this.proxyPassword = proxyPassword;
  }

  /**
   * @return Returns the port of the ftp-proxy.
   */
  public String getProxyPort() {
    return proxyPort;
  }

  /**
   * @return the conenction type
   */
  public int getConnectionType() {
    return connectionType;
  }

  /**
   * @param type the connectionType to set
   */
  public void setConnectionType( int type ) {
    connectionType = type;
  }

  /**
   * @param proxyPort The port of the ftp-proxy.
   */
  public void setProxyPort( String proxyPort ) {
    this.proxyPort = proxyPort;
  }

  /**
   * @return Returns the username which is used to authenticate at the proxy.
   */
  public String getProxyUsername() {
    return proxyUsername;
  }

  /**
   * @param proxyUsername The username which is used to authenticate at the proxy.
   */
  public void setProxyUsername( String proxyUsername ) {
    this.proxyUsername = proxyUsername;
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );
    long filesput = 0;

    if ( isDetailed() ) {
      logDetailed( BaseMessages.getString( PKG, "JobFTPSPUT.Log.Starting" ) );
    }

    // String substitution..
    String realServerName = environmentSubstitute( serverName );
    String realServerPort = environmentSubstitute( serverPort );
    String realUsername = environmentSubstitute( userName );
    String realPassword = Encr.decryptPasswordOptionallyEncrypted( environmentSubstitute( password ) );
    String realRemoteDirectory = environmentSubstitute( remoteDirectory );
    String realWildcard = environmentSubstitute( wildcard );
    String realLocalDirectory = environmentSubstitute( localDirectory );

    FtpsConnection connection = null;

    try {
      // Create FTPS client to host:port ...
      int realPort = Const.toInt( environmentSubstitute( realServerPort ), 0 );
      // Define a new connection
      connection = new FtpsConnection( getConnectionType(), realServerName, realPort, realUsername, realPassword );

      this.buildFtpsConnection( connection );

      // move to spool dir ...
      if ( !Utils.isEmpty( realRemoteDirectory ) ) {
        connection.changeDirectory( realRemoteDirectory );
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobFTPSPUT.Log.ChangedDirectory", realRemoteDirectory ) );
        }
      }

      realRemoteDirectory = Const.NVL( realRemoteDirectory, FtpsConnection.HOME_FOLDER );

      ArrayList<String> myFileList = new ArrayList<>();
      File localFiles = new File( realLocalDirectory );

      if ( !localFiles.exists() ) {
        // if local directory uses ${ signature this will be fail to MessageFormat.format ...
        String error = BaseMessages.getString(
          PKG, "JobFTPSPUT.LocalFileDirectoryNotExists" ) + realLocalDirectory;
        throw new Exception( error );
      }

      File[] children = localFiles.listFiles();
      for ( int i = 0; i < children.length; i++ ) {
        // Get filename of file or directory
        if ( !children[ i ].isDirectory() ) {
          myFileList.add( children[ i ].getName() );
        }
      }

      String[] filelist = new String[ myFileList.size() ];
      myFileList.toArray( filelist );

      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString(
          PKG, "JobFTPSPUT.Log.FoundFileLocalDirectory", filelist.length, realLocalDirectory ) );
      }

      Pattern pattern = null;
      if ( !Utils.isEmpty( realWildcard ) ) {
        pattern = Pattern.compile( realWildcard );

      } // end if

      // Get the files in the list and execute put each file in the FTP
      for ( int i = 0; i < filelist.length && !parentWorkflow.isStopped(); i++ ) {
        boolean getIt = true;

        // First see if the file matches the regular expression!
        if ( pattern != null ) {
          Matcher matcher = pattern.matcher( filelist[ i ] );
          getIt = matcher.matches();
        }

        if ( getIt ) {
          // File exists?
          boolean fileExist = connection.isFileExists( filelist[ i ] );

          if ( isDebug() ) {
            if ( fileExist ) {
              logDebug( BaseMessages.getString( PKG, "JobFTPSPUT.Log.FileExists", filelist[ i ] ) );
            } else {
              logDebug( BaseMessages.getString( PKG, "JobFTPSPUT.Log.FileDoesNotExists", filelist[ i ] ) );
            }
          }

          if ( !fileExist || ( !onlyPuttingNewFiles && fileExist ) ) {

            String localFilename = realLocalDirectory + Const.FILE_SEPARATOR + filelist[ i ];
            if ( isDebug() ) {
              logDebug( BaseMessages.getString(
                PKG, "JobFTPSPUT.Log.PuttingFileToRemoteDirectory", localFilename, realRemoteDirectory ) );
            }

            connection.uploadFile( localFilename, filelist[ i ] );

            filesput++;

            // Delete the file if this is needed!
            if ( remove ) {
              new File( localFilename ).delete();
              if ( isDetailed() ) {
                logDetailed( BaseMessages.getString( PKG, "JobFTPSPUT.Log.DeletedFile", localFilename ) );
              }
            }
          }
        }
      }

      result.setResult( true );
      result.setNrLinesOutput( filesput );
      if ( isDetailed() ) {
        logDebug( BaseMessages.getString( PKG, "JobFTPSPUT.Log.WeHavePut", filesput ) );
      }
    } catch ( Exception e ) {
      result.setNrErrors( 1 );
      logError( BaseMessages.getString( PKG, "JobFTPSPUT.Log.ErrorPuttingFiles", e.getMessage() ) );
      logError( Const.getStackTracker( e ) );
    } finally {
      if ( connection != null ) {
        try {
          connection.disconnect();
        } catch ( Exception e ) {
          logError( BaseMessages.getString( PKG, "JobFTPSPUT.Log.ErrorQuitingFTP", e.getMessage() ) );
        }
      }
    }

    return result;
  }

  public boolean evaluates() {
    return true;
  }

  public List<ResourceReference> getResourceDependencies( WorkflowMeta workflowMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( workflowMeta );
    if ( !Utils.isEmpty( serverName ) ) {
      String realServerName = workflowMeta.environmentSubstitute( serverName );
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( realServerName, ResourceType.SERVER ) );
      references.add( reference );
    }
    return references;
  }

  @Override
  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    ActionValidatorUtils.andValidator().validate( this, "serverName", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
    ActionValidatorUtils.andValidator().validate(
      this, "localDirectory", remarks, AndValidator.putValidators(
        ActionValidatorUtils.notBlankValidator(), ActionValidatorUtils.fileExistsValidator() ) );
    ActionValidatorUtils.andValidator().validate( this, "userName", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
    ActionValidatorUtils.andValidator().validate( this, "password", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notNullValidator() ) );
    ActionValidatorUtils.andValidator().validate( this, "serverPort", remarks,
      AndValidator.putValidators( ActionValidatorUtils.integerValidator() ) );
  }

  void buildFtpsConnection(FtpsConnection connection ) throws Exception {
    if ( !Utils.isEmpty( proxyHost ) ) {
      String realProxyHost = environmentSubstitute( proxyHost );
      String realProxyUsername = environmentSubstitute( proxyUsername );
      String realProxyPassword = environmentSubstitute( proxyPassword );
      realProxyPassword = Encr.decryptPasswordOptionallyEncrypted( realProxyPassword );

      connection.setProxyHost( realProxyHost );
      if ( !Utils.isEmpty( realProxyUsername ) ) {
        connection.setProxyUser( realProxyUsername );
      }
      if ( !Utils.isEmpty( realProxyPassword ) ) {
        connection.setProxyPassword( realProxyPassword );
      }
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "ActionFTPSPUT.OpenedProxyConnectionOn", realProxyHost ) );
      }

      int proxyport = Const.toInt( environmentSubstitute( proxyPort ), 21 );
      if ( proxyport != 0 ) {
        connection.setProxyPort( proxyport );
      }
    } else {
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "ActionFTPSPUT.OpenedConnectionTo", connection.getHostName() ) );
      }
    }

    // set activeConnection connectmode ...
    if ( activeConnection ) {
      connection.setPassiveMode( false );
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobFTPSPUT.Log.SetActiveConnection" ) );
      }
    } else {
      connection.setPassiveMode( true );
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobFTPSPUT.Log.SetPassiveConnection" ) );
      }
    }

    // Set the timeout
    connection.setTimeOut( timeout );
    if ( isDetailed() ) {
      logDetailed( BaseMessages.getString( PKG, "JobFTPSPUT.Log.SetTimeout", timeout ) );
    }

    // login to FTPS host ...
    connection.connect();
    if ( isDetailed() ) {
      // Remove password from logging, you don't know where it ends up.
      logDetailed( BaseMessages.getString( PKG, "JobFTPSPUT.Log.Logged", connection.getUserName() ) );
      logDetailed( BaseMessages.getString( PKG, "JobFTPSPUT.WorkingDirectory", connection.getWorkingDirectory() ) );
    }

    // Set binary mode
    if ( isBinaryMode() ) {
      connection.setBinaryMode( true );
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobFTPSPUT.Log.BinaryMod" ) );
      }
    }
  }

}
