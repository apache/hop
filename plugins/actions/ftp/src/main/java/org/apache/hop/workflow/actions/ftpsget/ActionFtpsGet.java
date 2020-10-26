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

package org.apache.hop.workflow.actions.ftpsget;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
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
import org.ftp4che.util.ftpfile.FTPFile;
import org.w3c.dom.Node;

/**
 * This defines an FTPS action.
 *
 * @author Samatar
 * @since 08-03-2010
 */

@Action(
  id = "FTPS_GET",
  i18nPackageName = "org.apache.hop.workflow.actions.ftpsget",
  name = "ActionFTPSGet.Name",
  description = "ActionFTPSGet.Description",
  image = "FTPSGet.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileTransfer",
  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/ftpsget.html"
)
public class ActionFtpsGet extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionFtpsGet.class; // for i18n purposes, needed by Translator!!

  private String serverName;
  private String userName;
  private String password;
  private String FTPSDirectory;
  private String targetDirectory;
  private String wildcard;
  private boolean binaryMode;
  private int timeout;
  private boolean remove;
  private boolean onlyGettingNewFiles; /* Don't overwrite files */
  private boolean activeConnection;

  private boolean movefiles;
  private String movetodirectory;

  private boolean adddate;
  private boolean addtime;
  private boolean SpecifyFormat;
  private String date_time_format;
  private boolean AddDateBeforeExtension;
  private boolean isaddresult;
  private boolean createmovefolder;
  private String port;
  private String proxyHost;

  private String proxyPort; /* string to allow variable substitution */

  private String proxyUsername;

  private String proxyPassword;

  private int connectionType;

  public static final String[] FILE_EXISTS_ACTIONS =
    new String[] { "ifFileExistsSkip", "ifFileExistsCreateUniq", "ifFileExistsFail" };
  public static final int ifFileExistsSkip = 0;
  public static final int ifFileExistsCreateUniq = 1;
  public static final int ifFileExistsFail = 2;

  private int ifFileExists;

  public String SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED = "success_when_at_least";
  public String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";
  public String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";

  private String nr_limit;
  private String success_condition;

  long NrErrors = 0;
  long NrfilesRetrieved = 0;
  boolean successConditionBroken = false;
  int limitFiles = 0;

  String localFolder = null;
  String realMoveToFolder = null;

  static String FILE_SEPARATOR = "/";

  public ActionFtpsGet( String n ) {
    super( n, "" );
    nr_limit = "10";
    port = "21";
    success_condition = SUCCESS_IF_NO_ERRORS;
    ifFileExists = 0;

    serverName = null;
    movefiles = false;
    movetodirectory = null;
    adddate = false;
    addtime = false;
    SpecifyFormat = false;
    AddDateBeforeExtension = false;
    isaddresult = true;
    createmovefolder = false;
    connectionType = FtpsConnection.CONNECTION_TYPE_FTP;
  }

  public ActionFtpsGet() {
    this( "" );
  }

  public Object clone() {
    ActionFtpsGet je = (ActionFtpsGet) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 550 ); // 490 chars in spaces and tag names alone

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "port", port ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "servername", serverName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "username", userName ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "password", Encr.encryptPasswordIfNotUsingVariables( password ) ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "FTPSdirectory", FTPSDirectory ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "targetdirectory", targetDirectory ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "wildcard", wildcard ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "binary", binaryMode ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "timeout", timeout ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "remove", remove ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "only_new", onlyGettingNewFiles ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "active", activeConnection ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "movefiles", movefiles ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "movetodirectory", movetodirectory ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "adddate", adddate ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "addtime", addtime ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "SpecifyFormat", SpecifyFormat ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "date_time_format", date_time_format ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "AddDateBeforeExtension", AddDateBeforeExtension ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "isaddresult", isaddresult ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "createmovefolder", createmovefolder ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "proxy_host", proxyHost ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "proxy_port", proxyPort ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "proxy_username", proxyUsername ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "proxy_password", Encr.encryptPasswordIfNotUsingVariables( proxyPassword ) ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "ifFileExists", getFileExistsAction( ifFileExists ) ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "nr_limit", nr_limit ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "success_condition", success_condition ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "connection_type", FtpsConnection.getConnectionTypeCode( connectionType ) ) );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      port = XmlHandler.getTagValue( entrynode, "port" );
      serverName = XmlHandler.getTagValue( entrynode, "servername" );
      userName = XmlHandler.getTagValue( entrynode, "username" );
      password = Encr.decryptPasswordOptionallyEncrypted( XmlHandler.getTagValue( entrynode, "password" ) );
      FTPSDirectory = XmlHandler.getTagValue( entrynode, "FTPSdirectory" );
      targetDirectory = XmlHandler.getTagValue( entrynode, "targetdirectory" );
      wildcard = XmlHandler.getTagValue( entrynode, "wildcard" );
      binaryMode = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "binary" ) );
      timeout = Const.toInt( XmlHandler.getTagValue( entrynode, "timeout" ), 10000 );
      remove = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "remove" ) );
      onlyGettingNewFiles = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "only_new" ) );
      activeConnection = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "active" ) );

      movefiles = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "movefiles" ) );
      movetodirectory = XmlHandler.getTagValue( entrynode, "movetodirectory" );

      adddate = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "adddate" ) );
      addtime = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "addtime" ) );
      SpecifyFormat = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "SpecifyFormat" ) );
      date_time_format = XmlHandler.getTagValue( entrynode, "date_time_format" );
      AddDateBeforeExtension =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "AddDateBeforeExtension" ) );

      String addresult = XmlHandler.getTagValue( entrynode, "isaddresult" );

      if ( Utils.isEmpty( addresult ) ) {
        isaddresult = true;
      } else {
        isaddresult = "Y".equalsIgnoreCase( addresult );
      }

      createmovefolder = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "createmovefolder" ) );

      proxyHost = XmlHandler.getTagValue( entrynode, "proxy_host" );
      proxyPort = XmlHandler.getTagValue( entrynode, "proxy_port" );
      proxyUsername = XmlHandler.getTagValue( entrynode, "proxy_username" );
      proxyPassword =
        Encr.decryptPasswordOptionallyEncrypted( XmlHandler.getTagValue( entrynode, "proxy_password" ) );

      ifFileExists = getFileExistsIndex( XmlHandler.getTagValue( entrynode, "ifFileExists" ) );
      nr_limit = XmlHandler.getTagValue( entrynode, "nr_limit" );
      success_condition =
        Const.NVL( XmlHandler.getTagValue( entrynode, "success_condition" ), SUCCESS_IF_NO_ERRORS );
      connectionType =
        FtpsConnection.getConnectionTypeByCode( Const.NVL(
          XmlHandler.getTagValue( entrynode, "connection_type" ), "" ) );
    } catch ( Exception xe ) {
      throw new HopXmlException( "Unable to load action of type 'FTPS' from XML node", xe );
    }
  }

  public void setLimit( String nr_limitin ) {
    this.nr_limit = nr_limitin;
  }

  public String getLimit() {
    return nr_limit;
  }

  public void setSuccessCondition( String success_condition ) {
    this.success_condition = success_condition;
  }

  public String getSuccessCondition() {
    return success_condition;
  }

  public void setCreateMoveFolder( boolean createmovefolderin ) {
    this.createmovefolder = createmovefolderin;
  }

  public boolean isCreateMoveFolder() {
    return createmovefolder;
  }

  public void setAddDateBeforeExtension( boolean AddDateBeforeExtension ) {
    this.AddDateBeforeExtension = AddDateBeforeExtension;
  }

  public boolean isAddDateBeforeExtension() {
    return AddDateBeforeExtension;
  }

  public void setAddToResult( boolean isaddresultin ) {
    this.isaddresult = isaddresultin;
  }

  public boolean isAddToResult() {
    return isaddresult;
  }

  public void setDateInFilename( boolean adddate ) {
    this.adddate = adddate;
  }

  public boolean isDateInFilename() {
    return adddate;
  }

  public void setTimeInFilename( boolean addtime ) {
    this.addtime = addtime;
  }

  public boolean isTimeInFilename() {
    return addtime;
  }

  public boolean isSpecifyFormat() {
    return SpecifyFormat;
  }

  public void setSpecifyFormat( boolean SpecifyFormat ) {
    this.SpecifyFormat = SpecifyFormat;
  }

  public String getDateTimeFormat() {
    return date_time_format;
  }

  public void setDateTimeFormat( String date_time_format ) {
    this.date_time_format = date_time_format;
  }

  /**
   * @return Returns the movefiles.
   */
  public boolean isMoveFiles() {
    return movefiles;
  }

  /**
   * @param movefilesin The movefiles to set.
   */
  public void setMoveFiles( boolean movefilesin ) {
    this.movefiles = movefilesin;
  }

  /**
   * @return Returns the movetodirectory.
   */
  public String getMoveToDirectory() {
    return movetodirectory;
  }

  /**
   * @param movetoin The movetodirectory to set.
   */
  public void setMoveToDirectory( String movetoin ) {
    this.movetodirectory = movetoin;
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
   * @return Returns the directory.
   */
  public String getFTPSDirectory() {
    return FTPSDirectory;
  }

  /**
   * @param directory The directory to set.
   */
  public void setFTPSDirectory( String directory ) {
    this.FTPSDirectory = directory;
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
   * @return Returns the targetDirectory.
   */
  public String getTargetDirectory() {
    return targetDirectory;
  }

  /**
   * @param targetDirectory The targetDirectory to set.
   */
  public void setTargetDirectory( String targetDirectory ) {
    this.targetDirectory = targetDirectory;
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

  /**
   * @return Returns the onlyGettingNewFiles.
   */
  public boolean isOnlyGettingNewFiles() {
    return onlyGettingNewFiles;
  }

  /**
   * @param onlyGettingNewFilesin The onlyGettingNewFiles to set.
   */
  public void setOnlyGettingNewFiles( boolean onlyGettingNewFilesin ) {
    this.onlyGettingNewFiles = onlyGettingNewFilesin;
  }

  /**
   * @return Returns the hostname of the FTPS-proxy.
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
   * @return Returns the port of the FTPS-proxy.
   */
  public String getProxyPort() {
    return proxyPort;
  }

  /**
   * @param proxyPort The port of the FTPS-proxy.
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

  public int getIfFileExists() {
    return ifFileExists;
  }

  public void setIfFileExists( int ifFileExists ) {
    this.ifFileExists = ifFileExists;
  }

  public static String getFileExistsAction( int actionId ) {
    if ( actionId < 0 || actionId >= FILE_EXISTS_ACTIONS.length ) {
      return FILE_EXISTS_ACTIONS[ 0 ];
    }
    return FILE_EXISTS_ACTIONS[ actionId ];
  }

  public static int getFileExistsIndex( String desc ) {
    int result = 0;
    if ( Utils.isEmpty( desc ) ) {
      return result;
    }
    for ( int i = 0; i < FILE_EXISTS_ACTIONS.length; i++ ) {
      if ( desc.equalsIgnoreCase( FILE_EXISTS_ACTIONS[ i ] ) ) {
        result = i;
        break;
      }
    }
    return result;
  }

  public Result execute( Result previousResult, int nr ) throws HopException {
    // LogWriter log = LogWriter.getInstance();
    logBasic( BaseMessages.getString( PKG, "ActionFTPS.Started", serverName ) );

    Result result = previousResult;
    result.setNrErrors( 1 );
    result.setResult( false );
    NrErrors = 0;
    NrfilesRetrieved = 0;
    successConditionBroken = false;
    boolean exitaction = false;
    limitFiles = Const.toInt( environmentSubstitute( getLimit() ), 10 );

    // Here let's put some controls before stating the workflow
    if ( movefiles ) {
      if ( Utils.isEmpty( movetodirectory ) ) {
        logError( BaseMessages.getString( PKG, "ActionFTPS.MoveToFolderEmpty" ) );
        return result;
      }
    }

    localFolder = environmentSubstitute( targetDirectory );

    if ( isDetailed() ) {
      logDetailed( BaseMessages.getString( PKG, "ActionFTPS.Start" ) );
    }

    FtpsConnection connection = null;

    try {
      // Create FTPS client to host:port ...

      String realServername = environmentSubstitute( serverName );
      String realUsername = environmentSubstitute( userName );
      String realPassword = Encr.decryptPasswordOptionallyEncrypted( environmentSubstitute( password ) );
      int realPort = Const.toInt( environmentSubstitute( this.port ), 0 );

      connection = new FtpsConnection( getConnectionType(), realServername, realPort, realUsername, realPassword, this );

      this.buildFTPSConnection( connection );

      // Create move to folder if necessary
      if ( movefiles && !Utils.isEmpty( movetodirectory ) ) {
        realMoveToFolder = normalizePath( environmentSubstitute( movetodirectory ) );
        // Folder exists?
        boolean folderExist = connection.isDirectoryExists( realMoveToFolder );
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "ActionFTPS.CheckMoveToFolder", realMoveToFolder ) );
        }

        if ( !folderExist ) {
          if ( createmovefolder ) {
            connection.createDirectory( realMoveToFolder );
            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "ActionFTPS.MoveToFolderCreated", realMoveToFolder ) );
            }
          } else {
            logError( BaseMessages.getString( PKG, "ActionFTPS.MoveToFolderNotExist" ) );
            exitaction = true;
            NrErrors++;
          }
        }
      }
      if ( !exitaction ) {
        Pattern pattern = null;
        if ( !Utils.isEmpty( wildcard ) ) {
          String realWildcard = environmentSubstitute( wildcard );
          pattern = Pattern.compile( realWildcard );
        }

        if ( !getSuccessCondition().equals( SUCCESS_IF_NO_ERRORS ) ) {
          limitFiles = Const.toInt( environmentSubstitute( getLimit() ), 10 );
        }

        // Get all the files in the current directory...
        downloadFiles( connection, connection.getWorkingDirectory(), pattern, result );
      }

    } catch ( Exception e ) {
      if ( !successConditionBroken && !exitaction ) {
        updateErrors();
      }
      logError( BaseMessages.getString( PKG, "ActionFTPS.ErrorGetting", e.getMessage() ) );
    } finally {
      if ( connection != null ) {
        try {
          connection.disconnect();
        } catch ( Exception e ) {
          logError( BaseMessages.getString( PKG, "ActionFTPS.ErrorQuitting", e.getMessage() ) );
        }
      }
    }

    result.setNrErrors( NrErrors );
    result.setNrFilesRetrieved( NrfilesRetrieved );
    if ( getSuccessStatus() ) {
      result.setResult( true );
    }
    if ( exitaction ) {
      result.setResult( false );
    }

    displayResults();

    return result;
  }

  private void downloadFiles( FtpsConnection connection, String folder, Pattern pattern, Result result ) throws HopException {

    List<FTPFile> fileList = connection.getFileList( folder );
    if ( isDetailed() ) {
      logDetailed( BaseMessages.getString( PKG, "ActionFTPS.FoundNFiles", fileList.size() ) );
    }

    for ( int i = 0; i < fileList.size(); i++ ) {

      if ( parentWorkflow.isStopped() ) {
        throw new HopException( BaseMessages.getString( PKG, "ActionFTPS.JobStopped" ) );
      }

      if ( successConditionBroken ) {
        throw new HopException( BaseMessages.getString( PKG, "ActionFTPS.SuccesConditionBroken", NrErrors ) );
      }

      FTPFile file = fileList.get( i );
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString(
          PKG, "ActionFTPS.AnalysingFile", file.getPath(), file.getName(), file.getMode(), file
            .getDate().toString(), file.getFileType() == 0 ? "File" : "Folder", String
            .valueOf( file.getSize() ) ) );
      }

      if ( !file.isDirectory() && !file.isLink() ) {
        // download file
        boolean getIt = true;
        if ( getIt ) {
          try {
            // See if the file matches the regular expression!
            if ( pattern != null ) {
              Matcher matcher = pattern.matcher( file.getName() );
              getIt = matcher.matches();
            }

            if ( getIt ) {
              // return local filename
              String localFilename = returnTargetFilename( file.getName() );

              if ( ( !onlyGettingNewFiles ) || ( onlyGettingNewFiles && needsDownload( localFilename ) ) ) {

                if ( isDetailed() ) {
                  logDetailed( BaseMessages.getString(
                    PKG, "ActionFTPS.GettingFile", file.getName(), targetDirectory ) );
                }

                // download file
                connection.downloadFile( file, returnTargetFilename( file.getName() ) );

                // Update retrieved files
                updateRetrievedFiles();

                if ( isDetailed() ) {
                  logDetailed( BaseMessages.getString( PKG, "ActionFTPS.GotFile", file.getName() ) );
                }

                // Add filename to result filenames
                addFilenameToResultFilenames( result, localFilename );

                // Delete the file if this is needed!
                if ( remove ) {
                  connection.deleteFile( file );

                  if ( isDetailed() ) {
                    if ( isDetailed() ) {
                      logDetailed( BaseMessages.getString( PKG, "ActionFTPS.DeletedFile", file.getName() ) );
                    }
                  }
                } else {
                  if ( movefiles ) {
                    // Try to move file to destination folder ...
                    connection.moveToFolder( file, realMoveToFolder );

                    if ( isDetailed() ) {
                      logDetailed( BaseMessages.getString(
                        PKG, "ActionFTPS.MovedFile", file.getName(), realMoveToFolder ) );
                    }
                  }
                }
              }
            }
          } catch ( Exception e ) {
            // Update errors number
            updateErrors();
            logError( BaseMessages.getString( PKG, "JobFTPS.UnexpectedError", e.toString() ) );
          }
        }
      }
    }
  }

  /**
   * normalize / to \ and remove trailing slashes from a path
   *
   * @param path
   * @return normalized path
   * @throws Exception
   */
  public String normalizePath( String path ) throws Exception {

    String normalizedPath = path.replaceAll( "\\\\", FILE_SEPARATOR );
    while ( normalizedPath.endsWith( "\\" ) || normalizedPath.endsWith( FILE_SEPARATOR ) ) {
      normalizedPath = normalizedPath.substring( 0, normalizedPath.length() - 1 );
    }

    return normalizedPath;
  }

  private void addFilenameToResultFilenames( Result result, String filename ) throws HopException {
    if ( isaddresult ) {
      FileObject targetFile = null;
      try {
        targetFile = HopVfs.getFileObject( filename );

        // Add to the result files...
        ResultFile resultFile =
          new ResultFile( ResultFile.FILE_TYPE_GENERAL, targetFile, parentWorkflow.getWorkflowName(), toString() );
        resultFile.setComment( BaseMessages.getString( PKG, "ActionFTPS.Downloaded", serverName ) );
        result.getResultFiles().put( resultFile.getFile().toString(), resultFile );

        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "ActionFTPS.FileAddedToResult", filename ) );
        }
      } catch ( Exception e ) {
        throw new HopException( e );
      } finally {
        try {
          targetFile.close();
          targetFile = null;
        } catch ( Exception e ) {
          // Ignore errors
        }
      }
    }
  }

  private void displayResults() {
    if ( isDetailed() ) {
      logDetailed( "=======================================" );
      logDetailed( BaseMessages.getString( PKG, "ActionFTPS.Log.Info.FilesInError", "" + NrErrors ) );
      logDetailed( BaseMessages.getString( PKG, "ActionFTPS.Log.Info.FilesRetrieved", "" + NrfilesRetrieved ) );
      logDetailed( "=======================================" );
    }
  }

  private boolean getSuccessStatus() {
    boolean retval = false;

    if ( ( NrErrors == 0 && getSuccessCondition().equals( SUCCESS_IF_NO_ERRORS ) )
      || ( NrfilesRetrieved >= limitFiles && getSuccessCondition().equals(
      SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED ) )
      || ( NrErrors <= limitFiles && getSuccessCondition().equals( SUCCESS_IF_ERRORS_LESS ) ) ) {
      retval = true;
    }

    return retval;
  }

  private void updateErrors() {
    NrErrors++;
    if ( checkIfSuccessConditionBroken() ) {
      // Success condition was broken
      successConditionBroken = true;
    }
  }

  private boolean checkIfSuccessConditionBroken() {
    boolean retval = false;
    if ( ( NrErrors > 0 && getSuccessCondition().equals( SUCCESS_IF_NO_ERRORS ) )
      || ( NrErrors >= limitFiles && getSuccessCondition().equals( SUCCESS_IF_ERRORS_LESS ) ) ) {
      retval = true;
    }
    return retval;
  }

  private void updateRetrievedFiles() {
    NrfilesRetrieved++;
  }

  /**
   * @param filename the filename from the FTPS server
   * @return the calculated target filename
   */
  private String returnTargetFilename( String filename ) {
    String retval = null;
    // Replace possible environment variables...
    if ( filename != null ) {
      retval = filename;
    } else {
      return null;
    }

    int lenstring = retval.length();
    int lastindexOfDot = retval.lastIndexOf( "." );
    if ( lastindexOfDot == -1 ) {
      lastindexOfDot = lenstring;
    }

    if ( isAddDateBeforeExtension() ) {
      retval = retval.substring( 0, lastindexOfDot );
    }

    SimpleDateFormat daf = new SimpleDateFormat();
    Date now = new Date();

    if ( SpecifyFormat && !Utils.isEmpty( date_time_format ) ) {
      daf.applyPattern( date_time_format );
      String dt = daf.format( now );
      retval += dt;
    } else {
      if ( adddate ) {
        daf.applyPattern( "yyyyMMdd" );
        String d = daf.format( now );
        retval += "_" + d;
      }
      if ( addtime ) {
        daf.applyPattern( "HHmmssSSS" );
        String t = daf.format( now );
        retval += "_" + t;
      }
    }

    if ( isAddDateBeforeExtension() ) {
      retval += retval.substring( lastindexOfDot, lenstring );
    }

    // Add foldername to filename
    retval = localFolder + Const.FILE_SEPARATOR + retval;
    return retval;
  }

  public boolean evaluates() {
    return true;
  }

  /**
   * See if the filename on the FTPS server needs downloading. The default is to check the presence of the file in the
   * target directory. If you need other functionality, extend this class and build it into a plugin.
   *
   * @param filename The local filename to check
   * @return true if the file needs downloading
   */
  protected boolean needsDownload( String filename ) {
    boolean retval = false;

    File file = new File( filename );

    if ( !file.exists() ) {
      // Local file not exists!
      if ( isDebug() ) {
        logDebug( toString(), BaseMessages.getString( PKG, "ActionFTPS.LocalFileNotExists" ), filename );
      }
      return true;
    } else {
      // Local file exists!
      if ( ifFileExists == ifFileExistsCreateUniq ) {
        if ( isDebug() ) {
          logDebug( toString(), BaseMessages.getString( PKG, "ActionFTPS.LocalFileExists" ), filename );
          // Create file with unique name
        }

        int lenstring = filename.length();
        int lastindexOfDot = filename.lastIndexOf( '.' );
        if ( lastindexOfDot == -1 ) {
          lastindexOfDot = lenstring;
        }

        filename =
          filename.substring( 0, lastindexOfDot )
            + StringUtil.getFormattedDateTimeNow( true ) + filename.substring( lastindexOfDot, lenstring );

        return true;
      } else if ( ifFileExists == ifFileExistsFail ) {
        logError( toString(), BaseMessages.getString( PKG, "ActionFTPS.LocalFileExists" ), filename );
        updateErrors();
      } else {
        if ( isDebug() ) {
          logDebug( toString(), BaseMessages.getString( PKG, "ActionFTPS.LocalFileExists" ), filename );
        }
      }
    }

    return retval;
  }

  /**
   * @return the activeConnection
   */
  public boolean isActiveConnection() {
    return activeConnection;
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
   * @param activeConnection the activeConnection to set
   */
  public void setActiveConnection( boolean activeConnection ) {
    this.activeConnection = activeConnection;
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

  void buildFTPSConnection( FtpsConnection connection ) throws Exception {
    if ( !Utils.isEmpty( proxyHost ) ) {
      String realProxy_host = environmentSubstitute( proxyHost );
      String realProxy_username = environmentSubstitute( proxyUsername );
      String realProxy_password =
        Encr.decryptPasswordOptionallyEncrypted( environmentSubstitute( proxyPassword ) );

      connection.setProxyHost( realProxy_host );
      if ( !Utils.isEmpty( realProxy_username ) ) {
        connection.setProxyUser( realProxy_username );
      }
      if ( !Utils.isEmpty( realProxy_password ) ) {
        connection.setProxyPassword( realProxy_password );
      }
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "ActionFTPS.OpenedProxyConnectionOn", realProxy_host ) );
      }

      int proxyport = Const.toInt( environmentSubstitute( proxyPort ), 21 );
      if ( proxyport != 0 ) {
        connection.setProxyPort( proxyport );
      }
    } else {
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "ActionFTPS.OpenedConnectionTo", connection.getHostName() ) );
      }
    }

    // set activeConnection connectmode ...
    if ( activeConnection ) {
      connection.setPassiveMode( false );
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "ActionFTPS.SetActive" ) );
      }
    } else {
      connection.setPassiveMode( true );
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "ActionFTPS.SetPassive" ) );
      }
    }

    // Set the timeout
    connection.setTimeOut( timeout );
    if ( isDetailed() ) {
      logDetailed( BaseMessages.getString( PKG, "ActionFTPS.SetTimeout", String.valueOf( timeout ) ) );
    }

    // login to FTPS host ...
    connection.connect();

    // Set binary mode
    if ( isBinaryMode() ) {
      connection.setBinaryMode( true );
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "ActionFTPS.SetBinary" ) );
      }
    }

    if ( isDetailed() ) {
      logDetailed( BaseMessages.getString( PKG, "ActionFTPS.LoggedIn", connection.getUserName() ) );
      logDetailed( BaseMessages.getString( PKG, "ActionFTPS.WorkingDirectory", connection
        .getWorkingDirectory() ) );
    }

    // move to spool dir ...
    if ( !Utils.isEmpty( FTPSDirectory ) ) {
      String realFTPSDirectory = environmentSubstitute( FTPSDirectory );
      realFTPSDirectory = normalizePath( realFTPSDirectory );
      connection.changeDirectory( realFTPSDirectory );
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "ActionFTPS.ChangedDir", realFTPSDirectory ) );
      }
    }
  }

}
