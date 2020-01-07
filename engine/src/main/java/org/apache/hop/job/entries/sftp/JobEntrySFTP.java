/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.job.entries.sftp;

import org.apache.hop.job.entry.validator.AbstractFileValidator;
import org.apache.hop.job.entry.validator.AndValidator;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.job.entry.validator.ValidatorContext;

import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * This defines a SFTP job entry.
 *
 * @author Matt
 * @since 05-11-2003
 *
 */
public class JobEntrySFTP extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntrySFTP.class; // for i18n purposes, needed by Translator2!!

  private static final int DEFAULT_PORT = 22;
  private String serverName;
  private String serverPort;
  private String userName;
  private String password;
  private String sftpDirectory;
  private String targetDirectory;
  private String wildcard;
  private boolean remove;
  private boolean isaddresult;
  private boolean createtargetfolder;
  private boolean copyprevious;
  private boolean usekeyfilename;
  private String keyfilename;
  private String keyfilepass;
  private String compression;
  // proxy
  private String proxyType;
  private String proxyHost;
  private String proxyPort;
  private String proxyUsername;
  private String proxyPassword;

  public JobEntrySFTP( String n ) {
    super( n, "" );
    serverName = null;
    serverPort = "22";
    isaddresult = true;
    createtargetfolder = false;
    copyprevious = false;
    usekeyfilename = false;
    keyfilename = null;
    keyfilepass = null;
    compression = "none";
    proxyType = null;
    proxyHost = null;
    proxyPort = null;
    proxyUsername = null;
    proxyPassword = null;
  }

  public JobEntrySFTP() {
    this( "" );
  }

  public Object clone() {
    JobEntrySFTP je = (JobEntrySFTP) super.clone();
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( super.getXML() );

    retval.append( "      " ).append( XMLHandler.addTagValue( "servername", serverName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "serverport", serverPort ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "username", userName ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "password", Encr.encryptPasswordIfNotUsingVariables( getPassword() ) ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "sftpdirectory", sftpDirectory ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "targetdirectory", targetDirectory ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "wildcard", wildcard ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "remove", remove ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "isaddresult", isaddresult ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "createtargetfolder", createtargetfolder ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "copyprevious", copyprevious ) );

    retval.append( "      " ).append( XMLHandler.addTagValue( "usekeyfilename", usekeyfilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "keyfilename", keyfilename ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "keyfilepass", Encr.encryptPasswordIfNotUsingVariables( keyfilepass ) ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "compression", compression ) );

    retval.append( "      " ).append( XMLHandler.addTagValue( "proxyType", proxyType ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "proxyHost", proxyHost ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "proxyPort", proxyPort ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "proxyUsername", proxyUsername ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "proxyPassword", Encr.encryptPasswordIfNotUsingVariables( proxyPassword ) ) );

    return retval.toString();
  }

  public void loadXML( Node entrynode, List<SlaveServer> slaveServers,
    IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode, slaveServers );
      serverName = XMLHandler.getTagValue( entrynode, "servername" );
      serverPort = XMLHandler.getTagValue( entrynode, "serverport" );
      userName = XMLHandler.getTagValue( entrynode, "username" );
      password = Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( entrynode, "password" ) );
      sftpDirectory = XMLHandler.getTagValue( entrynode, "sftpdirectory" );
      targetDirectory = XMLHandler.getTagValue( entrynode, "targetdirectory" );
      wildcard = XMLHandler.getTagValue( entrynode, "wildcard" );
      remove = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "remove" ) );

      String addresult = XMLHandler.getTagValue( entrynode, "isaddresult" );

      if ( Utils.isEmpty( addresult ) ) {
        isaddresult = true;
      } else {
        isaddresult = "Y".equalsIgnoreCase( addresult );
      }

      createtargetfolder = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "createtargetfolder" ) );
      copyprevious = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "copyprevious" ) );

      usekeyfilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "usekeyfilename" ) );
      keyfilename = XMLHandler.getTagValue( entrynode, "keyfilename" );
      keyfilepass = Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( entrynode, "keyfilepass" ) );
      compression = XMLHandler.getTagValue( entrynode, "compression" );

      proxyType = XMLHandler.getTagValue( entrynode, "proxyType" );
      proxyHost = XMLHandler.getTagValue( entrynode, "proxyHost" );
      proxyPort = XMLHandler.getTagValue( entrynode, "proxyPort" );
      proxyUsername = XMLHandler.getTagValue( entrynode, "proxyUsername" );
      proxyPassword =
        Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( entrynode, "proxyPassword" ) );
    } catch ( HopXMLException xe ) {
      throw new HopXMLException( "Unable to load job entry of type 'SFTP' from XML node", xe );
    }
  }

  /**
   * @return Returns the directory.
   */
  public String getScpDirectory() {
    return sftpDirectory;
  }

  /**
   * @param directory
   *          The directory to set.
   */
  public void setScpDirectory( String directory ) {
    this.sftpDirectory = directory;
  }

  /**
   * @return Returns the password.
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password
   *          The password to set.
   */
  public void setPassword( String password ) {
    this.password = password;
  }

  /**
   * @return Returns the compression.
   */
  public String getCompression() {
    return compression;
  }

  /**
   * @param compression
   *          The compression to set.
   */
  public void setCompression( String compression ) {
    this.compression = compression;
  }

  /**
   * @return Returns the serverName.
   */
  public String getServerName() {
    return serverName;
  }

  /**
   * @param serverName
   *          The serverName to set.
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
   * @param userName
   *          The userName to set.
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
   * @param wildcard
   *          The wildcard to set.
   */
  public void setWildcard( String wildcard ) {
    this.wildcard = wildcard;
  }

  public void setAddToResult( boolean isaddresultin ) {
    this.isaddresult = isaddresultin;
  }

  public boolean isAddToResult() {
    return isaddresult;
  }

  /**
   * @return Returns the targetDirectory.
   */
  public String getTargetDirectory() {
    return targetDirectory;
  }

  /**
   * @deprecated use {@link #setCreateTargetFolder(boolean)} instead
   */
  @Deprecated
  public void setcreateTargetFolder( boolean createtargetfolder ) {
    this.createtargetfolder = createtargetfolder;
  }

  /**
   * @deprecated use {@link #isCreateTargetFolder()} instead.
   * @return createTargetFolder
   */
  @Deprecated
  public boolean iscreateTargetFolder() {
    return createtargetfolder;
  }

  public boolean isCreateTargetFolder() {
    return createtargetfolder;
  }

  public void setCreateTargetFolder( boolean createtargetfolder ) {
    this.createtargetfolder = createtargetfolder;
  }

  public boolean isCopyPrevious() {
    return copyprevious;
  }

  public void setCopyPrevious( boolean copyprevious ) {
    this.copyprevious = copyprevious;
  }

  /**
   * @param targetDirectory
   *          The targetDirectory to set.
   */
  public void setTargetDirectory( String targetDirectory ) {
    this.targetDirectory = targetDirectory;
  }

  /**
   * @param remove
   *          The remove to set.
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

  public boolean isUseKeyFile() {
    return usekeyfilename;
  }

  public void setUseKeyFile( boolean value ) {
    this.usekeyfilename = value;
  }

  public String getKeyFilename() {
    return keyfilename;
  }

  public void setKeyFilename( String value ) {
    this.keyfilename = value;
  }

  public String getKeyPassPhrase() {
    return keyfilepass;
  }

  public void setKeyPassPhrase( String value ) {
    this.keyfilepass = value;
  }

  public String getProxyType() {
    return proxyType;
  }

  public void setProxyType( String value ) {
    this.proxyType = value;
  }

  public String getProxyHost() {
    return proxyHost;
  }

  public void setProxyHost( String value ) {
    this.proxyHost = value;
  }

  public String getProxyPort() {
    return proxyPort;
  }

  public void setProxyPort( String value ) {
    this.proxyPort = value;
  }

  public String getProxyUsername() {
    return proxyUsername;
  }

  public void setProxyUsername( String value ) {
    this.proxyUsername = value;
  }

  public String getProxyPassword() {
    return proxyPassword;
  }

  public void setProxyPassword( String value ) {
    this.proxyPassword = value;
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow = null;

    result.setResult( false );
    long filesRetrieved = 0;

    if ( log.isDetailed() ) {
      logDetailed( BaseMessages.getString( PKG, "JobSFTP.Log.StartJobEntry" ) );
    }
    HashSet<String> list_previous_filenames = new HashSet<String>();

    if ( copyprevious ) {
      if ( rows.size() == 0 ) {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobSFTP.ArgsFromPreviousNothing" ) );
        }
        result.setResult( true );
        return result;
      }
      try {

        // Copy the input row to the (command line) arguments
        for ( int iteration = 0; iteration < rows.size(); iteration++ ) {
          resultRow = rows.get( iteration );

          // Get file names
          String file_previous = resultRow.getString( 0, null );
          if ( !Utils.isEmpty( file_previous ) ) {
            list_previous_filenames.add( file_previous );
            if ( log.isDebug() ) {
              logDebug( BaseMessages.getString( PKG, "JobSFTP.Log.FilenameFromResult", file_previous ) );
            }
          }
        }
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "JobSFTP.Error.ArgFromPrevious" ) );
        result.setNrErrors( 1 );
        return result;
      }
    }

    SFTPClient sftpclient = null;

    // String substitution..
    String realServerName = environmentSubstitute( serverName );
    String realServerPort = environmentSubstitute( serverPort );
    String realUsername = environmentSubstitute( userName );
    String realPassword = Encr.decryptPasswordOptionallyEncrypted( environmentSubstitute( password ) );
    String realSftpDirString = environmentSubstitute( sftpDirectory );
    String realWildcard = environmentSubstitute( wildcard );
    String realTargetDirectory = environmentSubstitute( targetDirectory );
    String realKeyFilename = null;
    String realPassPhrase = null;
    FileObject TargetFolder = null;

    try {
      // Let's perform some checks before starting
      if ( isUseKeyFile() ) {
        // We must have here a private keyfilename
        realKeyFilename = environmentSubstitute( getKeyFilename() );
        if ( Utils.isEmpty( realKeyFilename ) ) {
          // Error..Missing keyfile
          logError( BaseMessages.getString( PKG, "JobSFTP.Error.KeyFileMissing" ) );
          result.setNrErrors( 1 );
          return result;
        }
        if ( !HopVFS.fileExists( realKeyFilename ) ) {
          // Error.. can not reach keyfile
          logError( BaseMessages.getString( PKG, "JobSFTP.Error.KeyFileNotFound", realKeyFilename ) );
          result.setNrErrors( 1 );
          return result;
        }
        realPassPhrase = environmentSubstitute( getKeyPassPhrase() );
      }

      if ( !Utils.isEmpty( realTargetDirectory ) ) {
        TargetFolder = HopVFS.getFileObject( realTargetDirectory, this );
        boolean TargetFolderExists = TargetFolder.exists();
        if ( TargetFolderExists ) {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobSFTP.Log.TargetFolderExists", realTargetDirectory ) );
          }
        } else {
          if ( !createtargetfolder ) {
            // Error..Target folder can not be found !
            logError( BaseMessages.getString( PKG, "JobSFTP.Error.TargetFolderNotExists", realTargetDirectory ) );
            result.setNrErrors( 1 );
            return result;
          } else {
            // create target folder
            TargetFolder.createFolder();
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "JobSFTP.Log.TargetFolderCreated", realTargetDirectory ) );
            }
          }
        }
      }

      if ( TargetFolder != null ) {
        TargetFolder.close();
        TargetFolder = null;
      }

      // Create sftp client to host ...
      sftpclient =
        new SFTPClient(
          InetAddress.getByName( realServerName ), Const.toInt( realServerPort, DEFAULT_PORT ), realUsername,
          realKeyFilename, realPassPhrase );
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString(
          PKG, "JobSFTP.Log.OpenedConnection", realServerName, realServerPort, realUsername ) );
      }

      // Set compression
      sftpclient.setCompression( getCompression() );

      // Set proxy?
      String realProxyHost = environmentSubstitute( getProxyHost() );
      if ( !Utils.isEmpty( realProxyHost ) ) {
        // Set proxy
        String password = getRealPassword( getProxyPassword() );
        sftpclient.setProxy(
          realProxyHost, environmentSubstitute( getProxyPort() ), environmentSubstitute( getProxyUsername() ),
            password, getProxyType() );
      }

      // login to ftp host ...
      sftpclient.login( realPassword );
      // Passwords should not appear in log files.
      // logDetailed("logged in using password "+realPassword); // Logging this seems a bad idea! Oh well.

      // move to spool dir ...
      if ( !Utils.isEmpty( realSftpDirString ) ) {
        try {
          sftpclient.chdir( realSftpDirString );
        } catch ( Exception e ) {
          logError( BaseMessages.getString( PKG, "JobSFTP.Error.CanNotFindRemoteFolder", realSftpDirString ) );
          throw new Exception( e );
        }
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobSFTP.Log.ChangedDirectory", realSftpDirString ) );
        }
      }
      Pattern pattern = null;
      // Get all the files in the current directory...
      String[] filelist = sftpclient.dir();
      if ( filelist == null ) {
        // Nothing was found !!! exit
        result.setResult( true );
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobSFTP.Log.Found", "" + 0 ) );
        }
        return result;
      }
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobSFTP.Log.Found", "" + filelist.length ) );
      }

      if ( !copyprevious ) {
        if ( !Utils.isEmpty( realWildcard ) ) {
          pattern = Pattern.compile( realWildcard );
        }
      }

      // Get the files in the list...
      for ( int i = 0; i < filelist.length && !parentJob.isStopped(); i++ ) {
        boolean getIt = true;

        if ( copyprevious ) {
          // filenames list is send by previous job entry
          // download if the current file is in this list
          getIt = list_previous_filenames.contains( filelist[i] );
        } else {
          // download files
          // but before see if the file matches the regular expression!
          if ( pattern != null ) {
            Matcher matcher = pattern.matcher( filelist[i] );
            getIt = matcher.matches();
          }
        }

        if ( getIt ) {
          if ( log.isDebug() ) {
            logDebug( BaseMessages.getString( PKG, "JobSFTP.Log.GettingFiles", filelist[i], realTargetDirectory ) );
          }

          FileObject targetFile = HopVFS.getFileObject(
            realTargetDirectory + Const.FILE_SEPARATOR + filelist[i], this );
          sftpclient.get( targetFile, filelist[i] );
          filesRetrieved++;

          if ( isaddresult ) {
            // Add to the result files...
            ResultFile resultFile =
              new ResultFile(
                ResultFile.FILE_TYPE_GENERAL, targetFile, parentJob
                  .getJobname(), toString() );
            result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "JobSFTP.Log.FilenameAddedToResultFilenames", filelist[i] ) );
            }
          }
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobSFTP.Log.TransferedFile", filelist[i] ) );
          }

          // Delete the file if this is needed!
          if ( remove ) {
            sftpclient.delete( filelist[i] );
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "JobSFTP.Log.DeletedFile", filelist[i] ) );
            }
          }
        }
      }

      result.setResult( true );
      result.setNrFilesRetrieved( filesRetrieved );
    } catch ( Exception e ) {
      result.setNrErrors( 1 );
      logError( BaseMessages.getString( PKG, "JobSFTP.Error.GettingFiles", e.getMessage() ) );
      logError( Const.getStackTracker( e ) );
    } finally {
      // close connection, if possible
      try {
        if ( sftpclient != null ) {
          sftpclient.disconnect();
        }
      } catch ( Exception e ) {
        // just ignore this, makes no big difference
      }

      try {
        if ( TargetFolder != null ) {
          TargetFolder.close();
          TargetFolder = null;
        }
        if ( list_previous_filenames != null ) {
          list_previous_filenames = null;
        }
      } catch ( Exception e ) {
        // Ignore errors
      }

    }

    return result;
  }

  public String getRealPassword( String password ) {
    return Utils.resolvePassword( variables, password );
  }

  public boolean evaluates() {
    return true;
  }

  public List<ResourceReference> getResourceDependencies( JobMeta jobMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( jobMeta );
    if ( !Utils.isEmpty( serverName ) ) {
      String realServerName = jobMeta.environmentSubstitute( serverName );
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( realServerName, ResourceType.SERVER ) );
      references.add( reference );
    }
    return references;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space,
    IMetaStore metaStore ) {
    JobEntryValidatorUtils.andValidator().validate( this, "serverName", remarks, AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace( ctx, getVariables() );
    AndValidator.putValidators( ctx, JobEntryValidatorUtils.notBlankValidator(), JobEntryValidatorUtils.fileExistsValidator() );
    JobEntryValidatorUtils.andValidator().validate( this, "targetDirectory", remarks, ctx );

    JobEntryValidatorUtils.andValidator().validate( this, "userName", remarks, AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );
    JobEntryValidatorUtils.andValidator().validate( this, "password", remarks, AndValidator.putValidators( JobEntryValidatorUtils.notNullValidator() ) );
    JobEntryValidatorUtils.andValidator().validate( this, "serverPort", remarks, AndValidator.putValidators( JobEntryValidatorUtils.integerValidator() ) );
  }
}
