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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.actions.ftpsget.ftp4che.SecureDataFtpConnection;
import org.ftp4che.FTPConnection;
import org.ftp4che.FTPConnectionFactory;
import org.ftp4che.event.FTPEvent;
import org.ftp4che.event.FTPListener;
import org.ftp4che.exception.ConfigurationException;
import org.ftp4che.util.ftpfile.FTPFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class FtpsConnection implements FTPListener {

  private static final Class<?> PKG = ActionFtpsGet.class; // Needed by Translator
  private ILogChannel logger;

  public static final String HOME_FOLDER = "/";
  public static final String COMMAND_SUCCESSUL = "COMMAND SUCCESSFUL";

  public static final int CONNECTION_TYPE_FTP = 0;
  public static final int CONNECTION_TYPE_FTP_IMPLICIT_SSL = 1;
  public static final int CONNECTION_TYPE_FTP_AUTH_SSL = 2;
  public static final int CONNECTION_TYPE_FTP_IMPLICIT_SSL_WITH_CRYPTED = 3;
  public static final int CONNECTION_TYPE_FTP_AUTH_TLS = 4;
  public static final int CONNECTION_TYPE_FTP_IMPLICIT_TLS = 5;
  public static final int CONNECTION_TYPE_FTP_IMPLICIT_TLS_WITH_CRYPTED = 6;

  public static final String[] connectionTypeDesc = new String[] {
    BaseMessages.getString( PKG, "JobFTPS.ConnectionType.FTP" ),
    BaseMessages.getString( PKG, "JobFTPS.ConnectionType.ImplicitSSL" ),
    BaseMessages.getString( PKG, "JobFTPS.ConnectionType.AuthSSL" ),
    BaseMessages.getString( PKG, "JobFTPS.ConnectionType.ImplicitSSLCrypted" ),
    BaseMessages.getString( PKG, "JobFTPS.ConnectionType.AuthTLS" ),
    BaseMessages.getString( PKG, "JobFTPS.ConnectionType.ImplicitTLS" ),
    BaseMessages.getString( PKG, "JobFTPS.ConnectionType.ImplicitTLSCrypted" ) };

  public static final String[] connectionTypeCode = new String[] {
    "FTP_CONNECTION", "IMPLICIT_SSL_FTP_CONNECTION", "AUTH_SSL_FTP_CONNECTION",
    "IMPLICIT_SSL_WITH_CRYPTED_DATA_FTP_CONNECTION", "AUTH_TLS_FTP_CONNECTION", "IMPLICIT_TLS_FTP_CONNECTION",
    "IMPLICIT_TLS_WITH_CRYPTED_DATA_FTP_CONNECTION" };

  private FTPConnection connection = null;
  private ArrayList<String> replies = new ArrayList<>();

  private String hostName;
  private int portNumber;
  private String userName;
  private String passWord;
  private int connectionType;
  private int timeOut;
  private boolean passiveMode;

  private String proxyHost;
  private String proxyUser;
  private String proxyPassword;
  private int proxyPort;
  private IVariables nameSpace;

  /**
   * Please supply real namespace as it is required for proper VFS operation
   */
  @Deprecated
  public FtpsConnection( int connectionType, String hostname, int port, String username, String password ) {
    this( connectionType, hostname, port, username, password, new Variables() );
  }

  public FtpsConnection( int connectionType, String hostname, int port, String username, String password,
                         IVariables nameSpace ) {
    this.hostName = hostname;
    this.portNumber = port;
    this.userName = username;
    this.passWord = password;
    this.connectionType = connectionType;
    this.passiveMode = false;
    this.nameSpace = nameSpace;
    this.logger = new LogChannel( this );
  }

  /**
   * this method is used to set the proxy host
   *
   * @param proxyhost true: proxy host
   */
  public void setProxyHost( String proxyhost ) {
    this.proxyHost = proxyhost;
  }

  /**
   * this method is used to set the proxy port
   *
   * @param proxyport true: proxy port
   */
  public void setProxyPort( int proxyport ) {
    this.proxyPort = proxyport;
  }

  /**
   * this method is used to set the proxy username
   *
   * @param username true: proxy username
   */
  public void setProxyUser( String username ) {
    this.proxyUser = username;
  }

  /**
   * this method is used to set the proxy password
   *
   * @param password true: proxy password
   */
  public void setProxyPassword( String password ) {
    this.proxyPassword = password;
  }

  /**
   * this method is used to connect to a remote host
   *
   * @throws HopException
   */
  public void connect() throws HopException {
    try {
      connection =
        FTPConnectionFactory.getInstance( getProperties(
          hostName, portNumber, userName, passWord, connectionType, timeOut, passiveMode ) );
      if ( connection.getConnectionType() == FTPConnection.IMPLICIT_SSL_WITH_CRYPTED_DATA_FTP_CONNECTION
        || connection.getConnectionType() == FTPConnection.IMPLICIT_TLS_WITH_CRYPTED_DATA_FTP_CONNECTION ) {
        // need to upgrade to our custom connection to force crypted data channel
        connection = getSecureDataFtpConnection( connection, passWord, timeOut );
      }
      connection.addFTPStatusListener( this );
      connection.connect();
    } catch ( Exception e ) {
      connection = null;
      throw new HopException( BaseMessages.getString( PKG, "JobFTPS.Error.Connecting", hostName ), e );
    }
  }

  @VisibleForTesting
  protected FTPConnection getSecureDataFtpConnection(FTPConnection connection, String password, int timeout )
    throws ConfigurationException {
    return new SecureDataFtpConnection( connection, password, timeout );
  }

  private Properties getProperties( String hostname, int port, String username, String password,
                                    int connectionType, int timeout, boolean passiveMode ) {
    Properties pt = new Properties();
    pt.setProperty( "connection.host", hostname );
    pt.setProperty( "connection.port", String.valueOf( port ) );
    pt.setProperty( "user.login", username );
    pt.setProperty( "user.password", password );
    pt.setProperty( "connection.type", getConnectionType( connectionType ) );
    pt.setProperty( "connection.timeout", String.valueOf( timeout ) );
    pt.setProperty( "connection.passive", String.valueOf( passiveMode ) );
    // Set proxy
    if ( this.proxyHost != null ) {
      pt.setProperty( "proxy.host", this.proxyHost );
    }
    if ( this.proxyPort != 0 ) {
      pt.setProperty( "proxy.port", String.valueOf( this.proxyPort ) );
    }
    if ( this.proxyUser != null ) {
      pt.setProperty( "proxy.user", this.proxyUser );
    }
    if ( this.proxyPassword != null ) {
      pt.setProperty( "proxy.pass", this.proxyPassword );
    }

    return pt;
  }

  public static String getConnectionTypeDesc( String tt ) {
    if ( Utils.isEmpty( tt ) ) {
      return connectionTypeDesc[ 0 ];
    }
    if ( tt.equalsIgnoreCase( connectionTypeCode[ 1 ] ) ) {
      return connectionTypeDesc[ 1 ];
    } else {
      return connectionTypeDesc[ 0 ];
    }
  }

  public static String getConnectionTypeCode( String tt ) {
    if ( tt == null ) {
      return connectionTypeCode[ 0 ];
    }
    if ( tt.equals( connectionTypeDesc[ 1 ] ) ) {
      return connectionTypeCode[ 1 ];
    } else {
      return connectionTypeCode[ 0 ];
    }
  }

  public static String getConnectionTypeDesc( int i ) {
    if ( i < 0 || i >= connectionTypeDesc.length ) {
      return connectionTypeDesc[ 0 ];
    }
    return connectionTypeDesc[ i ];
  }

  public static String getConnectionType( int i ) {
    return connectionTypeCode[ i ];
  }

  public static int getConnectionTypeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < connectionTypeDesc.length; i++ ) {
      if ( connectionTypeDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    // If this fails,return the first value
    return 0;
  }

  public static int getConnectionTypeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < connectionTypeCode.length; i++ ) {
      if ( connectionTypeCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public static String getConnectionTypeCode( int i ) {
    if ( i < 0 || i >= connectionTypeCode.length ) {
      return connectionTypeCode[ 0 ];
    }
    return connectionTypeCode[ i ];
  }

  /**
   * public void setBinaryMode(boolean type)
   * <p>
   * this method is used to set the transfer type to binary
   *
   * @param type true: Binary
   * @throws HopException
   */
  public void setBinaryMode( boolean type ) throws HopException {
    try {
      connection.setTransferType( true );
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  /**
   * this method is used to set the mode to passive
   *
   * @param passivemode true: passive mode
   */
  public void setPassiveMode( boolean passivemode ) {
    this.passiveMode = passivemode;
  }

  /**
   * this method is used to return the passive mode
   *
   * @return TRUE if we use passive mode
   */
  public boolean isPassiveMode() {
    return this.passiveMode;
  }

  /**
   * this method is used to set the timeout
   *
   * @param timeout
   */
  public void setTimeOut( int timeout ) {
    this.timeOut = timeout;
  }

  /**
   * this method is used to return the timeout
   *
   * @return timeout
   */
  public int getTimeOut() {
    return this.timeOut;
  }

  public String getUserName() {
    return userName;
  }

  public String getHostName() {
    return hostName;
  }

  public ArrayList<String> getReplies() {
    return replies;
  }

  /**
   * this method is used to set the connection type
   *
   * @param connectiontype true: connection type
   */
  public void setConnectionType( int connectiontype ) {
    this.connectionType = connectiontype;
  }

  public int getConnectionType() {
    return this.connectionType;
  }

  public void connectionStatusChanged( FTPEvent arg0 ) {
  }

  public void replyMessageArrived( FTPEvent event ) {
    this.replies = new ArrayList<>();
    for ( String e : event.getReply().getLines() ) {
      if ( !e.trim().equals( "" ) ) {
        e = e.substring( 3 ).trim().replace( "\n", "" );
        if ( !e.toUpperCase().contains( COMMAND_SUCCESSUL ) ) {
          e = e.substring( 1 ).trim();
          replies.add( e );
        }
      }
    }
  }

  /**
   * this method change FTP working directory
   *
   * @param directory change the working directory
   * @throws HopException
   */
  public void changeDirectory( String directory ) throws HopException {
    try {
      this.connection.changeDirectory( directory );
    } catch ( Exception f ) {
      throw new HopException( BaseMessages.getString( PKG, "JobFTPS.Error.ChangingFolder", directory ), f );
    }
  }

  /**
   * this method is used to create a directory in remote host
   *
   * @param directory directory name on remote host
   * @throws HopException
   */
  public void createDirectory( String directory ) throws HopException {
    try {
      this.connection.makeDirectory( directory );
    } catch ( Exception f ) {
      throw new HopException( BaseMessages.getString( PKG, "JobFTPS.Error.CreationFolder", directory ), f );
    }
  }

  public List<FTPFile> getFileList( String folder ) throws HopException {
    try {
      if ( connection != null ) {
        List<FTPFile> response = connection.getDirectoryListing( folder );
        return response;
      } else {
        return null;
      }
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  /**
   * this method is used to download a file from a remote host
   *
   * @param file          remote file to download
   * @param localFilename target filename
   * @throws HopException
   */
  public void downloadFile( FTPFile file, String localFilename ) throws HopException {
    try {
      FileObject localFile = HopVfs.getFileObject( localFilename );
      writeToFile( connection.downloadStream( file ), localFile.getContent().getOutputStream(), localFilename );
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  private void writeToFile( InputStream is, OutputStream os, String filename ) throws HopException {
    try {
      IOUtils.copy( is, os );
    } catch ( IOException e ) {
      throw new HopException( BaseMessages.getString( PKG, "JobFTPS.Error.WritingToFile", filename ), e );
    } finally {
      IOUtils.closeQuietly( is );
      IOUtils.closeQuietly( os );
    }
  }

  /**
   * this method is used to upload a file to a remote host
   *
   * @param localFileName Local full filename
   * @param shortFileName Filename in remote host
   * @throws HopException
   */
  public void uploadFile( String localFileName, String shortFileName ) throws HopException {
    FileObject file = null;

    try {
      file = HopVfs.getFileObject( localFileName );
      this.connection.uploadStream( file.getContent().getInputStream(), new FTPFile( new File( shortFileName ) ) );
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "JobFTPS.Error.UuploadingFile", localFileName ), e );
    } finally {
      if ( file != null ) {
        try {
          file.close();
        } catch ( Exception e ) {
          //we do not able to close file will log it
          logger.logDetailed( "Unable to close file file", e );
        }
      }
    }
  }

  /**
   * this method is used to return filenames in working directory
   *
   * @return filenames
   * @throws HopException
   */
  public String[] getFileNames() throws HopException {
    ArrayList<String> list = null;
    try {
      List<FTPFile> fileList = getFileList( getWorkingDirectory() );
      list = new ArrayList<>();
      Iterator<FTPFile> it = fileList.iterator();
      while ( it.hasNext() ) {
        FTPFile file = it.next();
        if ( !file.isDirectory() ) {
          list.add( file.getName() );
        }
      }
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "JobFTPS.Error.RetrievingFilenames" ), e );
    }
    return list == null ? null : list.toArray( new String[ list.size() ] );
  }

  /**
   * this method is used to delete a file in remote host
   *
   * @param file File on remote host to delete
   * @throws HopException
   */
  public void deleteFile( FTPFile file ) throws HopException {
    try {
      this.connection.deleteFile( file );
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "JobFTPS.Error.DeletingFile", file.getName() ), e );
    }
  }

  /**
   * this method is used to delete a file in remote host
   *
   * @param filename Name of file on remote host to delete
   * @throws HopException
   */
  public void deleteFile( String filename ) throws HopException {
    try {
      this.connection.deleteFile( new FTPFile( getWorkingDirectory(), filename ) );
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "JobFTPS.Error.DeletingFile", filename ), e );
    }
  }

  /**
   * this method is used to move a file to remote directory
   *
   * @param fromFile         File on remote host to move
   * @param targetFoldername Target remote folder
   * @throws HopException
   */
  public void moveToFolder( FTPFile fromFile, String targetFoldername ) throws HopException {
    try {
      this.connection.renameFile( fromFile, new FTPFile( targetFoldername, fromFile.getName() ) );
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "JobFTPS.Error.MovingFileToFolder", fromFile
        .getName(), targetFoldername ), e );
    }
  }

  /**
   * Checks if a directory exists
   *
   * @return true if the directory exists
   */
  public boolean isDirectoryExists( String directory ) {
    String currectDirectory = null;
    boolean retval = false;
    try {
      // Before save current directory
      currectDirectory = this.connection.getWorkDirectory();
      // Change directory
      this.connection.changeDirectory( directory );
      retval = true;
    } catch ( Exception e ) {
      // Ignore directory change errors
    } finally {
      // switch back to the current directory
      if ( currectDirectory != null ) {
        try {
          this.connection.changeDirectory( currectDirectory );
        } catch ( Exception e ) {
          // Ignore directory change errors
        }
      }
    }
    return retval;
  }

  /**
   * Checks if a file exists on remote host
   *
   * @param filename the name of the file to check
   * @return true if the file exists
   */
  public boolean isFileExists( String filename ) {
    boolean retval = false;
    try {
      FTPFile file = new FTPFile( new File( filename ) );
      // Get modification time just to check if file exists
      connection.getModificationTime( file );
      retval = true;
    } catch ( Exception e ) {
      // Ignore errors
    }
    return retval;
  }

  /**
   * Returns the working directory
   *
   * @return working directory
   * @throws Exception
   */
  public String getWorkingDirectory() throws Exception {
    return this.connection.getWorkDirectory();
  }

  /**
   * this method is used to disconnect the connection
   */
  public void disconnect() {
    if ( this.connection != null ) {
      this.connection.disconnect();
    }
    if ( this.replies != null ) {
      this.replies.clear();
    }
  }

}
