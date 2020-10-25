/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2010-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.vfs.googledrive;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.hop.vfs.googledrive.util.CustomAuthorizationCodeInstalledApp;
import org.apache.hop.vfs.googledrive.util.CustomDataStoreFactory;
import org.apache.hop.vfs.googledrive.util.CustomLocalServerReceiver;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

public class GoogleDriveFileObject extends AbstractFileObject {

  public static final String PROJECT_NAME = "googledrive";

  private static final List<String> SCOPES = Arrays.asList( DriveScopes.DRIVE );
  private CustomDataStoreFactory DATA_STORE_FACTORY;
  private final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private HttpTransport HTTP_TRANSPORT;
  private String APPLICATION_NAME = "";
  private Drive driveService;
  private FileType mimeType;
  private String id;

  private static ResourceBundle resourceBundle = PropertyResourceBundle.getBundle( "plugin" );

  public enum MIME_TYPES {
    FILE( "application/vnd.google-apps.file", FileType.FILE ), FOLDER( "application/vnd.google-apps.folder",
        FileType.FOLDER );
    private final String mimeType;
    private final FileType fileType;

    private MIME_TYPES( String mimeType, FileType fileType ) {
      this.mimeType = mimeType;
      this.fileType = fileType;
    }

    public static FileType get( String type ) {
      FileType fileType = null;
      if ( FOLDER.mimeType.equals( type ) ) {
        fileType = FOLDER.fileType;
      } else {
        fileType = FILE.fileType;
      }
      return fileType;
    }
  }

  private final java.io.File DATA_STORE_DIR = new java.io.File( resolveCredentialsPath() );

  protected GoogleDriveFileObject( final AbstractFileName fileName, final GoogleDriveFileSystem fileSystem )
      throws FileSystemException {
    super( fileName, fileSystem );
    try {
      HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      DATA_STORE_FACTORY = new CustomDataStoreFactory( DATA_STORE_DIR );
      driveService = getDriveService();
      resolveFileMetadata();
    } catch ( Exception e ) {
      throw new FileSystemException( e );
    }
  }

  protected String[] doListChildren() throws Exception {
    String[] children = null;
    if ( isFolder() ) {
      id = id == null ? "root" : id;
      String fileQuery = "'" + id + "' in parents and trashed=false";
      FileList files = driveService.files().list().setQ( fileQuery ).execute();
      List<String> fileNames = new ArrayList<String>();
      for ( File file : files.getFiles() ) {
        fileNames.add( file.getName() );
      }
      children = fileNames.toArray( new String[0] );
    }
    return children;
  }

  protected void doCreateFolder() throws Exception {
    if ( !getName().getBaseName().isEmpty() ) {
      File folder = new File();
      folder.setName( getName().getBaseName() );
      folder.setMimeType( MIME_TYPES.FOLDER.mimeType );
      folder = driveService.files().create( folder ).execute();
      if ( folder != null ) {
        id = folder.getId();
        mimeType = MIME_TYPES.get( folder.getMimeType() );
      }
    }
  }

  protected void doDelete() throws Exception {
    driveService.files().delete( id ).execute();
    id = null;
    mimeType = null;
  }

  protected long doGetContentSize() throws Exception {
    return -1;
  }

  protected FileType doGetType() throws Exception {
    return mimeType;
  }

  protected InputStream doGetInputStream() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    driveService.files().get( id ).executeMediaAndDownloadTo( out );
    ByteArrayInputStream in = new ByteArrayInputStream( out.toByteArray() );
    return in;
  }

  protected OutputStream doGetOutputStream( boolean append ) throws Exception {
    final File parent = getName().getParent() != null ? searchFile( getName().getParent().getBaseName(), null ) : null;
    ByteArrayOutputStream out = new ByteArrayOutputStream() {
      public void close() throws IOException {
        File file = new File();
        file.setName( getName().getBaseName() );
        if ( parent != null ) {
          file.setParents( Collections.singletonList( parent.getId() ) );
        }
        ByteArrayContent fileContent = new ByteArrayContent( "application/octet-stream", toByteArray() );
        if ( count > 0 ) {
          driveService.files().create( file, fileContent ).execute();
          ( (GoogleDriveFileSystem) getFileSystem() ).clearFileFromCache( getName() );
        }
      }
    };
    return out;
  }

  protected long doGetLastModifiedTime() throws Exception {
    return -1;
  }

  private void resolveFileMetadata() throws Exception {
    String parentId = null;
    if ( getName().getParent() != null ) {
      File parent = searchFile( getName().getParent().getBaseName(), null );
      if ( parent != null ) {
        FileType mime = MIME_TYPES.get( parent.getMimeType() );
        if ( mime.equals( FileType.FOLDER ) ) {
          parentId = parent.getId();
        }
      }
    }

    String fileName = getName().getBaseName();
    File file = searchFile( fileName, parentId );
    if ( file != null ) {
      mimeType = MIME_TYPES.get( file.getMimeType() );
      id = file.getId();
    } else {
      if ( getName().getURI().equals( GoogleDriveFileProvider.SCHEME + ":///" ) ) {
        mimeType = FileType.FOLDER;
      }
    }
  }

  private File searchFile( String fileName, String parentId ) throws Exception {
    File file = null;
    StringBuffer fileQuery = new StringBuffer();
    fileQuery.append( "name = '" + fileName + "'" );
    if ( parentId != null ) {
      fileQuery.append( " and '" + parentId + "' in parents and trashed=false" );
    }
    FileList fileList = driveService.files().list().setQ( fileQuery.toString() ).execute();
    if ( !fileList.getFiles().isEmpty() ) {
      file = fileList.getFiles().get( 0 );
    }
    return file;
  }

  private Credential authorize() throws IOException {
    InputStream in = new FileInputStream( resolveCredentialsPath() + "/" + resourceBundle.getString( "client.secrets" ) );
    GoogleClientSecrets clientSecrets = GoogleClientSecrets.load( JSON_FACTORY, new InputStreamReader( in ) );

    GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder( HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES )
            .setDataStoreFactory( DATA_STORE_FACTORY ).setAccessType( "offline" ).build();

    Credential credential = new CustomAuthorizationCodeInstalledApp( flow, new CustomLocalServerReceiver() ).authorize( "user" );
    return credential;
  }

  private Drive getDriveService() throws IOException {
    Credential credential = authorize();
    return new Drive.Builder( HTTP_TRANSPORT, JSON_FACTORY, credential ).setApplicationName( APPLICATION_NAME ).build();
  }

  public static String resolveCredentialsPath() {
    String path = GoogleDriveFileObject.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    path = path.substring( 0, path.indexOf( PROJECT_NAME ) );
    path = path + PROJECT_NAME + "/" + resourceBundle.getString( "credentials.folder" );
    return path;
  }
}
