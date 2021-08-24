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

package org.apache.hop.vfs.googledrive;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.hop.vfs.googledrive.config.GoogleDriveConfigSingleton;
import org.apache.hop.vfs.googledrive.util.CustomDataStoreFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GoogleDriveFileObject extends AbstractFileObject {

  public static final String APPLICATION_NAME = "Apache-Hop-Google-Drive-VFS";

  private static final List<String> SCOPES = Arrays.asList(DriveScopes.DRIVE);
  private CustomDataStoreFactory DATA_STORE_FACTORY;
  private static JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static NetHttpTransport HTTP_TRANSPORT;
  private Drive driveService;
  private FileType mimeType;
  private String id;

  public enum MIME_TYPES {
    FILE("application/vnd.google-apps.file", FileType.FILE),
    FOLDER("application/vnd.google-apps.folder", FileType.FOLDER);
    private final String mimeType;
    private final FileType fileType;

    private MIME_TYPES(String mimeType, FileType fileType) {
      this.mimeType = mimeType;
      this.fileType = fileType;
    }

    public static FileType get(String type) {
      FileType fileType = null;
      if (FOLDER.mimeType.equals(type)) {
        fileType = FOLDER.fileType;
      } else {
        fileType = FILE.fileType;
      }
      return fileType;
    }
  }

  protected GoogleDriveFileObject(
      final AbstractFileName fileName, final GoogleDriveFileSystem fileSystem)
      throws FileSystemException {
    super(fileName, fileSystem);
    try {
      HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      DATA_STORE_FACTORY = new CustomDataStoreFactory(getTokensFolder());
      driveService = getDriveService();
      resolveFileMetadata();
    } catch (Exception e) {
      throw new FileSystemException(e);
    }
  }

  protected String[] doListChildren() throws Exception {
    String[] children = null;
    if (isFolder()) {
      id = id == null ? "root" : id;
      String fileQuery = "'" + id + "' in parents and trashed=false";
      FileList files = driveService.files().list().setQ(fileQuery).execute();
      List<String> fileNames = new ArrayList<>();
      for (File file : files.getFiles()) {
        fileNames.add(file.getName());
      }
      children = fileNames.toArray(new String[0]);
    }
    return children;
  }

  protected void doCreateFolder() throws Exception {
    if (!getName().getBaseName().isEmpty()) {
      File folder = new File();
      folder.setName(getName().getBaseName());
      folder.setMimeType(MIME_TYPES.FOLDER.mimeType);
      folder = driveService.files().create(folder).execute();
      if (folder != null) {
        id = folder.getId();
        mimeType = MIME_TYPES.get(folder.getMimeType());
      }
    }
  }

  protected void doDelete() throws Exception {
    driveService.files().delete(id).execute();
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
    driveService.files().get(id).executeMediaAndDownloadTo(out);
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    return in;
  }

  protected OutputStream doGetOutputStream(boolean append) throws Exception {
    final File parent =
        getName().getParent() != null
            ? searchFile(getName().getParent().getBaseName(), null)
            : null;
    ByteArrayOutputStream out =
        new ByteArrayOutputStream() {
          public void close() throws IOException {
            File file = new File();
            file.setName(getName().getBaseName());
            if (parent != null) {
              file.setParents(Collections.singletonList(parent.getId()));
            }
            ByteArrayContent fileContent =
                new ByteArrayContent("application/octet-stream", toByteArray());
            if (count > 0) {
              driveService.files().create(file, fileContent).execute();
              ((GoogleDriveFileSystem) getFileSystem()).clearFileFromCache(getName());
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
    if (getName().getParent() != null) {
      File parent = searchFile(getName().getParent().getBaseName(), null);
      if (parent != null) {
        FileType mime = MIME_TYPES.get(parent.getMimeType());
        if (mime.equals(FileType.FOLDER)) {
          parentId = parent.getId();
        }
      }
    }

    String fileName = getName().getBaseName();
    File file = searchFile(fileName, parentId);
    if (file != null) {
      mimeType = MIME_TYPES.get(file.getMimeType());
      id = file.getId();
    } else {
      if (getName().getURI().equals(GoogleDriveFileProvider.SCHEME + ":///")) {
        mimeType = FileType.FOLDER;
      }
    }
  }

  private File searchFile(String fileName, String parentId) throws Exception {
    File file = null;
    StringBuffer fileQuery = new StringBuffer();
    fileQuery.append("name = '" + fileName + "'");
    if (parentId != null) {
      fileQuery.append(" and '" + parentId + "' in parents and trashed=false");
    }
    FileList fileList = driveService.files().list().setQ(fileQuery.toString()).execute();
    if (!fileList.getFiles().isEmpty()) {
      file = fileList.getFiles().get(0);
    }
    return file;
  }

  /**
   * Creates an authorized Credential object.
   *
   * @param HTTP_TRANSPORT The network HTTP Transport.
   * @return An authorized Credential object.
   * @throws IOException If the credentials.json file cannot be found.
   */
  private static Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT)
      throws IOException {
    // Load client secrets.
    InputStream in = new FileInputStream(getCredentialsFile());
    if (in == null) {
      throw new FileNotFoundException("Resource not found: " + getCredentialsFile());
    }
    GoogleClientSecrets clientSecrets =
        GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

    // Build flow and trigger user authorization request.
    GoogleAuthorizationCodeFlow flow =
        new GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
            .setDataStoreFactory(new FileDataStoreFactory(getTokensFolder()))
            .setAccessType("offline")
            .build();
    LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
    return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
  }

  private Drive getDriveService() throws IOException {
    Credential credential = getCredentials(HTTP_TRANSPORT);
    return new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
        .setApplicationName(APPLICATION_NAME)
        .build();
  }

  private static java.io.File getTokensFolder() throws FileSystemException {
    String tokensFolder = GoogleDriveConfigSingleton.getConfig().getTokensFolder();
    if (tokensFolder == null) {
      throw new FileSystemException(
          "Google Drive VFS: please specify a local folder to store tokens in the configuration.  You can do this in the 'Google Drive' tab of the options dialog in the GUI or using the Hop Config script.");
    }
    return new java.io.File(tokensFolder);
  }

  public static String getCredentialsFile() throws FileSystemException {

    String credentialsFile = GoogleDriveConfigSingleton.getConfig().getCredentialsFile();
    if (credentialsFile == null) {
      throw new FileSystemException(
          "Google Drive VFS: please specify a credentials JSON file in the configuration. You can do this in the 'Google Drive' tab of the options dialog in the GUI or using the Hop Config script.");
    }
    return credentialsFile;
  }
}
