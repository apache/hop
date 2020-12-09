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

package org.apache.hop.vfs.s3.s3common;

import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.hop.vfs.s3.s3n.vfs.S3NFileSystem;

/** Configuration Builder for S3 File System */
public class S3CommonFileSystemConfigBuilder extends FileSystemConfigBuilder {

  private static final String ACCESS_KEY = "accessKey";
  private static final String SECRET_KEY = "secretKey";
  private static final String SESSION_TOKEN = "sessionToken";
  private static final String REGION = "region";
  private static final String CREDENTIALS_FILE = "credentialsFile";
  private static final String PROFILE_NAME = "profileName";
  private static final String ENDPOINT = "endpoint";
  private static final String SIGNATURE_VERSION = "signature_version";
  private static final String PATHSTYLE_ACCESS = "pathSyleAccess";

  private FileSystemOptions fileSystemOptions;

  public S3CommonFileSystemConfigBuilder(FileSystemOptions fileSystemOptions) {
    this.fileSystemOptions = fileSystemOptions;
  }

  public FileSystemOptions getFileSystemOptions() {
    return fileSystemOptions;
  }

  public void setFileSystemOptions(FileSystemOptions fileSystemOptions) {
    this.fileSystemOptions = fileSystemOptions;
  }

  public void setAccessKey(String accessKey) {
    this.setParam(getFileSystemOptions(), ACCESS_KEY, accessKey);
  }

  public String getAccessKey() {
    return (String) this.getParam(getFileSystemOptions(), ACCESS_KEY);
  }

  public void setSecretKey(String secretKey) {
    this.setParam(getFileSystemOptions(), SECRET_KEY, secretKey);
  }

  public String getSecretKey() {
    return (String) this.getParam(getFileSystemOptions(), SECRET_KEY);
  }

  public void setSessionToken(String sessionToken) {
    this.setParam(getFileSystemOptions(), SESSION_TOKEN, sessionToken);
  }

  public String getSessionToken() {
    return (String) this.getParam(getFileSystemOptions(), SESSION_TOKEN);
  }

  public void setRegion(String region) {
    this.setParam(getFileSystemOptions(), REGION, region);
  }

  public String getRegion() {
    return (String) this.getParam(getFileSystemOptions(), REGION);
  }

  public void setCredentialsFile(String credentialsFile) {
    this.setParam(getFileSystemOptions(), CREDENTIALS_FILE, credentialsFile);
  }

  public String getCredentialsFile() {
    return (String) this.getParam(getFileSystemOptions(), CREDENTIALS_FILE);
  }

  public String getProfileName() {
    return (String) this.getParam(getFileSystemOptions(), PROFILE_NAME);
  }

  public void setProfileName(String profileName) {
    this.setParam(getFileSystemOptions(), PROFILE_NAME, profileName);
  }

  public void setEndpoint(String endpoint) {
    this.setParam(getFileSystemOptions(), ENDPOINT, endpoint);
  }

  public String getEndpoint() {
    return (String) this.getParam(getFileSystemOptions(), ENDPOINT);
  }

  public void setSignatureVersion(String signatureVersion) {
    this.setParam(getFileSystemOptions(), SIGNATURE_VERSION, signatureVersion);
  }

  public String getSignatureVersion() {
    return (String) this.getParam(getFileSystemOptions(), SIGNATURE_VERSION);
  }

  public void setPathStyleAccess(String pathStyleAccess) {
    this.setParam(getFileSystemOptions(), PATHSTYLE_ACCESS, pathStyleAccess);
  }

  public String getPathStyleAccess() {
    return (String) this.getParam(getFileSystemOptions(), PATHSTYLE_ACCESS);
  }

  @Override
  protected Class<? extends FileSystem> getConfigClass() {
    return S3NFileSystem.class;
  }
}
