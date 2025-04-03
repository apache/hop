/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.vfs.gs;

import com.google.auth.oauth2.GoogleCredentials;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemOptions;

public class GoogleStorageFileSystemConfigBuilder extends FileSystemConfigBuilder {

  private static final String GOOGLE_CREDENTIALS = "googleCredentials";
  private static final GoogleStorageFileSystemConfigBuilder builder =
      new GoogleStorageFileSystemConfigBuilder();

  public static GoogleStorageFileSystemConfigBuilder getInstance() {
    return builder;
  }

  private GoogleStorageFileSystemConfigBuilder() {}

  public void setGoogleCredentials(FileSystemOptions opts, GoogleCredentials credentials) {
    setParam(opts, GOOGLE_CREDENTIALS, credentials);
  }

  public GoogleCredentials getGoogleCredentials(FileSystemOptions opts) {
    return (GoogleCredentials) getParam(opts, GOOGLE_CREDENTIALS);
  }

  public void setSchema(FileSystemOptions opts, String schema) {
    setParam(opts, "Schema", schema);
  }

  public String getSchema(FileSystemOptions opts) {
    return (String) getParam(opts, "Schema");
  }

  @Override
  protected Class<? extends FileSystem> getConfigClass() {
    return GoogleStorageFileSystem.class;
  }
}
