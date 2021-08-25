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

import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemOptions;

public class GoogleStorageFileSystemConfigBuilder extends FileSystemConfigBuilder {

  private static final String JSON_CLIENT_ID_RESOURCE = "jsonClientIdResource";
  private static final GoogleStorageFileSystemConfigBuilder builder =
      new GoogleStorageFileSystemConfigBuilder();

  public static GoogleStorageFileSystemConfigBuilder getInstance() {
    return builder;
  }

  private GoogleStorageFileSystemConfigBuilder() {}

  public void setClientIdJSON(FileSystemOptions opts, String jsonClientIdResource) {
    setParam(opts, JSON_CLIENT_ID_RESOURCE, jsonClientIdResource);
  }

  public String getClientIdJSON(FileSystemOptions opts) {
    return (String) getParam(opts, JSON_CLIENT_ID_RESOURCE);
  }

  @Override
  protected Class<? extends FileSystem> getConfigClass() {
    return GoogleStorageFileSystem.class;
  }
}
