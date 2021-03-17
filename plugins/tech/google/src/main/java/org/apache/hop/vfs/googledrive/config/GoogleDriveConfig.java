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

package org.apache.hop.vfs.googledrive.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

public class GoogleDriveConfig {

  public static final String HOP_CONFIG_GOOGLE_DRIVE_CONFIG_KEY = "googleDrive";

  private String credentialsFile;
  private String tokensFolder;

  public GoogleDriveConfig() {}

  public GoogleDriveConfig(GoogleDriveConfig config) {
    this();
    credentialsFile = config.credentialsFile;
    tokensFolder = config.tokensFolder;
  }

  /**
   * Gets credentialsFile
   *
   * @return value of credentialsFile
   */
  public String getCredentialsFile() {
    return credentialsFile;
  }

  /** @param credentialsFile The credentialsFile to set */
  public void setCredentialsFile(String credentialsFile) {
    this.credentialsFile = credentialsFile;
  }

  /**
   * Gets tokensFolder
   *
   * @return value of tokensFolder
   */
  public String getTokensFolder() {
    return tokensFolder;
  }

  /** @param tokensFolder The tokensFolder to set */
  public void setTokensFolder(String tokensFolder) {
    this.tokensFolder = tokensFolder;
  }
}
