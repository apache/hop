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
package org.apache.hop.vfs.gs.metadatatype;

import org.apache.hop.core.util.TranslateUtil;

public enum GoogleStorageCredentialsType {
  KEY_FILE("i18n::GoogleStorageCredentialsType.KEY_FILE.name"),
  KEY_STRING("i18n::GoogleStorageCredentialsType.KEY_STRING.name");

  private final String credentialType;

  private GoogleStorageCredentialsType(String credentialType) {
    this.credentialType = credentialType;
  }

  @Override
  public String toString() {
    return TranslateUtil.translate(this.credentialType, GoogleStorageCredentialsType.class);
  }

  public static GoogleStorageCredentialsType getEnum(String credentialType) {
    for (GoogleStorageCredentialsType type : GoogleStorageCredentialsType.values()) {
      if (type.toString().equals(credentialType)) {
        return type;
      }
    }
    return null;
  }
}
