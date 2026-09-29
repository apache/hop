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
 */
package org.apache.hop.vfs.git;

/**
 * How a git connection proves who it is to the server.
 *
 * <p>Stored under the name of the constant rather than its code: the JSON serializer of the
 * metadata writes {@code Enum.name()} and reads it back with {@code Enum.valueOf}, and honours no
 * {@code storeWithCode} the way the XML one does.
 */
public enum GitAuthType {
  /** A public repository over {@code git://} or an anonymous HTTP clone. */
  NONE("None"),
  /** HTTP basic authentication: a user name with a password or a personal access token. */
  USERNAME_PASSWORD("User name and password"),
  /** An SSH deploy key: a private key file, optionally with a passphrase. */
  DEPLOY_KEY("SSH deploy key");

  private final String description;

  GitAuthType(String description) {
    this.description = description;
  }

  public String getDescription() {
    return description;
  }

  /** Never null: a connection saved before this enum existed means no authentication. */
  public static GitAuthType lookupDescription(String description) {
    if (description != null) {
      for (GitAuthType type : values()) {
        if (type.description.equals(description) || type.name().equalsIgnoreCase(description)) {
          return type;
        }
      }
    }
    return NONE;
  }

  /** The descriptions of every option, in order, for a combo box. */
  public static String[] getDescriptions() {
    GitAuthType[] types = values();
    String[] descriptions = new String[types.length];
    for (int i = 0; i < types.length; i++) {
      descriptions[i] = types[i].description;
    }
    return descriptions;
  }
}
