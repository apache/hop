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

package org.apache.hop.core.database;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * One SSH jump host (bastion) of a database connection's SSH tunnel. The jump hosts of a connection
 * are passed in order: Hop signs in to the first one directly, to every next one through the one
 * before it, and finally to the connection's own SSH server, which forwards the database port. Each
 * jump host has its own credentials.
 */
@Getter
@Setter
public class SshJumpHost {

  /** Host name or IP address, as seen from the previous jump host (or from Hop for the first). */
  @HopMetadataProperty private String host;

  /** SSH port, 22 when empty. */
  @HopMetadataProperty private String port;

  @HopMetadataProperty private String username;

  @HopMetadataProperty private SshAuthenticationMethod authenticationMethod;

  @HopMetadataProperty(password = true)
  private String password;

  @HopMetadataProperty private String privateKeyFile;

  @HopMetadataProperty(password = true)
  private String passphrase;

  public SshJumpHost() {
    authenticationMethod = SshAuthenticationMethod.PASSWORD;
  }

  public SshJumpHost(
      String host,
      String port,
      String username,
      SshAuthenticationMethod authenticationMethod,
      String password,
      String privateKeyFile,
      String passphrase) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.authenticationMethod = authenticationMethod;
    this.password = password;
    this.privateKeyFile = privateKeyFile;
    this.passphrase = passphrase;
  }

  public SshJumpHost(SshJumpHost other) {
    this(
        other.host,
        other.port,
        other.username,
        other.authenticationMethod,
        other.password,
        other.privateKeyFile,
        other.passphrase);
  }
}
