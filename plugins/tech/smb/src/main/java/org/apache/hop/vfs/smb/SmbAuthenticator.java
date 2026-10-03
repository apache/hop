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
package org.apache.hop.vfs.smb;

import com.hierynomus.smbj.SmbConfig;
import com.hierynomus.smbj.auth.AuthenticationContext;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.smb.metadata.SmbConnection;

/**
 * Builds the smbj authentication context for one {@link SmbAuthType}. A future Kerberos
 * implementation returns a {@code GSSAuthenticationContext} from {@link #authenticate} and, if it
 * needs to, registers its authenticator from {@link #configure}.
 */
public interface SmbAuthenticator {

  /**
   * Build the context. Implementations must not log the password or include it in an exception
   * message.
   */
  AuthenticationContext authenticate(SmbConnection connection, IVariables variables);

  /** Adjust the client config before connect. The default leaves the config alone. */
  default void configure(SmbConfig.Builder config) {}
}
