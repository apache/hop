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

import com.hierynomus.smbj.auth.AuthenticationContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.vfs.smb.metadata.SmbConnection;

/** NTLMv2 username and password. smbj negotiates NTLMv2; there is no NTLMv1 switch. */
final class NtlmSmbAuthenticator implements SmbAuthenticator {
  private static final Class<?> PKG = NtlmSmbAuthenticator.class;

  @Override
  public AuthenticationContext authenticate(SmbConnection connection, IVariables variables) {
    SmbIdentity identity =
        SmbIdentity.parse(
            variables.resolve(connection.getDomain()), variables.resolve(connection.getUsername()));
    if (StringUtils.isBlank(identity.username())) {
      throw new IllegalArgumentException(
          BaseMessages.getString(PKG, "Smb.Error.Username", Const.NVL(connection.getName(), "")));
    }
    String password =
        Encr.decryptPasswordOptionallyEncrypted(
            variables.resolve(Const.NVL(connection.getPassword(), "")));
    return new AuthenticationContext(
        identity.username(), password.toCharArray(), identity.domain());
  }
}
