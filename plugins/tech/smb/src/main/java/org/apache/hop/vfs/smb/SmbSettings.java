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
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.vfs.smb.metadata.SmbConnection;

/**
 * Resolved connection settings. The password is not a field here, so logging this object cannot
 * print it.
 */
public record SmbSettings(
    String name,
    String host,
    int port,
    String share,
    String basePath,
    SmbAuthType authType,
    SmbDialect minimumDialect,
    boolean requireSigning,
    boolean encryptData,
    boolean dfsEnabled,
    int callTimeoutSeconds,
    int socketTimeoutSeconds) {

  private static final Class<?> PKG = SmbSettings.class;
  static final int DEFAULT_PORT = 445;
  static final int DEFAULT_TIMEOUT_SECONDS = 60;

  public static SmbSettings resolve(SmbConnection connection, IVariables variables) {
    String name = Const.NVL(connection.getName(), "");
    String host = variables.resolve(Const.NVL(connection.getHostname(), "")).trim();
    if (StringUtils.isBlank(host)) {
      throw new IllegalArgumentException(BaseMessages.getString(PKG, "Smb.Error.Hostname", name));
    }
    String share = variables.resolve(Const.NVL(connection.getShare(), "")).trim();
    if (StringUtils.isBlank(share)) {
      throw new IllegalArgumentException(BaseMessages.getString(PKG, "Smb.Error.Share", name));
    }
    int port = positiveInt(variables.resolve(connection.getPort()), DEFAULT_PORT, name, "port");
    int callTimeout =
        positiveInt(
            variables.resolve(connection.getCallTimeoutSeconds()),
            DEFAULT_TIMEOUT_SECONDS,
            name,
            "call timeout");
    int socketTimeout =
        positiveInt(
            variables.resolve(connection.getSocketTimeoutSeconds()),
            DEFAULT_TIMEOUT_SECONDS,
            name,
            "socket timeout");
    SmbAuthType authType =
        connection.getAuthType() == null ? SmbAuthType.NTLM : connection.getAuthType();
    SmbDialect dialect =
        connection.getMinimumDialect() == null
            ? SmbDialect.SMB_2_0_2
            : connection.getMinimumDialect();
    String basePath = variables.resolve(Const.NVL(connection.getBasePath(), "")).trim();
    return new SmbSettings(
        name,
        host,
        port,
        share,
        basePath,
        authType,
        dialect,
        connection.isRequireSigning(),
        connection.isEncryptData(),
        connection.isDfsEnabled(),
        callTimeout,
        socketTimeout);
  }

  public SmbConfig toClientConfig() {
    SmbConfig.Builder builder =
        SmbConfig.builder()
            .withDialects(minimumDialect.negotiated())
            .withSigningRequired(requireSigning)
            .withEncryptData(encryptData)
            .withDfsEnabled(dfsEnabled)
            .withTimeout(callTimeoutSeconds, TimeUnit.SECONDS)
            .withSoTimeout(socketTimeoutSeconds, TimeUnit.SECONDS);
    SmbAuthenticators.forType(authType).configure(builder);
    return builder.build();
  }

  private static int positiveInt(String raw, int fallback, String name, String field) {
    if (raw == null || raw.isBlank()) {
      return fallback;
    }
    try {
      int value = Integer.parseInt(raw.trim());
      if (value <= 0) {
        throw new NumberFormatException(field);
      }
      return value;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          BaseMessages.getString(PKG, "Smb.Error.PositiveNumber", name, field));
    }
  }
}
