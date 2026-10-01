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
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.vfs.smb.metadata.SmbConnection;

/** Opens the share root. Used by the Test button on the connection editor. */
public final class SmbConnectionTester {
  private static final Class<?> PKG = SmbConnectionTester.class;

  private SmbConnectionTester() {}

  public static String test(IVariables variables, SmbConnection connection) throws Exception {
    SmbSettings settings = SmbSettings.resolve(connection, variables);
    AuthenticationContext context =
        SmbAuthenticators.forType(settings.authType()).authenticate(connection, variables);
    String domain = context.getDomain() == null ? "" : context.getDomain();
    String username = context.getUsername() == null ? "" : context.getUsername();
    try (SmbShare share =
        new SmbjSmbShare(
            settings.toClientConfig(),
            settings.host(),
            settings.port(),
            settings.share(),
            domain,
            username,
            context)) {
      share.children("");
    }
    return BaseMessages.getString(
        PKG, "Smb.Test.Ok", settings.host(), Integer.toString(settings.port()), settings.share());
  }
}
