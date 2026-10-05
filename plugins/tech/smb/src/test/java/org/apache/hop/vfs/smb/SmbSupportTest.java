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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hierynomus.mssmb2.SMB2Dialect;
import com.hierynomus.smbj.SmbConfig;
import com.hierynomus.smbj.auth.AuthenticationContext;
import java.util.Set;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.smb.metadata.SmbConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SmbSupportTest {

  @BeforeAll
  static void initPasswords() throws HopException {
    Encr.init("Hop");
  }

  @Test
  void pluginHasNoFixedScheme() {
    SmbVfsPlugin plugin = new SmbVfsPlugin();
    assertEquals(0, plugin.getUrlSchemes().length);
    assertEquals(null, plugin.getProvider());
  }

  @Test
  void parsesNamedConnectionUri() throws Exception {
    SmbFileName name =
        (SmbFileName)
            SmbFileNameParser.getInstance()
                .parseUri(null, null, "finance:///reports/2026/daily.csv");
    assertEquals("finance", name.getScheme());
    assertEquals("/reports/2026/daily.csv", name.getPath());
    assertEquals(FileType.FILE, name.getType());
    assertEquals("finance:///", name.getRootURI());
  }

  @Test
  void parsesRelativeUriWithBase() throws Exception {
    SmbFileName base =
        (SmbFileName)
            SmbFileNameParser.getInstance().parseUri(null, null, "finance:///reports/2026");
    SmbFileName child =
        (SmbFileName) SmbFileNameParser.getInstance().parseUri(null, base, "daily.csv");
    assertEquals("finance", child.getScheme());
    assertEquals("/reports/2026/daily.csv", child.getPath());

    SmbFileName sibling =
        (SmbFileName) SmbFileNameParser.getInstance().parseUri(null, base, "../annual.csv");
    assertEquals("finance", sibling.getScheme());
    assertEquals("/reports/annual.csv", sibling.getPath());

    SmbFileName absolute =
        (SmbFileName) SmbFileNameParser.getInstance().parseUri(null, base, "/direct.csv");
    assertEquals("finance", absolute.getScheme());
    assertEquals("/direct.csv", absolute.getPath());

    assertThrows(
        Exception.class,
        () -> SmbFileNameParser.getInstance().parseUri(null, null, "relative.csv"));
  }

  @Test
  void ntlmIdentity() {
    assertEquals(new SmbIdentity("", "alice"), SmbIdentity.parse("", "alice"));
    assertEquals(new SmbIdentity("CORP", "alice"), SmbIdentity.parse("", "CORP\\alice"));
    assertEquals(new SmbIdentity("CORP", "alice"), SmbIdentity.parse("CORP", "OTHER\\alice"));
    assertEquals(
        new SmbIdentity("", "alice@corp.example"), SmbIdentity.parse("", "alice@corp.example"));
  }

  @Test
  void ntlmContextResolvesPasswordVariableAndOmitsItFromErrors() {
    Variables variables = new Variables();
    variables.setVariable("SMB_PASSWORD", "super-secret-value");
    SmbConnection connection = connection();
    connection.setUsername("CORP\\alice");
    connection.setPassword("${SMB_PASSWORD}");
    AuthenticationContext context =
        SmbAuthenticators.forType(SmbAuthType.NTLM).authenticate(connection, variables);
    assertEquals("alice", context.getUsername());
    assertEquals("CORP", context.getDomain());
    assertArrayEquals("super-secret-value".toCharArray(), context.getPassword());
    assertFalse(context.toString().contains("super-secret-value"));

    connection.setUsername("  ");
    IllegalArgumentException missing =
        assertThrows(
            IllegalArgumentException.class,
            () -> SmbAuthenticators.forType(SmbAuthType.NTLM).authenticate(connection, variables));
    assertFalse(missing.getMessage().contains("super-secret-value"));
  }

  @Test
  void guestIgnoresUsernameAndPassword() {
    SmbConnection connection = connection();
    connection.setAuthType(SmbAuthType.GUEST);
    connection.setUsername("alice");
    connection.setPassword("super-secret-value");
    AuthenticationContext context =
        SmbAuthenticators.forType(SmbAuthType.GUEST).authenticate(connection, new Variables());
    assertTrue(context.isGuest());
  }

  @Test
  void nullAuthTypeIsNtlm() {
    SmbConnection connection = connection();
    connection.setUsername("alice");
    connection.setPassword("pw");
    AuthenticationContext context =
        SmbAuthenticators.forType(null).authenticate(connection, new Variables());
    assertEquals("alice", context.getUsername());
  }

  @Test
  void clientConfigUsesDialectFloorAndTimeouts() {
    SmbConnection connection = connection();
    connection.setMinimumDialect(SmbDialect.SMB_3_0);
    connection.setRequireSigning(true);
    connection.setEncryptData(true);
    connection.setDfsEnabled(true);
    connection.setCallTimeoutSeconds("45");
    connection.setSocketTimeoutSeconds("15");
    SmbConfig config = SmbSettings.resolve(connection, new Variables()).toClientConfig();
    assertEquals(
        Set.of(SMB2Dialect.SMB_3_0, SMB2Dialect.SMB_3_0_2, SMB2Dialect.SMB_3_1_1),
        config.getSupportedDialects());
    assertFalse(config.getSupportedDialects().contains(SMB2Dialect.SMB_2_0_2));
    assertFalse(config.getSupportedDialects().contains(SMB2Dialect.UNKNOWN));
    assertTrue(config.isSigningRequired());
    assertTrue(config.isEncryptData());
    assertTrue(config.isDfsEnabled());
    assertEquals(45_000L, config.getReadTimeout());
    assertEquals(45_000L, config.getWriteTimeout());
    assertEquals(15_000, config.getSoTimeout());
  }

  @Test
  void defaultDialectStartsAtSmb2() {
    SmbConfig config = SmbSettings.resolve(connection(), new Variables()).toClientConfig();
    assertTrue(config.getSupportedDialects().contains(SMB2Dialect.SMB_2_0_2));
    assertTrue(config.getSupportedDialects().contains(SMB2Dialect.SMB_3_1_1));
    assertFalse(config.isSigningRequired());
    assertFalse(config.isEncryptData());
    assertFalse(config.isDfsEnabled());
    assertEquals(60_000L, config.getReadTimeout());
    assertEquals(0, config.getSoTimeout());
  }

  @Test
  void socketTimeoutZeroLeavesTheReaderWaiting() {
    Variables variables = new Variables();
    SmbConnection connection = connection();
    connection.setSocketTimeoutSeconds("0");
    assertEquals(0, SmbSettings.resolve(connection, variables).toClientConfig().getSoTimeout());

    connection.setSocketTimeoutSeconds("  ");
    assertEquals(0, SmbSettings.resolve(connection, variables).toClientConfig().getSoTimeout());

    connection.setSocketTimeoutSeconds("-1");
    assertThrows(IllegalArgumentException.class, () -> SmbSettings.resolve(connection, variables));
  }

  @Test
  void rejectsBlankHostShareAndNonPositiveTimeout() {
    Variables variables = new Variables();
    SmbConnection connection = connection();
    connection.setHostname(" ");
    assertThrows(IllegalArgumentException.class, () -> SmbSettings.resolve(connection, variables));
    connection.setHostname("files");
    connection.setShare("");
    assertThrows(IllegalArgumentException.class, () -> SmbSettings.resolve(connection, variables));
    connection.setShare("data");
    connection.setPort("0");
    IllegalArgumentException port =
        assertThrows(
            IllegalArgumentException.class, () -> SmbSettings.resolve(connection, variables));
    assertFalse(port.getMessage().contains("super-secret-value"));
  }

  @Test
  void sharePathStaysUnderTheBaseFolder() throws Exception {
    assertEquals("reports\\daily.csv", SmbPaths.sharePath("", "/reports/daily.csv"));
    assertEquals(
        "restricted\\reports\\daily.csv", SmbPaths.sharePath("restricted", "/reports/daily.csv"));
    assertEquals("restricted", SmbPaths.sharePath("/restricted/", "/"));
    assertThrows(Exception.class, () -> SmbPaths.sharePath("", "/../outside.txt"));
    assertThrows(Exception.class, () -> SmbPaths.sharePath("restricted", "/../../outside.txt"));
  }

  private static SmbConnection connection() {
    SmbConnection connection = new SmbConnection();
    connection.setName("finance");
    connection.setHostname("files.example");
    connection.setShare("data");
    connection.setUsername("alice");
    connection.setPassword("pw");
    return connection;
  }
}
