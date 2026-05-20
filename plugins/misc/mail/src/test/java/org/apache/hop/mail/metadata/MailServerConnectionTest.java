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
package org.apache.hop.mail.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.mail.common.MailConst;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class MailServerConnectionTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void newConnectionHasSecureDefaults() {
    MailServerConnection connection = new MailServerConnection();

    assertTrue(connection.isUseAuthentication(), "auth should default on for production servers");
    assertTrue(connection.isUseSecureAuthentication(), "SSL/TLS should default on");
    assertEquals(MailConst.SSL_TLS, connection.getSecureConnectionType());
    assertTrue(connection.isCheckServerIdentity(), "server identity check should default on");
    assertFalse(connection.isUseProxy());
    assertFalse(connection.isUseXOAuth2());
  }

  @Test
  void roundTripPreservesAllPersistedFields() throws Exception {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    IHopMetadataSerializer<MailServerConnection> serializer =
        provider.getSerializer(MailServerConnection.class);

    MailServerConnection saved = new MailServerConnection();
    saved.setName("prod-imap");
    saved.setProtocol(MailConst.PROTOCOL_STRING_IMAP);
    saved.setServerHost("imap.example.com");
    saved.setServerPort("993");
    saved.setUseAuthentication(true);
    saved.setUsername("alice");
    saved.setPassword("secret");
    saved.setUseXOAuth2(false);
    saved.setUseSecureAuthentication(true);
    saved.setSecureConnectionType(MailConst.SSL_TLS_12);
    saved.setCheckServerIdentity(false);
    saved.setTrustedHosts("imap.example.com,backup.example.com");
    saved.setUseProxy(true);
    saved.setProxyUsername("proxyuser");

    serializer.save(saved);

    MailServerConnection loaded = serializer.load("prod-imap");
    assertEquals("prod-imap", loaded.getName());
    assertEquals(MailConst.PROTOCOL_STRING_IMAP, loaded.getProtocol());
    assertEquals("imap.example.com", loaded.getServerHost());
    assertEquals("993", loaded.getServerPort());
    assertTrue(loaded.isUseAuthentication());
    assertEquals("alice", loaded.getUsername());
    assertFalse(loaded.isUseXOAuth2());
    assertTrue(loaded.isUseSecureAuthentication());
    assertEquals(MailConst.SSL_TLS_12, loaded.getSecureConnectionType());
    assertFalse(
        loaded.isCheckServerIdentity(), "user choice for identity check must survive round-trip");
    assertEquals("imap.example.com,backup.example.com", loaded.getTrustedHosts());
    assertTrue(loaded.isUseProxy());
    assertEquals("proxyuser", loaded.getProxyUsername());
  }

  @Test
  void getSessionUsesLowercaseCheckServerIdentityKeyForImapPop3() {
    MailServerConnection connection = new MailServerConnection();
    connection.setProtocol(MailConst.PROTOCOL_STRING_IMAP);
    connection.setServerHost("imap.example.com");
    connection.setServerPort("993");
    connection.setUseSecureAuthentication(true);
    connection.setSecureConnectionType(MailConst.SSL_TLS);
    connection.setCheckServerIdentity(false);

    connection.getSession(new Variables());
    Properties props = connection.getProps();

    // Angus Mail reads the lowercase "checkserveridentity" — camelCase is silently ignored.
    for (String proto : new String[] {"imap", "imaps", "pop3", "pop3s"}) {
      assertEquals(
          "false",
          props.getProperty("mail." + proto + ".ssl.checkserveridentity"),
          "checkserveridentity must be set with lowercase key for " + proto);
      assertNull(
          props.getProperty("mail." + proto + ".ssl.checkServerIdentity"),
          "camelCase key must not be used for " + proto);
    }
  }

  @Test
  void getSessionUsesLowercaseCheckServerIdentityKeyForSmtp() {
    MailServerConnection connection = new MailServerConnection();
    connection.setProtocol(MailConst.PROTOCOL_SMTP);
    connection.setServerHost("smtp.example.com");
    connection.setServerPort("587");
    connection.setUseSecureAuthentication(true);
    connection.setSecureConnectionType(MailConst.SSL_TLS);
    connection.setCheckServerIdentity(true);

    connection.getSession(new Variables());
    Properties props = connection.getProps();

    assertEquals("true", props.getProperty("mail.smtp.ssl.checkserveridentity"));
    assertNull(props.getProperty("mail.smtp.ssl.checkServerIdentity"));
  }

  @Test
  void getSessionPropagatesTrustedHostsToAllProtocols() {
    MailServerConnection connection = new MailServerConnection();
    connection.setProtocol(MailConst.PROTOCOL_STRING_POP3);
    connection.setServerHost("pop3.example.com");
    connection.setServerPort("995");
    connection.setUseSecureAuthentication(true);
    connection.setSecureConnectionType(MailConst.SSL_TLS);
    connection.setTrustedHosts("pop3.example.com");

    connection.getSession(new Variables());
    Properties props = connection.getProps();

    for (String proto : new String[] {"imap", "imaps", "pop3", "pop3s"}) {
      assertEquals(
          "pop3.example.com",
          props.getProperty("mail." + proto + ".ssl.trust"),
          "trust must be set for " + proto);
    }
  }

  @Test
  void getSessionWiresProxyAsSaslAuthorizationId() {
    MailServerConnection connection = new MailServerConnection();
    connection.setProtocol(MailConst.PROTOCOL_STRING_IMAP);
    connection.setServerHost("imap.example.com");
    connection.setServerPort("993");
    connection.setUseProxy(true);
    connection.setProxyUsername("authzid");

    connection.getSession(new Variables());
    Properties props = connection.getProps();

    assertEquals("true", props.getProperty("mail.imap.sasl.enable"));
    assertEquals("authzid", props.getProperty("mail.imap.sasl.authorizationid"));
  }

  @Test
  void getSessionEnablesStartTlsForSmtpWithTls() {
    MailServerConnection connection = new MailServerConnection();
    connection.setProtocol(MailConst.PROTOCOL_SMTP);
    connection.setServerHost("smtp.example.com");
    connection.setServerPort("587");
    connection.setUseSecureAuthentication(true);
    connection.setSecureConnectionType(MailConst.SSL_TLS);

    connection.getSession(new Variables());
    Properties props = connection.getProps();

    assertEquals("true", props.getProperty("mail.smtp.starttls.enable"));
  }

  @Test
  void getSessionPinsTlsV12WhenRequested() {
    MailServerConnection connection = new MailServerConnection();
    connection.setProtocol(MailConst.PROTOCOL_SMTP);
    connection.setServerHost("smtp.example.com");
    connection.setServerPort("587");
    connection.setUseSecureAuthentication(true);
    connection.setSecureConnectionType(MailConst.SSL_TLS_12);

    connection.getSession(new Variables());
    Properties props = connection.getProps();

    assertEquals("true", props.getProperty("mail.smtp.starttls.enable"));
    assertEquals(MailConst.SSL_TLS_V12, props.getProperty("mail.smtp.ssl.protocols"));
  }
}
