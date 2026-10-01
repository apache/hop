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

package org.apache.hop.vfs.ftp;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.time.Duration;
import javax.net.ssl.TrustManager;
import org.apache.commons.lang3.Range;
import org.apache.commons.net.util.TrustManagerUtils;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftp.FtpFileType;
import org.apache.commons.vfs2.provider.ftps.FtpsDataChannelProtectionLevel;
import org.apache.commons.vfs2.provider.ftps.FtpsFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftps.FtpsMode;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.ftp.metadata.FtpConnection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** What a {@link FtpConnection} turns into for the Commons VFS providers. */
class FtpConnectionOptionsTest {

  private final IVariables variables = new Variables();
  private final FtpFileSystemConfigBuilder config = FtpFileSystemConfigBuilder.getInstance();

  @Test
  @DisplayName("A fresh connection is passive and binary")
  void defaultsArePassiveAndBinary() throws Exception {
    FileSystemOptions options = FtpConnectionOptions.build(variables, new FtpConnection(), null);

    assertEquals(Boolean.TRUE, config.getPassiveMode(options));
    assertEquals(FtpFileType.BINARY, config.getFileType(options));
    assertEquals(Boolean.TRUE, config.getUserDirIsRoot(options));
    assertEquals(Boolean.TRUE, config.getRemoteVerification(options));
  }

  @Test
  @DisplayName("An active connection is not passive")
  void activeConnectionTurnsOffPassiveMode() throws Exception {
    FtpConnection connection = new FtpConnection();
    connection.setActiveConnection(true);
    connection.setBinaryMode(false);

    FileSystemOptions options = FtpConnectionOptions.build(variables, connection, null);

    assertEquals(Boolean.FALSE, config.getPassiveMode(options));
    assertEquals(FtpFileType.ASCII, config.getFileType(options));
  }

  @Test
  @DisplayName("The timeouts are read as milliseconds, and variables in them are resolved")
  void timeoutsAreResolved() throws Exception {
    IVariables vars = new Variables();
    vars.setVariable("CONNECT_MS", "1500");

    FtpConnection connection = new FtpConnection();
    connection.setConnectTimeout("${CONNECT_MS}");
    connection.setSocketTimeout("2500");
    connection.setDataTimeout("3500");

    FileSystemOptions options = FtpConnectionOptions.build(vars, connection, null);

    assertEquals(Duration.ofMillis(1500), config.getConnectTimeoutDuration(options));
    assertEquals(Duration.ofMillis(2500), config.getSoTimeoutDuration(options));
    assertEquals(Duration.ofMillis(3500), config.getDataTimeoutDuration(options));
  }

  @Test
  @DisplayName("Timeouts left empty are not set at all, so the library default applies")
  void emptyTimeoutsAreLeftAlone() throws Exception {
    FileSystemOptions options = FtpConnectionOptions.build(variables, new FtpConnection(), null);

    assertNull(config.getConnectTimeoutDuration(options));
    assertNull(config.getSoTimeoutDuration(options));
    assertNull(config.getDataTimeoutDuration(options));
  }

  @Test
  @DisplayName("The keep alive and the active port range reach the options")
  void keepAliveAndActivePortRange() throws Exception {
    FtpConnection connection = new FtpConnection();
    connection.setControlKeepAliveTimeout("30000");
    connection.setControlKeepAliveReplyTimeout("2000");
    connection.setActivePortRangeFrom("40000");
    connection.setActivePortRangeTo("40100");

    FileSystemOptions options = FtpConnectionOptions.build(variables, connection, null);

    assertEquals(Duration.ofMillis(30000), config.getControlKeepAliveTimeout(options));
    assertEquals(Duration.ofMillis(2000), config.getControlKeepAliveReplyTimeout(options));
    assertEquals(Range.of(40000, 40100), config.getActivePortRange(options));
  }

  @Test
  @DisplayName("A port range which is empty or the wrong way round is left unset")
  void anIncompletePortRangeIsIgnored() throws Exception {
    FtpConnection connection = new FtpConnection();
    connection.setActivePortRangeFrom("40100");
    connection.setActivePortRangeTo("40000");

    assertNull(config.getActivePortRange(FtpConnectionOptions.build(variables, connection, null)));
  }

  @Test
  @DisplayName("Everything describing the listing format reaches the options")
  void listingFormatIsApplied() throws Exception {
    FtpConnection connection = new FtpConnection();
    connection.setEntryParser("UNIX");
    connection.setServerLanguageCode("fr");
    connection.setServerTimeZone("UTC");
    connection.setDefaultDateFormat("d MMM yyyy");
    connection.setRecentDateFormat("d MMM HH:mm");
    connection.setShortMonthNames("jan|fev|mar");
    connection.setAutodetectUtf8(true);
    connection.setMdtmLastModifiedTime(true);

    FileSystemOptions options = FtpConnectionOptions.build(variables, connection, null);

    assertEquals("UNIX", config.getEntryParser(options));
    assertEquals("fr", config.getServerLanguageCode(options));
    assertEquals("UTC", config.getServerTimeZoneId(options));
    assertEquals("d MMM yyyy", config.getDefaultDateFormat(options));
    assertEquals("d MMM HH:mm", config.getRecentDateFormat(options));
    assertArrayEquals(new String[] {"jan", "fev", "mar"}, config.getShortMonthNames(options));
    assertEquals(Boolean.TRUE, config.getAutodetectUtf8(options));
    assertEquals(Boolean.TRUE, config.getMdtmLastModifiedTime(options));
  }

  @Test
  @DisplayName("A socks proxy ends up on the options as a real SOCKS proxy")
  void socksProxyIsApplied() throws Exception {
    FtpConnection connection = new FtpConnection();
    connection.setSocksProxyHost("proxy.example.com");
    connection.setSocksProxyPort("1081");

    Proxy proxy = config.getProxy(FtpConnectionOptions.build(variables, connection, null));

    assertNotNull(proxy);
    assertEquals(Proxy.Type.SOCKS, proxy.type());
    assertEquals("proxy.example.com", ((InetSocketAddress) proxy.address()).getHostString());
    assertEquals(1081, ((InetSocketAddress) proxy.address()).getPort());
  }

  @Test
  @DisplayName("An FTPS connection carries its mode, protection level and trust manager")
  void ftpsSettingsAreApplied() throws Exception {
    FtpConnection connection = new FtpConnection();
    connection.setSecurityMode(FtpSecurityMode.FTPS_IMPLICIT);
    connection.setDataChannelProtection(FtpDataChannelProtection.CLEAR);

    FileSystemOptions options = FtpConnectionOptions.build(variables, connection, null);
    FtpsFileSystemConfigBuilder ftps = FtpsFileSystemConfigBuilder.getInstance();

    assertEquals(FtpsMode.IMPLICIT, ftps.getFtpsMode(options));
    assertEquals(FtpsDataChannelProtectionLevel.C, ftps.getDataChannelProtectionLevel(options));
    assertNotNull(ftps.getTrustManager(options));
  }

  @Test
  @DisplayName("An unset protection level is the private one, not null")
  void anUnsetProtectionLevelIsPrivate() throws Exception {
    FtpConnection connection = new FtpConnection();
    connection.setSecurityMode(FtpSecurityMode.FTPS_EXPLICIT);
    connection.setDataChannelProtection(null);

    FileSystemOptions options = FtpConnectionOptions.build(variables, connection, null);

    assertEquals(
        FtpsDataChannelProtectionLevel.P,
        FtpsFileSystemConfigBuilder.getInstance().getDataChannelProtectionLevel(options));
  }

  @Test
  @DisplayName("Turning off certificate verification swaps in an accept-all trust manager")
  void certificateVerificationPicksTheTrustManager() throws Exception {
    FtpsFileSystemConfigBuilder ftps = FtpsFileSystemConfigBuilder.getInstance();

    FtpConnection verifying = new FtpConnection();
    verifying.setSecurityMode(FtpSecurityMode.FTPS_EXPLICIT);
    TrustManager strict =
        ftps.getTrustManager(FtpConnectionOptions.build(variables, verifying, null));

    FtpConnection trusting = new FtpConnection();
    trusting.setSecurityMode(FtpSecurityMode.FTPS_EXPLICIT);
    trusting.setVerifyServerCertificate(false);
    TrustManager lenient =
        ftps.getTrustManager(FtpConnectionOptions.build(variables, trusting, null));

    // The VFS providers and the actions have to check the server the same way, so both ask
    // FtpClientFactory for the trust manager.
    assertSame(FtpClientFactory.trustManager(true).getClass(), strict.getClass());
    assertSame(
        TrustManagerUtils.getAcceptAllTrustManager(),
        lenient,
        "an unverified connection must not get a validating trust manager");
    assertNotSame(
        strict.getClass(),
        lenient.getClass(),
        "verifying and not verifying must not end up with the same trust manager");
  }

  @Test
  @DisplayName("The options VFS handed us are kept, not thrown away")
  void baseOptionsArePreserved() throws Exception {
    FileSystemOptions base = new FileSystemOptions();
    config.setEntryParser(base, "UNIX");

    FileSystemOptions options = FtpConnectionOptions.build(variables, new FtpConnection(), base);

    assertEquals("UNIX", config.getEntryParser(options));
    assertTrue(options != base, "the base options must not be modified in place");
  }
}
