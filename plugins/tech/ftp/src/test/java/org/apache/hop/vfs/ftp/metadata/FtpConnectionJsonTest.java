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

package org.apache.hop.vfs.ftp.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.vfs.ftp.FtpDataChannelProtection;
import org.apache.hop.vfs.ftp.FtpSecurityMode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A connection has to survive the trip through the JSON files it actually lives in.
 *
 * <p>The in-memory provider the other tests use is not the same thing: it hands the object back
 * without serializing it, so it agrees with whatever the class does. The JSON serializer writes an
 * enum as {@code Enum.name()} and reads it back with {@code Enum.valueOf} - it honours no {@code
 * storeWithCode} - and a mismatch there makes every connection in a project unreadable.
 */
class FtpConnectionJsonTest {

  @TempDir private Path metadataFolder;

  private IVariables variables;

  @BeforeAll
  static void initEncryption() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");
  }

  @BeforeEach
  void pointAtTheMetadataFolder() {
    variables = new Variables();
    variables.setVariable(Const.HOP_METADATA_FOLDER, metadataFolder.toString());
  }

  @ParameterizedTest
  @EnumSource(FtpSecurityMode.class)
  @DisplayName("Every security mode survives a save and a load")
  void everySecurityModeRoundTrips(FtpSecurityMode mode) throws Exception {
    FtpConnection connection = full();
    connection.setSecurityMode(mode);

    assertSame(mode, saveAndLoad(connection).getSecurityMode());
  }

  @ParameterizedTest
  @EnumSource(FtpDataChannelProtection.class)
  @DisplayName("Every data connection choice survives a save and a load")
  void everyProtectionLevelRoundTrips(FtpDataChannelProtection protection) throws Exception {
    FtpConnection connection = full();
    connection.setDataChannelProtection(protection);

    assertSame(protection, saveAndLoad(connection).getDataChannelProtection());
  }

  @Test
  @DisplayName("Every setting survives a save and a load")
  void everySettingRoundTrips() throws Exception {
    FtpConnection loaded = saveAndLoad(full());

    assertEquals("ftp.example.com", loaded.getServerName());
    assertEquals("2121", loaded.getServerPort());
    assertEquals("hop", loaded.getUserName());
    assertEquals("secret", loaded.getPassword());
    assertTrue(loaded.isBinaryMode());
    assertTrue(loaded.isActiveConnection());
    assertEquals("UTF-8", loaded.getControlEncoding());
    assertEquals("1000", loaded.getConnectTimeout());
    assertEquals("2000", loaded.getSocketTimeout());
    assertEquals("3000", loaded.getDataTimeout());
    assertEquals("4000", loaded.getControlKeepAliveTimeout());
    assertEquals("5000", loaded.getControlKeepAliveReplyTimeout());
    assertEquals("40000", loaded.getActivePortRangeFrom());
    assertEquals("40100", loaded.getActivePortRangeTo());
    assertFalse(loaded.isRemoteVerification());
    assertFalse(loaded.isUserDirIsRoot());
    assertTrue(loaded.isAutodetectUtf8());
    assertTrue(loaded.isMdtmLastModifiedTime());
    assertEquals("UNIX", loaded.getEntryParser());
    assertEquals("fr", loaded.getServerLanguageCode());
    assertEquals("UTC", loaded.getServerTimeZone());
    assertEquals("d MMM yyyy", loaded.getDefaultDateFormat());
    assertEquals("d MMM HH:mm", loaded.getRecentDateFormat());
    assertEquals("jan|fev|mar", loaded.getShortMonthNames());
    assertFalse(loaded.isVerifyServerCertificate());
    assertEquals("/keys/client.p12", loaded.getClientCertificateFile());
    assertEquals("keystore-secret", loaded.getClientCertificatePassword());
    assertEquals("the-alias", loaded.getClientCertificateAlias());
    assertEquals("PKCS12", loaded.getClientCertificateType());
    assertEquals("proxy.example.com", loaded.getProxyHost());
    assertEquals("8021", loaded.getProxyPort());
    assertEquals("proxy-user", loaded.getProxyUsername());
    assertEquals("proxy-secret", loaded.getProxyPassword());
    assertEquals("socks.example.com", loaded.getSocksProxyHost());
    assertEquals("1080", loaded.getSocksProxyPort());
    assertEquals("socks-user", loaded.getSocksProxyUsername());
    assertEquals("socks-secret", loaded.getSocksProxyPassword());
  }

  @Test
  @DisplayName("The passwords are encrypted on disk, in plain text nowhere")
  void thePasswordsAreEncryptedOnDisk() throws Exception {
    save(full());

    String json = Files.readString(connectionFile());

    for (String secret :
        new String[] {"secret", "proxy-secret", "socks-secret", "keystore-secret"}) {
      assertFalse(
          json.contains("\"" + secret + "\""), "a password is stored in clear text:\n" + json);
    }
    assertTrue(json.contains("Encrypted "), "the passwords should be stored encrypted");
  }

  @Test
  @DisplayName("A default connection reads back as a default connection")
  void theDefaultsRoundTrip() throws Exception {
    FtpConnection fresh = new FtpConnection();
    fresh.setName("defaults");

    FtpConnection loaded = saveAndLoad(fresh);

    assertSame(FtpSecurityMode.FTP, loaded.getSecurityMode());
    assertSame(FtpDataChannelProtection.PRIVATE, loaded.getDataChannelProtection());
    assertEquals("21", loaded.getServerPort());
    assertTrue(loaded.isBinaryMode());
    assertTrue(loaded.isRemoteVerification());
    assertTrue(loaded.isUserDirIsRoot());
    assertTrue(loaded.isVerifyServerCertificate());
  }

  /**
   * The connections the integration tests ship with are hand written JSON, so nothing but this
   * stops them from drifting away from the class they describe. A wrong enum value in one of them
   * only shows up as a failing docker run, long after the change that caused it.
   */
  @Test
  @DisplayName("The connections of the integration tests still load")
  void theIntegrationTestConnectionsLoad() throws Exception {
    Path folder = integrationTestProject().resolve("metadata/ftp-connection");
    assertTrue(Files.isDirectory(folder), "no FTP connections found at " + folder);

    try (Stream<Path> files = Files.list(folder)) {
      for (Path file : files.filter(f -> f.toString().endsWith(".json")).toList()) {
        Files.copy(
            file,
            Files.createDirectories(metadataFolder.resolve("ftp-connection"))
                .resolve(file.getFileName()));
      }
    }

    var connections = provider().getSerializer(FtpConnection.class).loadAll();

    assertFalse(connections.isEmpty(), "the integration tests ship no FTP connection to check");
    for (FtpConnection connection : connections) {
      assertNotNull(connection.getSecurityMode(), connection.getName() + " has no security mode");
      assertNotNull(
          connection.getDataChannelProtection(),
          connection.getName() + " has no data connection setting");
      assertNotNull(connection.getServerName(), connection.getName() + " has no server name");
      assertFalse(
          connection.getServerName().isEmpty(), connection.getName() + " has no server name");
    }
  }

  /** The integration-test project of this plugin, found by walking up to the repository root. */
  private static Path integrationTestProject() {
    Path directory = Path.of("").toAbsolutePath();
    while (directory != null) {
      Path candidate = directory.resolve("integration-tests/ftp");
      if (Files.isDirectory(candidate)) {
        return candidate;
      }
      directory = directory.getParent();
    }
    throw new IllegalStateException(
        "integration-tests/ftp not found above " + Path.of("").toAbsolutePath());
  }

  // --- helpers ------------------------------------------------------------------------------

  private IHopMetadataProvider provider() {
    return HopMetadataUtil.getStandardHopMetadataProvider(variables);
  }

  private void save(FtpConnection connection) throws Exception {
    provider().getSerializer(FtpConnection.class).save(connection);
  }

  private FtpConnection saveAndLoad(FtpConnection connection) throws Exception {
    save(connection);
    FtpConnection loaded = provider().getSerializer(FtpConnection.class).load(connection.getName());
    assertNotNull(loaded, "the connection could not be read back");
    return loaded;
  }

  private Path connectionFile() throws Exception {
    try (Stream<Path> files = Files.walk(metadataFolder)) {
      return files
          .filter(p -> p.getFileName().toString().endsWith(".json"))
          .findFirst()
          .orElseThrow();
    }
  }

  private FtpConnection full() {
    FtpConnection connection = new FtpConnection();
    connection.setName("everything");
    connection.setDescription("a connection with every setting filled in");
    connection.setSecurityMode(FtpSecurityMode.FTPS_EXPLICIT);
    connection.setServerName("ftp.example.com");
    connection.setServerPort("2121");
    connection.setUserName("hop");
    connection.setPassword("secret");
    connection.setBinaryMode(true);
    connection.setActiveConnection(true);
    connection.setControlEncoding("UTF-8");
    connection.setConnectTimeout("1000");
    connection.setSocketTimeout("2000");
    connection.setDataTimeout("3000");
    connection.setControlKeepAliveTimeout("4000");
    connection.setControlKeepAliveReplyTimeout("5000");
    connection.setActivePortRangeFrom("40000");
    connection.setActivePortRangeTo("40100");
    connection.setRemoteVerification(false);
    connection.setUserDirIsRoot(false);
    connection.setAutodetectUtf8(true);
    connection.setMdtmLastModifiedTime(true);
    connection.setEntryParser("UNIX");
    connection.setServerLanguageCode("fr");
    connection.setServerTimeZone("UTC");
    connection.setDefaultDateFormat("d MMM yyyy");
    connection.setRecentDateFormat("d MMM HH:mm");
    connection.setShortMonthNames("jan|fev|mar");
    connection.setVerifyServerCertificate(false);
    connection.setDataChannelProtection(FtpDataChannelProtection.CLEAR);
    connection.setClientCertificateFile("/keys/client.p12");
    connection.setClientCertificatePassword("keystore-secret");
    connection.setClientCertificateAlias("the-alias");
    connection.setClientCertificateType("PKCS12");
    connection.setProxyHost("proxy.example.com");
    connection.setProxyPort("8021");
    connection.setProxyUsername("proxy-user");
    connection.setProxyPassword("proxy-secret");
    connection.setSocksProxyHost("socks.example.com");
    connection.setSocksProxyPort("1080");
    connection.setSocksProxyUsername("socks-user");
    connection.setSocksProxyPassword("socks-secret");
    return connection;
  }
}
