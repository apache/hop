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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.commons.vfs2.provider.ftp.FtpFileProvider;
import org.apache.commons.vfs2.provider.ftps.FtpsFileProvider;
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
import org.apache.hop.vfs.ftp.metadata.FtpConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What the VFS plugins hand to Hop: the fixed schemes, and a provider per named connection which
 * has to match the security mode of that connection.
 */
class FtpVfsPluginTest {

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
  void pointAtAnEmptyMetadataFolder() {
    variables = new Variables();
    variables.setVariable(Const.HOP_METADATA_FOLDER, metadataFolder.toString());
  }

  @Test
  @DisplayName("The plain schemes are ftp and ftps, one plugin each")
  void fixedSchemes() {
    assertArrayEquals(new String[] {"ftp"}, new FtpVfsPlugin().getUrlSchemes());
    assertInstanceOf(FtpFileProvider.class, new FtpVfsPlugin().getProvider());

    assertArrayEquals(new String[] {"ftps"}, new FtpsVfsPlugin().getUrlSchemes());
    assertInstanceOf(FtpsFileProvider.class, new FtpsVfsPlugin().getProvider());
  }

  @Test
  @DisplayName("The FTPS plugin serves no named connections: they all live on the FTP one")
  void ftpsPluginHasNoNamedConnections() {
    assertTrue(new FtpsVfsPlugin().getProviders(variables).isEmpty());
  }

  @Test
  @DisplayName("Without any connections in the metadata there is nothing to register")
  void noConnectionsMeansNoProviders() {
    assertTrue(new FtpVfsPlugin().getProviders(variables).isEmpty());
  }

  @Test
  @DisplayName("Each named connection is registered under its own name")
  void everyConnectionBecomesAScheme() throws Exception {
    save("first", FtpSecurityMode.FTP);
    save("second", FtpSecurityMode.FTP);

    Map<String, FileProvider> providers = new FtpVfsPlugin().getProviders(variables);

    assertEquals(2, providers.size());
    assertTrue(providers.containsKey("first"));
    assertTrue(providers.containsKey("second"));
  }

  @Test
  @DisplayName("A connection gets the provider of its security mode, not of its name")
  void theSecurityModePicksTheProvider() throws Exception {
    save("plain", FtpSecurityMode.FTP);
    save("explicit", FtpSecurityMode.FTPS_EXPLICIT);
    save("implicit", FtpSecurityMode.FTPS_IMPLICIT);

    Map<String, FileProvider> providers = new FtpVfsPlugin().getProviders(variables);

    assertInstanceOf(FtpConnectionFileProvider.class, providers.get("plain"));
    assertInstanceOf(FtpsConnectionFileProvider.class, providers.get("explicit"));
    assertInstanceOf(FtpsConnectionFileProvider.class, providers.get("implicit"));
  }

  @Test
  @DisplayName("Unreadable metadata is logged, not thrown: a bad file can't stop Hop from starting")
  void brokenMetadataDoesNotThrow() throws Exception {
    Path folder = Files.createDirectories(metadataFolder.resolve("ftp-connection"));
    Files.writeString(folder.resolve("broken.json"), "this is not json");

    // Registering happens while the file system manager is being built, long before anybody can
    // be asked what to do about it.
    assertTrue(new FtpVfsPlugin().getProviders(variables).isEmpty());
  }

  private IHopMetadataProvider provider() {
    return HopMetadataUtil.getStandardHopMetadataProvider(variables);
  }

  private void save(String name, FtpSecurityMode mode) throws Exception {
    FtpConnection connection = new FtpConnection();
    connection.setName(name);
    connection.setSecurityMode(mode);
    connection.setServerName("ftp.example.com");
    connection.setUserName("hop");
    connection.setPassword("secret");
    provider().getSerializer(FtpConnection.class).save(connection);
  }
}
