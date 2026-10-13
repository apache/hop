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

package org.apache.hop.vfs.sftp.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A connection has to survive the trip through the JSON files it actually lives in. The in-memory
 * provider the other tests use is not the same thing: it hands the object straight back, so it
 * agrees with whatever the class does.
 */
class SftpConnectionJsonTest {

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

  @Test
  @DisplayName("Every setting survives a save and a load")
  void everySettingRoundTrips() throws Exception {
    SftpConnection connection = connection();
    connection.setUserDirIsRoot(true);
    connection.setCompression("zlib");

    SftpConnection loaded = saveAndLoad(connection);

    assertEquals("mainframe.example.com", loaded.getServerName());
    assertEquals("22", loaded.getServerPort());
    assertEquals("hop", loaded.getUsername());
    assertTrue(loaded.isUserDirIsRoot());
    assertEquals("zlib", loaded.getCompression());
  }

  private SftpConnection connection() {
    SftpConnection connection = new SftpConnection();
    connection.setName("mainframe");
    connection.setServerName("mainframe.example.com");
    connection.setServerPort("22");
    connection.setUsername("hop");
    return connection;
  }

  private SftpConnection saveAndLoad(SftpConnection connection) throws Exception {
    IHopMetadataProvider provider = HopMetadataUtil.getStandardHopMetadataProvider(variables);
    provider.getSerializer(SftpConnection.class).save(connection);
    SftpConnection loaded = provider.getSerializer(SftpConnection.class).load(connection.getName());
    assertNotNull(loaded, "the connection could not be read back");
    return loaded;
  }
}
