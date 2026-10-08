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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.vfs.smb.metadata.SmbConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SmbConnectionSerializerTest {

  @BeforeAll
  static void initPasswords() throws HopException {
    Encr.init("Hop");
  }

  @Test
  void roundTripEncryptsThePassword(@TempDir Path folder) throws Exception {
    JsonMetadataProvider provider =
        new JsonMetadataProvider(
            new HopTwoWayPasswordEncoder(), folder.toString(), new Variables());
    SmbConnection connection = new SmbConnection();
    connection.setName("finance");
    connection.setDescription("nightly");
    connection.setHostname("files.example");
    connection.setPort("445");
    connection.setShare("data");
    connection.setBasePath("restricted");
    connection.setAuthType(SmbAuthType.NTLM);
    connection.setDomain("CORP");
    connection.setUsername("alice");
    connection.setPassword("super-secret-value");
    connection.setMinimumDialect(SmbDialect.SMB_3_1_1);
    connection.setRequireSigning(true);
    connection.setEncryptData(true);
    connection.setDfsEnabled(true);
    connection.setCallTimeoutSeconds("30");
    connection.setSocketTimeoutSeconds("20");
    provider.getSerializer(SmbConnection.class).save(connection);

    String stored = Files.readString(findJson(folder), StandardCharsets.UTF_8);
    assertFalse(stored.contains("super-secret-value"));

    SmbConnection loaded = provider.getSerializer(SmbConnection.class).load("finance");
    assertEquals("nightly", loaded.getDescription());
    assertEquals("files.example", loaded.getHostname());
    assertEquals("445", loaded.getPort());
    assertEquals("data", loaded.getShare());
    assertEquals("restricted", loaded.getBasePath());
    assertEquals(SmbAuthType.NTLM, loaded.getAuthType());
    assertEquals("CORP", loaded.getDomain());
    assertEquals("alice", loaded.getUsername());
    assertEquals(
        "super-secret-value", Encr.decryptPasswordOptionallyEncrypted(loaded.getPassword()));
    assertEquals(SmbDialect.SMB_3_1_1, loaded.getMinimumDialect());
    assertTrue(loaded.isRequireSigning());
    assertTrue(loaded.isEncryptData());
    assertTrue(loaded.isDfsEnabled());
    assertEquals("30", loaded.getCallTimeoutSeconds());
    assertEquals("20", loaded.getSocketTimeoutSeconds());
  }

  private static Path findJson(Path folder) throws Exception {
    try (var files = Files.walk(folder)) {
      return files.filter(path -> path.toString().endsWith(".json")).findFirst().orElseThrow();
    }
  }
}
