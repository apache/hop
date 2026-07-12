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
 *
 */

package org.apache.hop.passwords.aes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AesTwoWayPasswordEncoderTest {

  private ITwoWayPasswordEncoder encoder;

  private static final String TEST_AES_KEY = "<TheKeyForTheseTestsHere!!>";

  private static final String[] TEST_PASSWORDS = {
    "MySillyButGoodPassword!", "", null, "abcd", "${DB_PASSWORD}"
  };

  @BeforeEach
  void setup() throws Exception {
    System.setProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN, "AES");
    System.setProperty(AesTwoWayPasswordEncoder.VARIABLE_HOP_AES_ENCODER_KEY, TEST_AES_KEY);
    System.clearProperty(Const.HOP_AES_ENCODER_KEY_FILE);
    HopEnvironment.init();
    // HopEnvironment.init() only runs once; re-bind the encoder for each test.
    Encr.init("AES");
    encoder = Encr.getEncoder();
  }

  @AfterEach
  void tearDown() throws Exception {
    // Restore a default Hop encoder so other tests in the same Surefire fork are not polluted.
    System.setProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN, "Hop");
    System.clearProperty(Const.HOP_AES_ENCODER_KEY);
    System.clearProperty(Const.HOP_AES_ENCODER_KEY_FILE);
    Encr.init("Hop");
  }

  @Test
  void testInit() {
    // See if we've picked up the plugin
    //
    assertEquals(AesTwoWayPasswordEncoder.class, encoder.getClass());
  }

  @Test
  void testEncodeDecode() {

    for (String password : TEST_PASSWORDS) {
      String encoded = Encr.encryptPassword(password);
      String decoded = Encr.decryptPassword(encoded);
      assertEquals(password, decoded);
    }
  }

  @Test
  void testEncodeDecodeWithPrefixes() {
    for (String password : TEST_PASSWORDS) {
      // This is used in Hop
      //
      String encoded = Encr.encryptPasswordIfNotUsingVariables(password);
      String decoded = Encr.decryptPasswordOptionallyEncrypted(encoded);
      assertEquals(password, decoded);

      if (StringUtils.isNotEmpty(password)) {
        if (password.contains("${")) {
          // If the password is a variable, keep it a variable...
          //
          assertEquals(password, encoded);
        } else {
          // The other encrypted passwords should have an AES prefix...
          //
          assertTrue(encoded.startsWith(AesTwoWayPasswordEncoder.AES_PREFIX));
        }
      }
    }
  }

  @Test
  void testEncodingInMetadata() throws HopException {

    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    ITwoWayPasswordEncoder twoWayPasswordEncoder = metadataProvider.getTwoWayPasswordEncoder();
    assertNotNull(twoWayPasswordEncoder);
    assertEquals(AesTwoWayPasswordEncoder.class, twoWayPasswordEncoder.getClass());

    // Store something in there...
    //
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName("test");
    databaseMeta.setDatabaseType("None");
    databaseMeta.setUsername("user");
    databaseMeta.setPassword("password");

    IHopMetadataSerializer<DatabaseMeta> serializer =
        metadataProvider.getSerializer(DatabaseMeta.class);
    serializer.save(databaseMeta);

    String json = new SerializableMetadataProvider(metadataProvider).toJson();

    assertTrue(json.contains("\"password\":\"AES 86lpAqp+Xpa\\/zp6m3SYcFQ==\""));

    databaseMeta.setPassword("${DB_PASSWORD}");
    serializer.save(databaseMeta);

    json = new SerializableMetadataProvider(metadataProvider).toJson();
    assertTrue(json.contains("\"password\":\"${DB_PASSWORD}\""));
  }

  @Test
  void testDifferentKeyDifferentEncoding() throws Exception {
    String password = "My Password";

    String encoded1 = Encr.encryptPasswordIfNotUsingVariables(password);
    String decoded1 = Encr.decryptPasswordOptionallyEncrypted(encoded1);

    assertEquals(password, decoded1);

    // Change the Key
    System.setProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN, "AES");
    System.setProperty(
        AesTwoWayPasswordEncoder.VARIABLE_HOP_AES_ENCODER_KEY, "A completely different key");
    Encr.init("AES");

    String encoded2 = Encr.encryptPasswordIfNotUsingVariables(password);
    String decoded2 = Encr.decryptPasswordOptionallyEncrypted(encoded2);

    assertEquals(password, decoded2);

    assertNotEquals(encoded1, encoded2);
  }

  @Test
  void testInitFromVariablesWithKey() throws Exception {
    IVariables variables = new Variables();
    variables.setVariable(Const.HOP_PASSWORD_ENCODER_PLUGIN, "AES");
    variables.setVariable(Const.HOP_AES_ENCODER_KEY, "VariableSpaceKeyForAesTests!!");
    // Ensure system property key is different so we prove variables win
    System.setProperty(Const.HOP_AES_ENCODER_KEY, "system-property-key-should-not-win");

    Encr.initFromVariables(variables);
    assertEquals(AesTwoWayPasswordEncoder.class, Encr.getEncoder().getClass());

    String password = "VariableKeyPassword";
    String encoded = Encr.encryptPasswordIfNotUsingVariables(password);
    assertTrue(encoded.startsWith(AesTwoWayPasswordEncoder.AES_PREFIX));
    assertEquals(password, Encr.decryptPasswordOptionallyEncrypted(encoded));
  }

  @Test
  void testInitFromKeyFile() throws Exception {
    Path keyFile = Files.createTempFile("hop-aes-key-", ".txt");
    try {
      Files.writeString(keyFile, "KeyFromFileForAesTests!!\n");
      IVariables variables = new Variables();
      variables.setVariable(Const.HOP_PASSWORD_ENCODER_PLUGIN, "AES");
      // Prefer key file when key var empty: clear variable/system key for this test
      System.clearProperty(Const.HOP_AES_ENCODER_KEY);
      variables.setVariable(Const.HOP_AES_ENCODER_KEY_FILE, keyFile.toAbsolutePath().toString());

      Encr.initFromVariables(variables);
      assertEquals(AesTwoWayPasswordEncoder.class, Encr.getEncoder().getClass());

      String password = "FileKeyPassword";
      String encoded = Encr.encryptPasswordIfNotUsingVariables(password);
      assertEquals(password, Encr.decryptPasswordOptionallyEncrypted(encoded));
    } finally {
      Files.deleteIfExists(keyFile);
    }
  }

  @Test
  void testReinitToHopObfuscation() throws Exception {
    String aesEncoded = Encr.encryptPasswordIfNotUsingVariables("OnlyValidWithAes");
    assertTrue(aesEncoded.startsWith(AesTwoWayPasswordEncoder.AES_PREFIX));

    IVariables variables = new Variables();
    variables.setVariable(Const.HOP_PASSWORD_ENCODER_PLUGIN, "Hop");
    Encr.initFromVariables(variables);

    assertEquals(HopTwoWayPasswordEncoder.class, Encr.getEncoder().getClass());
    String hopEncoded = Encr.encryptPasswordIfNotUsingVariables("hop-pass");
    assertTrue(hopEncoded.startsWith(Encr.PASSWORD_ENCRYPTED_PREFIX));
    assertEquals("hop-pass", Encr.decryptPasswordOptionallyEncrypted(hopEncoded));
  }
}
