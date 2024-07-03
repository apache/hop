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

package org.apache.hop.core.encryption;

import static org.junit.Assert.assertEquals;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Test cases for encryption, to make sure that encrypted password remain the same between versions.
 */
public class EncrTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  /**
   * Test password encryption.
   *
   * @throws HopValueException
   */
  @Test
  public void testEncryptPassword() {
    String encryption;

    encryption = Encr.encryptPassword(null);
    assertEquals("", encryption);

    encryption = Encr.encryptPassword("");
    assertEquals("", encryption);

    encryption = Encr.encryptPassword("     ");
    assertEquals("2be98afc86aa7f2e4cb79ce309ed2ef9a", encryption);

    encryption = Encr.encryptPassword("Test of different encryptions!!@#$%");
    assertEquals(
        "54657374206f6620646966666572656e742067d0fbddb11ad39b8ba50aef31fed1eb9f", encryption);

    encryption = Encr.encryptPassword("  Spaces left");
    assertEquals("2be98afe84af48285a81cbd30d297a9ce", encryption);

    encryption = Encr.encryptPassword("Spaces right");
    assertEquals("2be98afc839d79387ae0aee62d795a7ce", encryption);

    encryption = Encr.encryptPassword("     Spaces  ");
    assertEquals("2be98afe84a87d2c49809af73db81ef9a", encryption);

    encryption = Encr.encryptPassword("1234567890");
    assertEquals("2be98afc86aa7c3d6f84dfb2689caf68a", encryption);
  }

  /**
   * Test password decryption.
   *
   * @throws HopValueException
   */
  @Test
  public void testDecryptPassword() {
    String encryption;
    String decryption;

    encryption = Encr.encryptPassword(null);
    decryption = Encr.decryptPassword(encryption);
    assertEquals("", decryption);

    encryption = Encr.encryptPassword("");
    decryption = Encr.decryptPassword(encryption);
    assertEquals("", decryption);

    encryption = Encr.encryptPassword("     ");
    decryption = Encr.decryptPassword(encryption);
    assertEquals("     ", decryption);

    encryption = Encr.encryptPassword("Test of different encryptions!!@#$%");
    decryption = Encr.decryptPassword(encryption);
    assertEquals("Test of different encryptions!!@#$%", decryption);

    encryption = Encr.encryptPassword("  Spaces left");
    decryption = Encr.decryptPassword(encryption);
    assertEquals("  Spaces left", decryption);

    encryption = Encr.encryptPassword("Spaces right");
    decryption = Encr.decryptPassword(encryption);
    assertEquals("Spaces right", decryption);

    encryption = Encr.encryptPassword("     Spaces  ");
    decryption = Encr.decryptPassword(encryption);
    assertEquals("     Spaces  ", decryption);

    encryption = Encr.encryptPassword("1234567890");
    decryption = Encr.decryptPassword(encryption);
    assertEquals("1234567890", decryption);
  }

  /**
   * Test password encryption (variable style).
   *
   * @throws HopValueException
   */
  @Test
  public void testEncryptPasswordIfNotUsingVariables() {
    String encryption;

    encryption = Encr.encryptPasswordIfNotUsingVariables(null);
    assertEquals("Encrypted ", encryption);

    encryption = Encr.encryptPasswordIfNotUsingVariables("");
    assertEquals("Encrypted ", encryption);

    encryption = Encr.encryptPasswordIfNotUsingVariables("String");
    assertEquals("Encrypted 2be98afc86aa7f2e4cb799d64cc9ba1dd", encryption);

    encryption = Encr.encryptPasswordIfNotUsingVariables(" ${VAR} String");
    assertEquals(" ${VAR} String", encryption);

    encryption = Encr.encryptPasswordIfNotUsingVariables(" %%VAR%% String");
    assertEquals(" %%VAR%% String", encryption);

    encryption = Encr.encryptPasswordIfNotUsingVariables(" %% VAR String");
    assertEquals("Encrypted 2be988fed4f87a4a599599d64cc9ba1dd", encryption);

    encryption = Encr.encryptPasswordIfNotUsingVariables("${%%$$$$");
    assertEquals("Encrypted 2be98afc86aa7f2e4ef02eb359ad6eb9e", encryption);
  }

  /**
   * Test password decryption (variable style).
   *
   * @throws HopValueException
   */
  @Test
  public void testDecryptPasswordIfNotUsingVariables() {
    String encryption;
    String decryption;

    encryption = Encr.encryptPasswordIfNotUsingVariables(null);
    decryption = Encr.decryptPasswordOptionallyEncrypted(encryption);
    assertEquals("", decryption);

    encryption = Encr.encryptPasswordIfNotUsingVariables("");
    decryption = Encr.decryptPasswordOptionallyEncrypted(encryption);
    assertEquals("", decryption);

    encryption = Encr.encryptPasswordIfNotUsingVariables("String");
    decryption = Encr.decryptPasswordOptionallyEncrypted(encryption);
    assertEquals("String", decryption);

    encryption = Encr.encryptPasswordIfNotUsingVariables(" ${VAR} String");
    decryption = Encr.decryptPasswordOptionallyEncrypted(encryption);
    assertEquals(" ${VAR} String", decryption);

    encryption = Encr.encryptPasswordIfNotUsingVariables(" %%VAR%% String");
    decryption = Encr.decryptPasswordOptionallyEncrypted(encryption);
    assertEquals(" %%VAR%% String", decryption);

    encryption = Encr.encryptPasswordIfNotUsingVariables(" %% VAR String");
    decryption = Encr.decryptPasswordOptionallyEncrypted(encryption);
    assertEquals(" %% VAR String", decryption);

    encryption = Encr.encryptPasswordIfNotUsingVariables("${%%$$$$");
    decryption = Encr.decryptPasswordOptionallyEncrypted(encryption);
    assertEquals("${%%$$$$", decryption);
  }
}
