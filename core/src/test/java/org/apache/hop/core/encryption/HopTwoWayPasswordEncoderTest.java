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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

/**
 * Test cases for encryption, to make sure that encrypted password remain the same between versions.
 */
public class HopTwoWayPasswordEncoderTest {

  /** Test password encryption. */
  @Test
  public void testEncode1() {

    HopTwoWayPasswordEncoder encoder = new HopTwoWayPasswordEncoder();

    String encryption;

    encryption = encoder.encode(null, false);
    assertEquals("", encryption);

    encryption = encoder.encode("", false);
    assertEquals("", encryption);

    encryption = encoder.encode("     ", false);
    assertEquals("2be98afc86aa7f2e4cb79ce309ed2ef9a", encryption);

    encryption = encoder.encode("Test of different encryptions!!@#$%", false);
    assertEquals(
        "54657374206f6620646966666572656e742067d0fbddb11ad39b8ba50aef31fed1eb9f", encryption);

    encryption = encoder.encode("  Spaces left", false);
    assertEquals("2be98afe84af48285a81cbd30d297a9ce", encryption);

    encryption = encoder.encode("Spaces right", false);
    assertEquals("2be98afc839d79387ae0aee62d795a7ce", encryption);

    encryption = encoder.encode("     Spaces  ", false);
    assertEquals("2be98afe84a87d2c49809af73db81ef9a", encryption);

    encryption = encoder.encode("1234567890", false);
    assertEquals("2be98afc86aa7c3d6f84dfb2689caf68a", encryption);
  }

  /** Test password decryption. */
  @Test
  public void testDecode1() {
    HopTwoWayPasswordEncoder encoder = new HopTwoWayPasswordEncoder();

    String encryption;
    String decryption;

    encryption = encoder.encode(null);
    decryption = encoder.decode(encryption);
    assertEquals("", decryption);

    encryption = encoder.encode("");
    decryption = encoder.decode(encryption);
    assertEquals("", decryption);

    encryption = encoder.encode("     ");
    decryption = encoder.decode(encryption);
    assertEquals("     ", decryption);

    encryption = encoder.encode("Test of different encryptions!!@#$%");
    decryption = encoder.decode(encryption);
    assertEquals("Test of different encryptions!!@#$%", decryption);

    encryption = encoder.encode("  Spaces left");
    decryption = encoder.decode(encryption);
    assertEquals("  Spaces left", decryption);

    encryption = encoder.encode("Spaces right");
    decryption = encoder.decode(encryption);
    assertEquals("Spaces right", decryption);

    encryption = encoder.encode("     Spaces  ");
    decryption = encoder.decode(encryption);
    assertEquals("     Spaces  ", decryption);

    encryption = encoder.encode("1234567890");
    decryption = encoder.decode(encryption);
    assertEquals("1234567890", decryption);

    assertEquals("", encoder.decode(null));
  }

  /** Test password encryption (variable style). */
  @Test
  public void testEncode2() {
    HopTwoWayPasswordEncoder encoder = new HopTwoWayPasswordEncoder();

    String encryption;

    encryption = encoder.encode(null);
    assertEquals("Encrypted ", encryption);

    encryption = encoder.encode("");
    assertEquals("Encrypted ", encryption);

    encryption = encoder.encode("String");
    assertEquals("Encrypted 2be98afc86aa7f2e4cb799d64cc9ba1dd", encryption);

    encryption = encoder.encode(" ${VAR} String");
    assertEquals(" ${VAR} String", encryption);

    encryption = encoder.encode(" %%VAR%% String");
    assertEquals(" %%VAR%% String", encryption);

    encryption = encoder.encode(" %% VAR String");
    assertEquals("Encrypted 2be988fed4f87a4a599599d64cc9ba1dd", encryption);

    encryption = encoder.encode("${%%$$$$");
    assertEquals("Encrypted 2be98afc86aa7f2e4ef02eb359ad6eb9e", encryption);
  }

  /** Test password decryption (variable style). */
  @Test
  public void testDecode2() {
    HopTwoWayPasswordEncoder encoder = new HopTwoWayPasswordEncoder();

    String encryption;
    String decryption;

    encryption = encoder.encode(null);
    decryption = encoder.decode(encryption);
    assertEquals("", decryption);

    encryption = encoder.encode("");
    decryption = encoder.decode(encryption);
    assertEquals("", decryption);

    encryption = encoder.encode("String");
    decryption = encoder.decode(encryption);
    assertEquals("String", decryption);

    encryption = encoder.encode(" ${VAR} String", false);
    decryption = encoder.decode(encryption);
    assertEquals(" ${VAR} String", decryption);

    encryption = encoder.encode(" %%VAR%% String", false);
    decryption = encoder.decode(encryption);
    assertEquals(" %%VAR%% String", decryption);

    encryption = encoder.encode(" %% VAR String", false);
    decryption = encoder.decode(encryption);
    assertEquals(" %% VAR String", decryption);

    encryption = encoder.encode("${%%$$$$", false);
    decryption = encoder.decode(encryption);
    assertEquals("${%%$$$$", decryption);
  }

  @Test
  public void testEncodeDifferentSeed() {

    HopTwoWayPasswordEncoder encoder = new HopTwoWayPasswordEncoder();
    String encodeWithDefaultSeed = encoder.encode("Wibble", false);
    assertNotNull(encodeWithDefaultSeed);
    String decodeWithDefaultSeed = encoder.decode(encodeWithDefaultSeed);
    assertNotNull(decodeWithDefaultSeed);

    TestHopTwoWayPasswordEncoder encoder2 = new TestHopTwoWayPasswordEncoder();

    String encodeWithNondefaultSeed = encoder2.encode("Wibble", false);
    assertNotNull(encodeWithNondefaultSeed);
    String decodeWithNondefaultSeed = encoder2.decode(encodeWithNondefaultSeed);
    assertNotNull(decodeWithNondefaultSeed);

    assertNotEquals(
        encodeWithDefaultSeed,
        encodeWithNondefaultSeed); // Make sure that if the seed changes, so does the the
    // encoded value
    assertEquals(
        decodeWithDefaultSeed,
        decodeWithNondefaultSeed); // Make sure that the decode from either is correct.
  }

  private class TestHopTwoWayPasswordEncoder extends HopTwoWayPasswordEncoder {

    public TestHopTwoWayPasswordEncoder() {
      super();
    }

    @Override
    protected String getSeed() {
      return "123456789012345435987";
    }
  }
}
