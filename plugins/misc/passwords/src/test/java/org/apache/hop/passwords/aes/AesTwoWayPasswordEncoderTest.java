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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class AesTwoWayPasswordEncoderTest {

  private ITwoWayPasswordEncoder encoder;

  private static final String[] TEST_PASSWORDS = {
    "MySillyButGoodPassword!",
    "",
    null,
    "abcd",
    "${DB_PASSWORD}"
  };

  @Before
  public void setup() throws Exception {
    System.setProperty( Const.HOP_PASSWORD_ENCODER_PLUGIN, "AES" );
    System.setProperty( AesTwoWayPasswordEncoder.VARIABLE_HOP_AES_ENCODER_KEY, "<TheKeyForTheseTestsHere!!>" );
    HopEnvironment.init();
    encoder = Encr.getEncoder();
  }

  @Test
  public void testInit() {
    // See if we've picked up the plugin
    //
    assertEquals( AesTwoWayPasswordEncoder.class, encoder.getClass() );
  }

  @Test
  public void testEncodeDecode() {

    for (String password : TEST_PASSWORDS ) {
      String encoded = Encr.encryptPassword(password);
      String decoded = Encr.decryptPassword( encoded );
      assertEquals( password, decoded );
    }
  }

  @Test
  public void testEncodeDecodeWithPrefixes() {
    for (String password : TEST_PASSWORDS) {
      // This is used in Hop
      //
      String encoded = Encr.encryptPasswordIfNotUsingVariables(password);
      String decoded = Encr.decryptPasswordOptionallyEncrypted( encoded );
      assertEquals( password, decoded );

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
  public void testEncodingInMetadata() throws HopException {

    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    ITwoWayPasswordEncoder twoWayPasswordEncoder = metadataProvider.getTwoWayPasswordEncoder();
    assertNotNull( twoWayPasswordEncoder );
    assertEquals( AesTwoWayPasswordEncoder.class, twoWayPasswordEncoder.getClass() );

    // Store something in there...
    //
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName( "test" );
    databaseMeta.setDatabaseType( "Generic" );
    databaseMeta.setUsername( "user" );
    databaseMeta.setPassword( "password" );

    IHopMetadataSerializer<DatabaseMeta> serializer = metadataProvider.getSerializer( DatabaseMeta.class );
    serializer.save( databaseMeta );

    String json = new SerializableMetadataProvider(metadataProvider).toJson();

    assertTrue(json.contains("\"password\":\"AES 86lpAqp+Xpa\\/zp6m3SYcFQ==\""));

    databaseMeta.setPassword( "${DB_PASSWORD}" );
    serializer.save( databaseMeta );

    json = new SerializableMetadataProvider(metadataProvider).toJson();
    assertTrue(json.contains("\"password\":\"${DB_PASSWORD}\""));
  }

  @Test
  public void testDifferentKeyDifferentEncoding() throws Exception {
    String password = "My Password";

    String encoded1 = Encr.encryptPasswordIfNotUsingVariables( password );
    String decoded1 = Encr.decryptPasswordOptionallyEncrypted( password );

    assertEquals( password, decoded1 );

    // Change the Key
    System.setProperty( Const.HOP_PASSWORD_ENCODER_PLUGIN, "AES" );
    System.setProperty( AesTwoWayPasswordEncoder.VARIABLE_HOP_AES_ENCODER_KEY, "A completely different key" );
    Encr.init("AES");

    String encoded2 = Encr.encryptPasswordIfNotUsingVariables( password );
    String decoded2 = Encr.decryptPasswordOptionallyEncrypted( password );

    assertEquals(password, decoded2);

    assertNotEquals( encoded1, encoded2 );
  }
}