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

package org.apache.hop.vfs.azure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.vfs.azure.metadatatype.AzureMetadataType;
import org.junit.jupiter.api.Test;

/** Tests for the SAS token authentication option of the Azure VFS plugin. */
class AzureSasTokenAuthenticationTest {

  @Test
  void sasTokenIsStoredOnTheConnectionMetadata() {
    AzureMetadataType metadataType = new AzureMetadataType();
    metadataType.setAuthenticationType("SAS Token");
    metadataType.setSasToken("sv=2022-11-02&ss=b&srt=co&sp=rl&se=2026-12-31T00:00:00Z&sig=abc123");

    assertEquals("SAS Token", metadataType.getAuthenticationType());
    assertEquals(
        "sv=2022-11-02&ss=b&srt=co&sp=rl&se=2026-12-31T00:00:00Z&sig=abc123",
        metadataType.getSasToken());
  }

  /**
   * A SAS token grants access on its own, so it has to be treated as a secret exactly like the
   * storage account key: {@code password = true} is what makes Hop encrypt it at rest and mask it
   * in the UI. The whole point of the option is to avoid handing out long lived credentials, which
   * is undone if the token is written to metadata in clear text.
   */
  @Test
  void sasTokenIsMarkedAsAPassword() throws NoSuchFieldException {
    Field sasToken = AzureMetadataType.class.getDeclaredField("sasToken");
    HopMetadataProperty property = sasToken.getAnnotation(HopMetadataProperty.class);

    assertNotNull(property, "sasToken must be a HopMetadataProperty to be serialized");
    assertTrue(property.password(), "sasToken must be marked as a password so it is encrypted");
  }

  /** The storage account key must keep the same protection, so the two stay consistent. */
  @Test
  void storageAccountKeyRemainsAPassword() throws NoSuchFieldException {
    Field key = AzureMetadataType.class.getDeclaredField("storageAccountKey");
    HopMetadataProperty property = key.getAnnotation(HopMetadataProperty.class);

    assertNotNull(property);
    assertTrue(property.password());
  }

  /**
   * Adding a third authentication type must not disturb existing connections. A new instance still
   * defaults to "Key", and a key-based connection carries no SAS token, so nothing an existing user
   * has configured changes meaning.
   */
  @Test
  void keyAuthenticationRemainsTheDefault() {
    AzureMetadataType metadataType = new AzureMetadataType();
    metadataType.setStorageAccountName("hopsa");
    metadataType.setStorageAccountKey("aGVsbG93b3JsZA==");

    assertEquals("Key", metadataType.getAuthenticationType());
    assertNull(metadataType.getSasToken());
  }
}
