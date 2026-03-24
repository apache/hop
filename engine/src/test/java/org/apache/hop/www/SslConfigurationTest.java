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

package org.apache.hop.www;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SslConfigurationTest {

  @Test
  void defaultConstructorSetsKeyStoreType() {
    SslConfiguration ssl = new SslConfiguration();
    assertNotNull(ssl.getKeyStoreType());
  }

  @Test
  void setKeyStoreTypeIgnoresNull() {
    SslConfiguration ssl = new SslConfiguration();
    String before = ssl.getKeyStoreType();
    ssl.setKeyStoreType(null);
    assertEquals(before, ssl.getKeyStoreType());
    ssl.setKeyStoreType("PKCS12");
    assertEquals("PKCS12", ssl.getKeyStoreType());
  }

  @Test
  void setKeyStoreRequiresNonEmpty() {
    SslConfiguration ssl = new SslConfiguration();
    assertThrows(IllegalArgumentException.class, () -> ssl.setKeyStore(null));
    assertThrows(IllegalArgumentException.class, () -> ssl.setKeyStore(""));
    ssl.setKeyStore("/path/ks.p12");
    assertEquals("/path/ks.p12", ssl.getKeyStore());
  }

  @Test
  void setKeyStorePasswordRequiresNonEmpty() {
    SslConfiguration ssl = new SslConfiguration();
    ssl.setKeyStore("/ks");
    assertThrows(IllegalArgumentException.class, () -> ssl.setKeyStorePassword(null));
    assertThrows(IllegalArgumentException.class, () -> ssl.setKeyStorePassword(""));
    ssl.setKeyStorePassword("secret");
    assertEquals("secret", ssl.getKeyStorePassword());
  }

  @Test
  void getKeyPasswordFallsBackToKeyStorePassword() {
    SslConfiguration ssl = new SslConfiguration();
    ssl.setKeyStore("/ks");
    ssl.setKeyStorePassword("storePass");
    assertEquals("storePass", ssl.getKeyPassword());
    ssl.setKeyPassword("keyPass");
    assertEquals("keyPass", ssl.getKeyPassword());
  }

  @Test
  void getXmlIncludesTagWhenValuesSet() {
    SslConfiguration ssl = new SslConfiguration();
    ssl.setKeyStore("/tmp/test.jks");
    ssl.setKeyStorePassword("p1");
    String xml = ssl.getXml();
    assertTrue(xml.contains(SslConfiguration.XML_TAG));
    assertTrue(xml.contains("/tmp/test.jks"));
  }
}
