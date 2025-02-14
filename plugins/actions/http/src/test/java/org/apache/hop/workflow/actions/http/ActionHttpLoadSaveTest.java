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

package org.apache.hop.workflow.actions.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ActionHttpLoadSaveTest {

  @BeforeAll
  static void beforeClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @Test
  void testSerialization() throws Exception {
    ActionHttp meta =
        ActionSerializationTestUtil.testSerialization("/http-action.xml", ActionHttp.class);

    assertEquals("url", meta.getUrl());
    assertEquals("/target/file.csv", meta.getTargetFilename());
    assertEquals("/tmp/file.csv", meta.getUploadFilename());
    assertEquals("username", meta.getUsername());
    assertEquals("proxy", meta.getProxyHostname());
    assertEquals("8080", meta.getProxyPort());
    assertEquals(2, meta.getHeaders().size());
    assertTrue(meta.isIgnoreSsl());
    assertFalse(meta.isRunForEveryRow());
  }

  @Test
  void testClone() throws Exception {
    ActionHttp meta =
        ActionSerializationTestUtil.testSerialization("/http-action.xml", ActionHttp.class);

    ActionHttp clone = (ActionHttp) meta.clone();

    assertEquals(clone.getUrl(), meta.getUrl());
    assertEquals(clone.getTargetFilename(), meta.getTargetFilename());
    assertEquals(clone.getUploadFilename(), meta.getUploadFilename());
    assertEquals(clone.getUsername(), meta.getUsername());
    assertEquals(clone.getProxyHostname(), meta.getProxyHostname());
    assertEquals(clone.getProxyPort(), meta.getProxyPort());
    assertEquals(clone.getHeaders().size(), meta.getHeaders().size());
    assertEquals(clone.isIgnoreSsl(), meta.isIgnoreSsl());
    assertEquals(clone.isRunForEveryRow(), meta.isRunForEveryRow());
  }
}
