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
 *
 */

package org.apache.hop.workflow.actions.sftp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActionSftpTest {
  @BeforeEach
  void beforeEach() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    ActionSftp action =
        ActionSerializationTestUtil.testSerialization("/action-sftp.xml", ActionSftp.class);

    assertEquals("sftp-server", action.getServerName());
    assertEquals("22", action.getServerPort());
    assertEquals("username", action.getUserName());
    assertEquals("password", action.getPassword());
    assertEquals("remote-dir", action.getSftpDirectory());
    assertEquals("target-dir", action.getTargetDirectory());
    assertEquals("wildcard", action.getWildcard());
    assertTrue(action.isRemove());
    assertTrue(action.isAddFilenameToResult());
    assertTrue(action.isPreserveTargetFileTimestamp());
    assertTrue(action.isCreateTargetFolder());
    assertFalse(action.isCopyPrevious());
    assertTrue(action.isUseKeyFilename());
    assertEquals("key-file", action.getKeyFilename());
    assertEquals("key-phrase", action.getKeyPassPhrase());
    assertEquals("zlib", action.getCompression());
    assertEquals("HTTP", action.getProxyType());
    assertEquals("proxy-host", action.getProxyHost());
    assertEquals("80", action.getProxyPort());
    assertEquals("proxy-username", action.getProxyUsername());
    assertEquals("proxy-password", action.getProxyPassword());
  }
}
