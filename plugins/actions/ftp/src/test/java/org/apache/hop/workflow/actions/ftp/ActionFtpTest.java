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

package org.apache.hop.workflow.actions.ftp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActionFtpTest {
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
    ActionFtp action =
        ActionSerializationTestUtil.testSerialization("/action-ftp-get.xml", ActionFtp.class);

    assertEquals("21", action.getServerPort());
    assertEquals("server", action.getServerName());
    assertEquals("username", action.getUserName());
    assertEquals("password", action.getPassword());
    assertEquals("remote-dir", action.getRemoteDirectory());
    assertEquals("target-folder", action.getTargetDirectory());
    assertEquals(".*", action.getWildcard());
    assertTrue(action.isBinaryMode());
    assertEquals(999, action.getTimeout());
    assertTrue(action.isRemove());
    assertTrue(action.isOnlyGettingNewFiles());
    assertTrue(action.isActiveConnection());
    assertEquals("ISO-8859-1", action.getControlEncoding());
    assertTrue(action.isMoveFiles());
    assertEquals("move-to-folder", action.getMoveToDirectory());
    assertTrue(action.isAddDate());
    assertTrue(action.isAddTime());
    assertTrue(action.isSpecifyFormat());
    assertEquals("dt-format", action.getDateTimeFormat());
    assertTrue(action.isAddDateBeforeExtension());
    assertTrue(action.isAddResult());
    assertTrue(action.isCreateMoveFolder());

    assertEquals("proxy-host", action.getProxyHost());
    assertEquals("proxy-port", action.getProxyPort());
    assertEquals("proxy-username", action.getProxyUsername());
    assertEquals("proxy-password", action.getProxyPassword());

    assertEquals("socks-host", action.getSocksProxyHost());
    assertEquals("1080", action.getSocksProxyPort());
    assertEquals("socks-user", action.getSocksProxyUsername());
    assertEquals("socks-pass", action.getSocksProxyPassword());
    assertEquals(ActionFtp.IfFileExistsOperation.CREATE_UNIQUE, action.getIfFileExistsOperation());
    assertEquals("10", action.getNrLimit());
    assertEquals("success_when_at_least", action.getSuccessCondition());
  }
}
