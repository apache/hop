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

package org.apache.hop.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.utils.TestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Tests for the {@link HopServerMeta} metadata class. */
class HopServerMetaTest {

  @BeforeAll
  static void beforeClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @Test
  void testModifyingName() {
    HopServerMeta hopServer = spy(new HopServerMeta());
    hopServer.setName("test");
    List<HopServerMeta> list = new ArrayList<>();
    list.add(hopServer);

    HopServerMeta hopServer2 = spy(new HopServerMeta());
    hopServer2.setName("test");

    hopServer2.verifyAndModifyHopServerName(list, null);

    assertNotEquals(hopServer.getName(), hopServer2.getName());
  }

  @Test
  void testEqualsHashCodeConsistency() {
    HopServerMeta server = new HopServerMeta();
    server.setName("server");
    TestUtils.checkEqualsHashCodeConsistency(server, server);

    HopServerMeta serverSame = new HopServerMeta();
    serverSame.setName("server");
    assertEquals(server, serverSame);
    TestUtils.checkEqualsHashCodeConsistency(server, serverSame);

    HopServerMeta serverCaps = new HopServerMeta();
    serverCaps.setName("SERVER");
    TestUtils.checkEqualsHashCodeConsistency(server, serverCaps);

    HopServerMeta serverOther = new HopServerMeta();
    serverOther.setName("something else");
    TestUtils.checkEqualsHashCodeConsistency(server, serverOther);
  }
}
