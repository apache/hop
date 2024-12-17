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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests for HopServerMeta class */
public class HopServerMetaTest {
  HopServerMeta hopServer;
  IVariables variables;

  @BeforeClass
  public static void beforeClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @AfterClass
  public static void tearDown() {
    PluginRegistry.getInstance().reset();
  }

  @Test
  public void testModifyingName() {
    HopServerMeta hopServer1 = spy(new HopServerMeta());
    hopServer1.setName("test");
    List<HopServerMeta> list = new ArrayList<>();
    list.add(hopServer1);

    HopServerMeta hopServer2 = spy(new HopServerMeta());
    hopServer2.setName("test");

    hopServer2.verifyAndModifyHopServerName(list, null);

    assertFalse(hopServer1.getName().equals(hopServer2.getName()));
  }

  @Test
  public void testEqualsHashCodeConsistency() {
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
