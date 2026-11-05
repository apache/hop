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

package org.apache.hop.workflow.actions.setpasswordencoder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Unit test for {@link ActionSetPasswordEncoder} */
class ActionSetPasswordEncoderTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopLogStore.init();
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");
  }

  @Test
  void unknownPluginIdFailsWithoutChangingTheEncoder() throws Exception {
    ActionSetPasswordEncoder action = new ActionSetPasswordEncoder("encoder");
    action.setEncoderPluginId("Nope");
    action.setVariable(Const.HOP_PASSWORD_ENCODER_PLUGIN, "Hop");

    Result result = action.execute(new Result(), 0);

    assertEquals(1, result.getNrErrors());
    assertEquals("Hop", action.getVariable(Const.HOP_PASSWORD_ENCODER_PLUGIN));
    assertInstanceOf(HopTwoWayPasswordEncoder.class, Encr.getEncoder());
  }

  @Test
  void registeredPluginIdInitializesTheEncoder() throws Exception {
    ActionSetPasswordEncoder action = new ActionSetPasswordEncoder("encoder");
    action.setEncoderPluginId("Hop");

    Result result = action.execute(new Result(), 0);

    assertEquals(0, result.getNrErrors());
    assertEquals("Hop", action.getVariable(Const.HOP_PASSWORD_ENCODER_PLUGIN));
    assertInstanceOf(HopTwoWayPasswordEncoder.class, Encr.getEncoder());
  }
}
