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

package org.apache.hop.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.ExtensionPointMap;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The UI-test harness calls {@link HopEnvironment#reset()} before {@link HopEnvironment#init()}.
 * Loading a pipeline earlier in the same JVM constructs {@link ExtensionPointMap}, and {@code
 * PluginRegistry.reset()} used to drop that map's listener. A plugin registered afterwards, such as
 * the git pre-commit check, was then never called.
 */
class HopEnvironmentExtensionPointResetTest {

  private static final String PLUGIN_ID = "HopEnvironmentExtensionPointResetTest";
  private static final String EXTENSION_POINT_ID = "HopEnvironmentExtensionPointReset";

  @AfterEach
  void removeTestListener() {
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin plugin = registry.getPlugin(ExtensionPointPluginType.class, PLUGIN_ID);
    if (plugin != null) {
      registry.removePlugin(ExtensionPointPluginType.class, plugin);
    }
    HopEnvironment.reset();
  }

  @Test
  void extensionPointRegisteredAfterEnvironmentResetIsCalled() throws Exception {
    // Construct the map first, the way an earlier test that loads a pipeline does.
    ExtensionPointMap.getInstance();
    RecordingExtension.called = false;

    HopEnvironment.reset();
    HopEnvironment.init();

    ExtensionPointPluginType.getInstance()
        .registerCustom(
            RecordingExtension.class,
            "test",
            PLUGIN_ID,
            EXTENSION_POINT_ID,
            "Listener registered after the environment is reset",
            null);

    ExtensionPointHandler.callExtensionPoint(
        LogChannel.GENERAL, null, EXTENSION_POINT_ID, "payload");

    assertTrue(RecordingExtension.called);
    assertFalse(RecordingExtension.calledWithNull);
  }

  public static class RecordingExtension implements IExtensionPoint<Object> {
    static volatile boolean called;
    static volatile boolean calledWithNull;

    @Override
    public void callExtensionPoint(ILogChannel log, IVariables variables, Object object) {
      called = true;
      calledWithNull = object == null;
    }
  }
}
