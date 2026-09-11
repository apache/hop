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

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointMap;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * hopper-edw's {@code HopEnvironmentAfterInit} handler calls {@code HEnvironment.initEmbed()},
 * which calls {@link HopEnvironment#init()} again before the outer init future is completed.
 * Waiting on that future deadlocks hop-conf / hop-web startup.
 */
class HopEnvironmentReentrantInitTest {

  @BeforeEach
  void setUp() throws Exception {
    HopEnvironment.reset();
    ExtensionPointMap.getInstance().reset();
    ReenterFromAfterInit.reset();
    HopClientEnvironment.init();
    ExtensionPointPluginType.getInstance()
        .registerCustom(
            ReenterFromAfterInit.class,
            "test",
            "HopEnvironmentReentrantInitTest",
            HopExtensionPoint.HopEnvironmentAfterInit.id,
            "Re-enter HopEnvironment.init from AfterInit",
            null);
  }

  @AfterEach
  void tearDown() {
    HopEnvironment.reset();
    ExtensionPointMap.getInstance().reset();
  }

  @Test
  void afterInitMayCallInitAndIsInitializedWithoutDeadlock() throws Exception {
    assertTimeoutPreemptively(
        Duration.ofSeconds(60),
        () -> {
          HopEnvironment.init();
        });

    assertTrue(HopEnvironment.isInitialized());
    assertTrue(ReenterFromAfterInit.called, "HopEnvironmentAfterInit must run");
    assertTrue(
        ReenterFromAfterInit.initializedDuringCall,
        "isInitialized() must be true during AfterInit");
    assertTrue(ReenterFromAfterInit.reenteredInit, "nested init() must return");
  }

  public static class ReenterFromAfterInit implements IExtensionPoint<PluginRegistry> {
    static volatile boolean called;
    static volatile boolean initializedDuringCall;
    static volatile boolean reenteredInit;

    static void reset() {
      called = false;
      initializedDuringCall = false;
      reenteredInit = false;
    }

    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, PluginRegistry pluginRegistry) throws HopException {
      called = true;
      initializedDuringCall = HopEnvironment.isInitialized();
      HopEnvironment.init();
      reenteredInit = true;
    }
  }
}
