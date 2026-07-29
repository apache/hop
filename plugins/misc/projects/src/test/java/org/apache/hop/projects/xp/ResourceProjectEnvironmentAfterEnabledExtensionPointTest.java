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

package org.apache.hop.projects.xp;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.hop.core.AttributesContext;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.projects.resources.ResourceAttributes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ResourceProjectEnvironmentAfterEnabledExtensionPointTest {

  private final ResourceProjectEnvironmentAfterEnabledExtensionPoint xp =
      new ResourceProjectEnvironmentAfterEnabledExtensionPoint();
  private final ILogChannel log = mock(ILogChannel.class);

  private String platformRuntime;

  /**
   * The warn path reports through a modal dialog when it believes it is running in the GUI, and
   * {@link Const#getHopPlatformRuntime()} reads a system property that the {@code HopGui}
   * constructor sets and nobody clears. A UI test earlier in this JVM (surefire reuses one fork per
   * module) therefore leaves every later test looking like the GUI, and {@code MessageBox.open()}
   * blocks on its own event loop until somebody clicks OK. The dialog is not what these tests are
   * about, so pin the runtime for the duration.
   */
  @BeforeEach
  void hideTheRuntimeFromLeakedGuiState() {
    platformRuntime = System.getProperty(Const.HOP_PLATFORM_RUNTIME);
    System.clearProperty(Const.HOP_PLATFORM_RUNTIME);
  }

  @AfterEach
  void restorePlatformRuntime() {
    if (platformRuntime == null) {
      System.clearProperty(Const.HOP_PLATFORM_RUNTIME);
    } else {
      System.setProperty(Const.HOP_PLATFORM_RUNTIME, platformRuntime);
    }
  }

  @Test
  void offSkipsCheckEvenWithImpossibleThresholds() {
    AttributesContext ctx = new AttributesContext();
    ctx.setEnvironmentName("dev");
    ctx.setAttribute(
        ResourceAttributes.GROUP,
        ResourceAttributes.KEY_ON_ENABLE,
        ResourceAttributes.ON_ENABLE_OFF);
    ctx.setAttribute(ResourceAttributes.GROUP, ResourceAttributes.KEY_MIN_PROCESSORS, "999999");
    assertDoesNotThrow(() -> xp.callExtensionPoint(log, new Variables(), ctx));
  }

  @Test
  void noThresholdsSkipsEvenWhenEnforce() {
    AttributesContext ctx = new AttributesContext();
    ctx.setEnvironmentName("prod");
    ctx.setAttribute(
        ResourceAttributes.GROUP,
        ResourceAttributes.KEY_ON_ENABLE,
        ResourceAttributes.ON_ENABLE_ENFORCE);
    assertDoesNotThrow(() -> xp.callExtensionPoint(log, new Variables(), ctx));
  }

  @Test
  void enforceThrowsWhenProcessorsInsufficient() {
    AttributesContext ctx = new AttributesContext();
    ctx.setEnvironmentName("prod");
    ctx.setAttribute(
        ResourceAttributes.GROUP,
        ResourceAttributes.KEY_ON_ENABLE,
        ResourceAttributes.ON_ENABLE_ENFORCE);
    ctx.setAttribute(ResourceAttributes.GROUP, ResourceAttributes.KEY_MIN_PROCESSORS, "999999");

    HopException ex =
        assertThrows(HopException.class, () -> xp.callExtensionPoint(log, new Variables(), ctx));
    assertTrue(ex.getMessage().contains("FATAL"));
    assertTrue(ex.getMessage().contains("processors") || ex.getMessage().contains("999999"));
  }

  @Test
  void warnDoesNotThrowWhenProcessorsInsufficient() {
    AttributesContext ctx = new AttributesContext();
    ctx.setEnvironmentName("test");
    ctx.setAttribute(
        ResourceAttributes.GROUP,
        ResourceAttributes.KEY_ON_ENABLE,
        ResourceAttributes.ON_ENABLE_WARN);
    ctx.setAttribute(ResourceAttributes.GROUP, ResourceAttributes.KEY_MIN_PROCESSORS, "999999");
    assertDoesNotThrow(() -> xp.callExtensionPoint(log, new Variables(), ctx));
  }

  @Test
  void enforcePassesWithLenientThresholds() {
    AttributesContext ctx = new AttributesContext();
    ctx.setEnvironmentName("prod");
    ctx.setAttribute(
        ResourceAttributes.GROUP,
        ResourceAttributes.KEY_ON_ENABLE,
        ResourceAttributes.ON_ENABLE_ENFORCE);
    ctx.setAttribute(ResourceAttributes.GROUP, ResourceAttributes.KEY_MIN_PROCESSORS, "1");
    ctx.setAttribute(ResourceAttributes.GROUP, ResourceAttributes.KEY_MIN_MAX_MEMORY_MB, "1");
    assertDoesNotThrow(() -> xp.callExtensionPoint(log, new Variables(), ctx));
  }
}
