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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.projects.resources.ResourceAttributes;
import org.junit.jupiter.api.Test;

class ResourceProjectEnvironmentAfterEnabledExtensionPointTest {

  private final ResourceProjectEnvironmentAfterEnabledExtensionPoint xp =
      new ResourceProjectEnvironmentAfterEnabledExtensionPoint();
  private final ILogChannel log = mock(ILogChannel.class);

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
