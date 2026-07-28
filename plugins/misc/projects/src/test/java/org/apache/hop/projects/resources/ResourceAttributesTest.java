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

package org.apache.hop.projects.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.AttributesContext;
import org.junit.jupiter.api.Test;

class ResourceAttributesTest {

  @Test
  void purposeDefaults() {
    assertEquals(
        ResourceAttributes.ON_ENABLE_ENFORCE,
        ResourceAttributes.defaultOnEnableForPurpose("Production"));
    assertEquals(
        ResourceAttributes.ON_ENABLE_ENFORCE, ResourceAttributes.defaultOnEnableForPurpose("prod"));
    assertEquals(
        ResourceAttributes.ON_ENABLE_WARN, ResourceAttributes.defaultOnEnableForPurpose("Testing"));
    assertEquals(
        ResourceAttributes.ON_ENABLE_WARN,
        ResourceAttributes.defaultOnEnableForPurpose("Acceptance"));
    assertEquals(
        ResourceAttributes.ON_ENABLE_OFF,
        ResourceAttributes.defaultOnEnableForPurpose("Development"));
    assertEquals(
        ResourceAttributes.ON_ENABLE_OFF, ResourceAttributes.defaultOnEnableForPurpose(null));
  }

  @Test
  void explicitOnEnableOverridesPurpose() {
    AttributesContext ctx = new AttributesContext();
    ctx.setAttribute(
        ResourceAttributes.GROUP,
        ResourceAttributes.KEY_ON_ENABLE,
        ResourceAttributes.ON_ENABLE_OFF);
    assertEquals(
        ResourceAttributes.ON_ENABLE_OFF, ResourceAttributes.resolveOnEnable(ctx, "Production"));
  }

  @Test
  void toRequirementEmptyWhenUnset() {
    AttributesContext ctx = new AttributesContext();
    SystemResourceRequirement req = ResourceAttributes.toRequirement(ctx);
    assertFalse(req.hasAnyRequirement());
    assertNull(req.getMinMaxMemoryMb());
    assertNull(req.getMinProcessors());
    assertTrue(req.getDiskChecks().isEmpty());
  }

  @Test
  void toRequirementParsesFields() {
    AttributesContext ctx = new AttributesContext();
    ctx.setAttribute(ResourceAttributes.GROUP, ResourceAttributes.KEY_MIN_MAX_MEMORY_MB, "4096");
    ctx.setAttribute(ResourceAttributes.GROUP, ResourceAttributes.KEY_MIN_PROCESSORS, "4");
    ctx.setAttribute(
        ResourceAttributes.GROUP,
        ResourceAttributes.KEY_DISK_CHECKS,
        "${PROJECT_HOME}/data|10240\n/tmp|${MIN_DISK_MB}\n");

    SystemResourceRequirement req = ResourceAttributes.toRequirement(ctx);
    assertTrue(req.hasAnyRequirement());
    assertEquals(4096L, req.getMinMaxMemoryMb());
    assertEquals(4, req.getMinProcessors());
    assertEquals(2, req.getDiskChecks().size());
    assertEquals("${PROJECT_HOME}/data", req.getDiskChecks().get(0).getPath());
    assertEquals("10240", req.getDiskChecks().get(0).getMinFreeBytes());
    assertEquals("/tmp", req.getDiskChecks().get(1).getPath());
    assertEquals("${MIN_DISK_MB}", req.getDiskChecks().get(1).getMinFreeBytes());
  }

  @Test
  void parseDiskChecksIgnoresInvalidLinesButKeepsVariableExpressions() {
    List<DiskSpaceRequirement> list =
        ResourceAttributes.parseDiskChecks(
            """
            /valid|100
            no-pipe
            |only-min
            path-only|
            /with-var|${MIN_FREE}
            /expanded|1.5m

            /ok|1
            """);
    assertEquals(4, list.size());
    assertEquals("/valid", list.get(0).getPath());
    assertEquals("100", list.get(0).getMinFreeBytes());
    assertEquals("/with-var", list.get(1).getPath());
    assertEquals("${MIN_FREE}", list.get(1).getMinFreeBytes());
    assertEquals("/expanded", list.get(2).getPath());
    assertEquals("1.5m", list.get(2).getMinFreeBytes());
    assertEquals("/ok", list.get(3).getPath());
  }

  @Test
  void formatDiskChecksRoundTrip() {
    List<DiskSpaceRequirement> original =
        List.of(
            new DiskSpaceRequirement("/data", "10"),
            new DiskSpaceRequirement("/tmp", "${MIN_DISK}"));
    String encoded = ResourceAttributes.formatDiskChecks(original);
    List<DiskSpaceRequirement> parsed = ResourceAttributes.parseDiskChecks(encoded);
    assertEquals(2, parsed.size());
    assertEquals("/data", parsed.get(0).getPath());
    assertEquals("10", parsed.get(0).getMinFreeBytes());
    assertEquals("/tmp", parsed.get(1).getPath());
    assertEquals("${MIN_DISK}", parsed.get(1).getMinFreeBytes());
  }

  @Test
  void nonPositiveThresholdsIgnored() {
    AttributesContext ctx = new AttributesContext();
    ctx.setAttribute(ResourceAttributes.GROUP, ResourceAttributes.KEY_MIN_MAX_MEMORY_MB, "0");
    ctx.setAttribute(ResourceAttributes.GROUP, ResourceAttributes.KEY_MIN_PROCESSORS, "-1");
    SystemResourceRequirement req = ResourceAttributes.toRequirement(ctx);
    assertFalse(req.hasAnyRequirement());
  }
}
