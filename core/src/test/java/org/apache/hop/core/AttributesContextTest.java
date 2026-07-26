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

package org.apache.hop.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AttributesContextTest {

  @Test
  void setAndGetAttribute() {
    AttributesContext ctx = new AttributesContext();
    ctx.setAttribute("marketplace", "onEnable", "enforce");
    assertEquals("enforce", ctx.getAttribute("marketplace", "onEnable"));
    assertNull(ctx.getAttribute("marketplace", "missing"));
    assertNull(ctx.getAttribute("other", "onEnable"));
  }

  @Test
  void copyAttributesFromIsDeep() {
    AttributesContext source = new AttributesContext();
    source.setAttribute("resources", "minDiskGb", "10");
    Map<String, String> group = source.getAttributes("resources");
    assertNotNull(group);

    AttributesContext target = new AttributesContext(source);
    assertEquals("10", target.getAttribute("resources", "minDiskGb"));

    // Mutating source group must not change the copy
    group.put("minDiskGb", "99");
    assertEquals("10", target.getAttribute("resources", "minDiskGb"));
    assertNotSame(source.getAttributesMap(), target.getAttributesMap());
  }

  @Test
  void copyAttributesTo() {
    AttributesContext source = new AttributesContext();
    source.setAttribute("marketplace", "envFile", "hop-env.yaml");

    AttributesContext target = new AttributesContext();
    source.copyAttributesTo(target);
    assertEquals("hop-env.yaml", target.getAttribute("marketplace", "envFile"));
  }

  @Test
  void setAttributesReplacesGroup() {
    AttributesContext ctx = new AttributesContext();
    ctx.setAttribute("g", "a", "1");
    Map<String, String> replacement = new HashMap<>();
    replacement.put("b", "2");
    ctx.setAttributes("g", replacement);
    assertNull(ctx.getAttribute("g", "a"));
    assertEquals("2", ctx.getAttribute("g", "b"));
  }
}
