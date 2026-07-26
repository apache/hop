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

package org.apache.hop.marketplace.env;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.AttributesContext;
import org.junit.jupiter.api.Test;

class MarketplaceAttributesTest {

  @Test
  void purposeDefaults() {
    assertEquals(
        MarketplaceAttributes.ON_ENABLE_ENFORCE,
        MarketplaceAttributes.defaultOnEnableForPurpose("Production"));
    assertEquals(
        MarketplaceAttributes.ON_ENABLE_WARN,
        MarketplaceAttributes.defaultOnEnableForPurpose("Testing"));
    assertEquals(
        MarketplaceAttributes.ON_ENABLE_WARN,
        MarketplaceAttributes.defaultOnEnableForPurpose("Acceptance"));
    assertEquals(
        MarketplaceAttributes.ON_ENABLE_OFF,
        MarketplaceAttributes.defaultOnEnableForPurpose("Development"));
    assertEquals(
        MarketplaceAttributes.ON_ENABLE_OFF, MarketplaceAttributes.defaultOnEnableForPurpose(null));
  }

  @Test
  void explicitOverridesPurpose() {
    AttributesContext ctx = new AttributesContext();
    ctx.setAttribute(
        MarketplaceAttributes.GROUP,
        MarketplaceAttributes.KEY_ON_ENABLE,
        MarketplaceAttributes.ON_ENABLE_OFF);
    assertEquals(
        MarketplaceAttributes.ON_ENABLE_OFF,
        MarketplaceAttributes.resolveOnEnable(ctx, "Production"));
  }

  @Test
  void strictAndAutoApply() {
    AttributesContext ctx = new AttributesContext();
    assertFalse(MarketplaceAttributes.isStrict(ctx));
    ctx.setAttribute(MarketplaceAttributes.GROUP, MarketplaceAttributes.KEY_STRICT, "true");
    assertTrue(MarketplaceAttributes.isStrict(ctx));
    ctx.setAttribute(MarketplaceAttributes.GROUP, MarketplaceAttributes.KEY_AUTO_APPLY, "Y");
    assertTrue(MarketplaceAttributes.isAutoApply(ctx));
  }
}
