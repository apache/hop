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

package org.apache.hop.ui.hopgui.delegates;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Map;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.junit.jupiter.api.Test;

class HopGuiAuditDelegateStatePropertiesTest {

  @Test
  void copyStatePropertiesAllowsPutWhenHandlerReturnsEmptyMap() {
    IHopFileTypeHandler handler = new EmptyHopFileTypeHandler();
    Map<String, Object> copy = HopGuiAuditDelegate.copyStateProperties(handler);
    assertDoesNotThrow(() -> copy.put("active", true));
    assertEquals(true, copy.get("active"));
  }

  @Test
  void copyStatePropertiesCopiesExistingEntries() {
    IHopFileTypeHandler handler =
        new EmptyHopFileTypeHandler() {
          @Override
          public Map<String, Object> getStateProperties() {
            return Collections.singletonMap("zoom", 1.5);
          }
        };
    Map<String, Object> copy = HopGuiAuditDelegate.copyStateProperties(handler);
    assertEquals(1.5, copy.get("zoom"));
    copy.put("active", true);
    assertTrue((Boolean) copy.get("active"));
  }
}
