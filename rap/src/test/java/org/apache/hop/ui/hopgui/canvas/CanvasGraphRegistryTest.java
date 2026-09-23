/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.canvas;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.eclipse.rap.json.JsonObject;
import org.junit.jupiter.api.Test;

class CanvasGraphRegistryTest {

  @Test
  void unregisterDropsOnlyThatCanvasSnapshot() {
    CanvasGraphRegistry registry = new CanvasGraphRegistry();
    registry.updateSnapshot(
        "a", new CanvasRenderSnapshot(1, "<svg a/>", List.of(), new JsonObject()));
    registry.updateSnapshot(
        "b", new CanvasRenderSnapshot(2, "<svg b/>", List.of(), new JsonObject()));

    registry.unregister("a");

    assertNull(registry.getSnapshot("a"));
    assertEquals(0, registry.getCurrentRevision("a"));
    assertEquals(2, registry.getCurrentRevision("b"));
    assertEquals("<svg b/>", registry.getSnapshot("b").getSvg());
  }
}
