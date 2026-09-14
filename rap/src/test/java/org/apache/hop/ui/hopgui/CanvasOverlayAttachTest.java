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

package org.apache.hop.ui.hopgui;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Guards the Hop Web canvas overlay against issue #8286.
 *
 * <p>RAP does not put widget ids on DOM elements unless enableUITests is on, so looking up {@code
 * document.getElementById(canvasId)} and then guessing "the first canvas larger than 500x500" left
 * a blank graph in a small viewport and drew the graph inside a dialog when one was open.
 */
class CanvasOverlayAttachTest {

  @Test
  void svgOverlayResolvesTheRapWidgetNotALargeCanvas() throws IOException {
    String js = readResource("org/apache/hop/ui/hopgui/canvas-svg.js");

    assertFalse(js.contains("findVisibleGraphCanvas"), js);
    assertFalse(js.contains("rect.width > 500"), js);
    assertTrue(js.contains("ObjectRegistry"), js);
    assertTrue(js.contains("this._canvasId = properties.canvasId"), js);
  }

  @Test
  void zoomResolvesTheRapWidgetNotALargeCanvas() throws IOException {
    String js = readResource("org/apache/hop/ui/hopgui/canvas-zoom.js");

    assertFalse(js.contains("rect.width > 500"), js);
    assertTrue(js.contains("ObjectRegistry"), js);
    assertTrue(js.contains("this._canvasId = properties.canvas"), js);
  }

  private static String readResource(String name) throws IOException {
    InputStream in = CanvasOverlayAttachTest.class.getClassLoader().getResourceAsStream(name);
    assertNotNull(in, name);
    try (in) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
