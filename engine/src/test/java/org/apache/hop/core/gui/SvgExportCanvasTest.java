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

package org.apache.hop.core.gui;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/** Unit test for {@link SvgExportCanvas} canvas. */
class SvgExportCanvasTest {

  @Test
  void canvasFitsBoundsWithEvenMargin() {
    SvgExportCanvas.Bounds bounds = new SvgExportCanvas.Bounds();
    bounds.include(800, 400, 40, 50);

    Point size = SvgExportCanvas.canvasSize(bounds, 1f, 16);
    DPoint offset = SvgExportCanvas.offsetFor(bounds, 1f, 16);

    assertEquals(72, size.x);
    assertEquals(82, size.y);
    assertEquals(-784d, offset.x, 0.001);
    assertEquals(-384d, offset.y, 0.001);
  }

  @Test
  void magnificationScalesContentAndNotTheMargin() {
    SvgExportCanvas.Bounds bounds = new SvgExportCanvas.Bounds();
    bounds.include(0, 0, 100, 40);

    Point size = SvgExportCanvas.canvasSize(bounds, 2f, 16);
    DPoint offset = SvgExportCanvas.offsetFor(bounds, 2f, 16);

    assertEquals(232, size.x);
    assertEquals(112, size.y);
    assertEquals(8d, offset.x, 0.001);
    assertEquals(8d, offset.y, 0.001);
  }

  @Test
  void emptyDiagramIsTheMargin() {
    Point size = SvgExportCanvas.canvasSize(new SvgExportCanvas.Bounds(), 1f, 16);

    assertEquals(32, size.x);
    assertEquals(32, size.y);
  }
}
