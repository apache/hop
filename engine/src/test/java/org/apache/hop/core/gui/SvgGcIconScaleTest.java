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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.svg.HopSvgGraphics2D;
import org.apache.hop.core.svg.SvgFile;
import org.junit.jupiter.api.Test;

class SvgGcIconScaleTest {

  private static final SvgFile TEST_ICON =
      new SvgFile("test.svg", SvgGcIconScaleTest.class.getClassLoader());

  @Test
  void embeddedIconScaleIsIndependentOfMagnificationParameter() throws Exception {
    float scaleAt1 = embeddedIconScale(1.0f, 1.0f);
    float scaleAt2 = embeddedIconScale(2.0f, 2.0f);

    assertTrue(scaleAt1 > 0, "Expected embedded icon scale at 100% zoom");
    assertEquals(scaleAt1, scaleAt2, 0.0001, "Embed scale should not multiply magnification");
  }

  private static float embeddedIconScale(float transformMagnification, float drawMagnification)
      throws Exception {
    HopSvgGraphics2D graphics2D = HopSvgGraphics2D.newDocument();
    SvgGc gc = new SvgGc(graphics2D, new Point(200, 200), 32, 0, 0);
    gc.setTransform(0.0f, 0.0f, transformMagnification);
    gc.drawImage(TEST_ICON, 10, 10, 32, 32, drawMagnification, 0);

    Matcher matcher = Pattern.compile("scale\\(([0-9.]+)").matcher(graphics2D.toXml());
    assertTrue(matcher.find(), "Expected embedded SVG icon scale transform");
    return Float.parseFloat(matcher.group(1));
  }
}
