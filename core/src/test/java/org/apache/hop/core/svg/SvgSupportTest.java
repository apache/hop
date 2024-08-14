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
package org.apache.hop.core.svg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for the SvgSupport class */
public class SvgSupportTest {

  public static final String SVG_IMAGE =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
          + "<svg xmlns=\"http://www.w3.org/2000/svg\" version=\"1.0\"\n"
          + "\twidth=\"38\" height=\"32\"  viewBox=\"0 0 39.875 33.6667\">\n"
          + "<path style=\"stroke: none; fill: #323296;\" d=\"M 10,0 L 30.5,0 39.875,17.5 30.5,33.6667 10,33.6667 L 0,17.5"
          + "L 10,0 z\"/>\n</svg>";

  @Before
  public void setUp() {
    // This isn't really a setup line, instead it is here to show that we didn't privatize the
    // default constructor.
    // It will stop compiling if we make that change to the code. However it should probably be
    // refactored a bit
    // further than that, to better support unit testing.
    new SvgSupport();
  }

  @Test
  public void testIsSvgEnabled() {
    assertTrue(SvgSupport.isSvgEnabled());
  }

  @Test
  public void testLoadSvgImage() throws Exception {
    SvgImage image = SvgSupport.loadSvgImage(new ByteArrayInputStream(SVG_IMAGE.getBytes()));
    assertNotNull(image);
  }

  @Test
  public void testToPngName() {
    assertTrue(SvgSupport.isPngName("my_file.png"));
    assertTrue(SvgSupport.isPngName("my_file.PNG"));
    assertTrue(SvgSupport.isPngName(".png"));
    assertFalse(SvgSupport.isPngName("png"));
    assertFalse(SvgSupport.isPngName("myFile.svg"));
    assertEquals("myFile.png", SvgSupport.toPngName("myFile.svg"));
  }

  @Test
  public void testToSvgName() {
    assertTrue(SvgSupport.isSvgName("my_file.svg"));
    assertTrue(SvgSupport.isSvgName("my_file.SVG"));
    assertTrue(SvgSupport.isSvgName(".svg"));
    assertFalse(SvgSupport.isSvgName("svg"));
    assertFalse(SvgSupport.isSvgName("myFile.png"));
    assertEquals("myFile.svg", SvgSupport.toSvgName("myFile.png"));
  }
}
