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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.w3c.dom.svg.SVGDocument;

class SvgDarkModeContrastTest {

  @Test
  void replacesKnownDarkBlueFillOnClone() throws Exception {
    SvgFile svgFile = new SvgFile("test-icon.svg", SvgDarkModeContrastTest.class.getClassLoader());
    SvgCacheEntry cacheEntry = SvgCache.loadSvg(svgFile);

    Map<String, String> colors = Map.of("#0e3a5a", "#c8e7fa");
    SVGDocument contrasted =
        SvgDarkModeContrast.cloneWithContrast(cacheEntry.getSvgDocument(), colors);

    String fill =
        contrasted
            .getRootElement()
            .getElementsByTagName("path")
            .item(0)
            .getAttributes()
            .getNamedItem("fill")
            .getNodeValue();
    assertTrue(fill.toLowerCase().contains("c8e7fa"));
    assertFalse(fill.toLowerCase().contains("0e3a5a"));
  }
}
