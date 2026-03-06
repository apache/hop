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

package org.apache.hop.ui.core.widget.text;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.eclipse.swt.custom.StyleRange;
import org.junit.jupiter.api.Test;

class TextFormatterTest {

  @Test
  void testUrlFormatter() {
    String value =
        "This has one [this is the first value](http://www.example.com/page/index.html?query=test1) [this is the second"
            + " value](http://www.example.com/page/index.html?query=test2)";

    Format format = TextFormatter.getInstance().execute(value);

    assertEquals("This has one this is the first value this is the second value", format.getText());
    assertEquals(2, format.getStyleRanges().size());

    // Cast Object to StyleRange to access properties
    StyleRange styleRange0 = (StyleRange) format.getStyleRanges().get(0);
    StyleRange styleRange1 = (StyleRange) format.getStyleRanges().get(1);

    assertEquals("http://www.example.com/page/index.html?query=test1", styleRange0.data);
    assertEquals("http://www.example.com/page/index.html?query=test2", styleRange1.data);
    assertEquals(13, styleRange0.start);
    assertEquals(37, styleRange1.start);
  }
}
