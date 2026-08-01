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

package org.apache.hop.core.gui.markdown;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class CommonMarkConfigTest {

  @Test
  void parseAndHtmlRenderHeadingsLinksAndTables() {
    String md =
        """
        # Title
        A [link](https://hop.apache.org) and **bold**.

        | A | B |
        | - | - |
        | 1 | 2 |
        """;
    String html = CommonMarkConfig.toHtmlBody(md);
    assertTrue(html.contains("<h1>"));
    assertTrue(html.contains("https://hop.apache.org"));
    assertTrue(html.contains("<strong>") || html.contains("<b>"));
    assertTrue(html.contains("<table>"));
    assertFalse(html.isBlank());
  }

  @Test
  void emptyMarkdownYieldsEmptyHtml() {
    assertTrue(CommonMarkConfig.toHtmlBody("").isEmpty());
    assertTrue(CommonMarkConfig.toHtmlBody(null).isEmpty());
  }
}
