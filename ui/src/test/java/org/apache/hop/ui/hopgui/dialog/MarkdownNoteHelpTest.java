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

package org.apache.hop.ui.hopgui.dialog;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MarkdownNoteHelpTest {

  @Test
  void helpHtmlCoversCoreTopics() {
    String html = MarkdownNoteHelp.buildHtml(false);
    assertTrue(html.contains("Markdown notes in Hop"));
    assertTrue(html.contains("**bold**"));
    assertTrue(html.contains("![Diagram]"));
    assertTrue(html.contains("http"));
    assertTrue(html.contains(".hpl"));
    assertTrue(html.contains("What is supported"));
    assertTrue(html.contains("What is not supported"));
  }
}
