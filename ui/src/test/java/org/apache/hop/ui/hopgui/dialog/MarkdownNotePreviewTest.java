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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MarkdownNotePreviewTest {

  @TempDir Path tempDir;

  @Test
  void expandImageSourcesRewritesRelativePath() throws Exception {
    Path base = tempDir.resolve("flow.hpl");
    Files.writeString(base, "<pipeline/>");
    Path image = tempDir.resolve("pic.png");
    Files.writeString(image, "not-a-real-png");

    String html = "<p><img src=\"pic.png\" alt=\"x\" /></p>";
    String expanded =
        MarkdownNotePreview.expandImageSources(
            html, new Variables(), base.toAbsolutePath().toString());

    assertTrue(expanded.contains("src=\""));
    assertFalse(expanded.contains("src=\"pic.png\""));
    assertTrue(
        expanded.contains("pic.png")
            && (expanded.contains("file:")
                || expanded.contains(tempDir.toString())
                || expanded.contains("file%3A")
                || expanded.contains("/")));
  }

  @Test
  void expandImageSourcesLeavesHttpsAlone() {
    String html = "<img src=\"https://example.com/a.png\" alt=\"r\"/>";
    String expanded = MarkdownNotePreview.expandImageSources(html, new Variables(), "/tmp/a.hpl");
    assertEquals(html, expanded);
  }

  @Test
  void toBrowserImageUrlLeavesNetwork() {
    assertEquals(
        "https://example.com/x.png",
        MarkdownNotePreview.toBrowserImageUrl(
            new Variables(), "/tmp/p.hpl", "https://example.com/x.png"));
  }
}
