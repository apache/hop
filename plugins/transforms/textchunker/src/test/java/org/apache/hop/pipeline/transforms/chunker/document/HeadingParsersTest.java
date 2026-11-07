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
package org.apache.hop.pipeline.transforms.chunker.document;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Markdown and AsciiDoc section parsing (#9282). */
class HeadingParsersTest {

  private static List<String> sections(DocumentParser parser, String text) {
    return parser.parse(text).flattenSections().stream()
        .map(s -> s.breadcrumb() + ": " + s.getBody())
        .toList();
  }

  @Test
  void asciiDocExampleBlockDelimiterIsNotAHeading() {
    String text =
        "= Title\n\nIntro.\n\n== Section\n\nBefore the block.\n\n====\n"
            + "Example text inside the block.\n====\n\nAfter the block.\n\n== Next\n\nMore text.";

    assertEquals(
        List.of(
            "Title: Intro.",
            "Title > Section: Before the block.\n\n====\nExample text inside the block.\n====\n\n"
                + "After the block.",
            "Title > Next: More text."),
        sections(new AsciiDocDocumentParser(), text));
  }

  @Test
  void asciiDocTitlesInsideDelimitedBlocksAreIgnored() {
    String text =
        "= Title\n\nIntro.\n\n[source]\n----\n== not a section\n----\n\n"
            + "////\n== commented out\n////\n\n"
            + "====\n.Nested\n------\n= neither\n------\n====\n\n== Next\n\nMore text.";

    List<String> sections = sections(new AsciiDocDocumentParser(), text);

    assertEquals(2, sections.size());
    assertEquals("Title > Next: More text.", sections.get(1));
  }

  @Test
  void asciiDocSymmetricTitleDropsTheClosingSequence() {
    assertEquals(
        List.of("Install: Text.", "Install > Install = Upgrade: More."),
        sections(
            new AsciiDocDocumentParser(),
            "== Install ==\n\nText.\n\n=== Install = Upgrade ===\n\nMore."));
  }

  @Test
  void asciiDocDoesNotTreatAnEmptyHeadingMarkerAsAHeading() {
    // The old pattern let \s+ match the line break, so a bare "==" took the next line as title.
    assertEquals(
        List.of("Title: Intro.\n==\nnot a title"),
        sections(new AsciiDocDocumentParser(), "= Title\nIntro.\n==\nnot a title"));
  }

  @Test
  void markdownCodeFenceLinesAreNotHeadings() {
    String text =
        "# Guide\n\nIntro.\n\n```bash\n# download the zip\ncurl -O https://example.com/hop.zip\n```"
            + "\n\nAfter the block.\n\n~~~~\n## also code\n```\n~~~~\n\n## Next\n\nMore.";

    assertEquals(
        List.of(
            "Guide: Intro.\n\n```bash\n# download the zip\ncurl -O https://example.com/hop.zip\n```"
                + "\n\nAfter the block.\n\n~~~~\n## also code\n```\n~~~~",
            "Guide > Next: More."),
        sections(new MarkdownDocumentParser(), text));
  }

  @Test
  void markdownUnclosedFenceRunsToTheEnd() {
    assertEquals(
        List.of("Guide: ```\n# comment"),
        sections(new MarkdownDocumentParser(), "# Guide\n```\n# comment"));
  }

  @Test
  void markdownSkippedLevelKeepsTheNearestAncestor() {
    assertEquals(
        List.of("Guide: Intro.", "Guide > Detail: Detail text.", "Guide > Section: Body."),
        sections(
            new MarkdownDocumentParser(),
            "# Guide\n\nIntro.\n\n### Detail\n\nDetail text.\n\n## Section\n\nBody."));
  }

  @Test
  void markdownClosingHashesAreNotPartOfTheTitle() {
    assertEquals(
        List.of("Guide: Intro.", "Guide > Install: Text.", "Guide > Install > C#: Code."),
        sections(
            new MarkdownDocumentParser(),
            "# Guide #\n\nIntro.\n\n## Install ##   \n\nText.\n\n### C#\n\nCode."));
  }

  @Test
  void markdownHashWithoutTitleIsNotAHeading() {
    assertEquals(
        List.of("Guide: #\nnot a title"),
        sections(new MarkdownDocumentParser(), "# Guide\n#\nnot a title"));
  }

  @Test
  void windowsLineEndingsAreHandled() {
    assertEquals(
        List.of("Guide: Intro.", "Guide > Next: More."),
        sections(
            new MarkdownDocumentParser(), "# Guide\r\n\r\nIntro.\r\n\r\n## Next ##\r\n\r\nMore."));
  }
}
