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

import org.junit.jupiter.api.Test;

class DocumentParserRegistryTest {

  @Test
  void detectsAsciiDocFromEqualsHeadings() {
    String text = "= Title\n\nIntro\n\n== Section\n\nBody";
    assertEquals(ContentType.ASCIIDOC, DocumentParserRegistry.detect(text));
  }

  @Test
  void detectsMarkdownFromHashHeadings() {
    String text = "# Title\n\nIntro\n\n## Section\n\nBody";
    assertEquals(ContentType.MARKDOWN, DocumentParserRegistry.detect(text));
  }

  @Test
  void mapsDocSourceTypeToAsciiDoc() {
    assertEquals(ContentType.ASCIIDOC, ContentTypeResolver.fromSourceType("doc"));
  }

  @Test
  void mapsArticleAndBlogSourceTypeToMarkdown() {
    assertEquals(ContentType.METADATA, ContentTypeResolver.fromSourceType("metadata"));
    assertEquals(ContentType.MARKDOWN, ContentTypeResolver.fromSourceType("article"));
    assertEquals(ContentType.MARKDOWN, ContentTypeResolver.fromSourceType("blog"));
    assertEquals(ContentType.PLAIN, ContentTypeResolver.fromSourceType("plugin"));
  }

  @Test
  void mapsPipelineSourceTypeToPipelineXml() {
    assertEquals(ContentType.PIPELINE, ContentTypeResolver.fromSourceType("pipeline"));
  }

  @Test
  void detectsPipelineXml() {
    assertEquals(
        ContentType.PIPELINE,
        DocumentParserRegistry.detect("<pipeline><info><name>x</name></info></pipeline>"));
  }

  /**
   * Regression test: detection used to require a newline before the heading, so a document whose
   * only heading sat on the first line was classified as plain text and never chunked by section.
   */
  @Test
  void detectsAHeadingOnTheVeryFirstLine() {
    assertEquals(ContentType.MARKDOWN, DocumentParserRegistry.detect("# Title\n\nSome body text."));
    assertEquals(ContentType.ASCIIDOC, DocumentParserRegistry.detect("= Title\n\nSome body text."));
  }

  @Test
  void detectsAHeadingLaterInTheDocument() {
    assertEquals(
        ContentType.MARKDOWN, DocumentParserRegistry.detect("Intro\n\n## Section\n\nbody"));
    assertEquals(
        ContentType.ASCIIDOC, DocumentParserRegistry.detect("Intro\n\n== Section\n\nbody"));
  }

  @Test
  void doesNotTreatAHashInProseAsAHeading() {
    assertEquals(ContentType.PLAIN, DocumentParserRegistry.detect("issue #42 was fixed yesterday"));
  }

  @Test
  void aHeadingOnTheFirstLineIsChunkedByStructure() {
    String markdown = "# Title\n\nFirst body.\n\n## Sub\n\nSecond body.";
    DocumentParser parser = DocumentParserRegistry.parserFor(ContentType.AUTO, markdown);

    assertEquals(ContentType.MARKDOWN, parser.getContentType());
    assertEquals(2, parser.parse(markdown).flattenSections().size());
  }
}
