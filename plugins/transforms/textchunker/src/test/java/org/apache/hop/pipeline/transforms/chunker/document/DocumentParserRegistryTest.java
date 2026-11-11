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

  @Test
  void markdownMentioningHopXmlIsNotHopXml() {
    String text = "# Pipelines\n\nA <pipeline> runs transforms, a <workflow> runs actions.";
    assertEquals(ContentType.MARKDOWN, DocumentParserRegistry.detect(text));
  }

  @Test
  void detectsHopXmlAfterTheDeclarationAndLicenseComment() {
    String header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!--\nlicense\n-->\n";
    assertEquals(
        ContentType.PIPELINE,
        DocumentParserRegistry.detect(header + "<pipeline><info/><workflow_name/></pipeline>"));
    assertEquals(
        ContentType.WORKFLOW,
        DocumentParserRegistry.detect(header + "<workflow><name/></workflow>"));
  }

  @Test
  void markdownStartingWithAShortcodeIsNotJson() {
    String text = "{{< note >}}\nRead this first.\n{{< /note >}}\n\n# Guide\n\nIntro.";
    assertEquals(ContentType.MARKDOWN, DocumentParserRegistry.detect(text));
  }

  @Test
  void detectsMetadataJson() {
    assertEquals(
        ContentType.METADATA, DocumentParserRegistry.detect("  {\n  \"name\": \"conn\"\n}\n"));
  }

  @Test
  void anEqualsLineInMarkdownDoesNotMakeItAsciiDoc() {
    String text = "# Guide\n\nThe answer:\n= 42\n\n## Next\n\nMore text.";
    assertEquals(ContentType.MARKDOWN, DocumentParserRegistry.detect(text));
  }

  @Test
  void asciiDocPageWithShellCommentsIsAsciiDoc() {
    String page =
        "////\nLicensed to the ASF\n////\n:documentationPath: /pipeline/\n:language: en_US\n\n"
            + "[[Install]]\n= Install\n\n== Download\n\n[source,bash]\n----\n# download\n"
            + "# unzip\n# run\n----\n";
    assertEquals(ContentType.ASCIIDOC, DocumentParserRegistry.detect(page));
    // Without the document title the shell comments in the listing block still do not count.
    String fragment = page.substring(page.indexOf("== Download"));
    assertEquals(ContentType.ASCIIDOC, DocumentParserRegistry.detect(fragment));
  }

  @Test
  void markdownWithCodeFenceCommentsIsMarkdown() {
    String text = "Intro\n\n## Install\n\n```\n# a\n# b\n```\n";
    assertEquals(ContentType.MARKDOWN, DocumentParserRegistry.detect(text));
  }

  @Test
  void hashLinesInAsciiDocCommentAndPassthroughBlocksAreNotMarkdownHeadings() {
    assertEquals(
        ContentType.ASCIIDOC,
        DocumentParserRegistry.detect("== Setup\n\nText.\n\n////\n# one\n# two\n////\n"));
    assertEquals(
        ContentType.ASCIIDOC,
        DocumentParserRegistry.detect("== Setup\n\nText.\n\n++++\n# one\n# two\n++++\n"));
  }

  @Test
  void asciiDocTitlesInAMarkdownTildeFenceDoNotCount() {
    String text = "## Example\n\n~~~\n== Section A\n\n== Section B\n~~~\n";
    assertEquals(ContentType.MARKDOWN, DocumentParserRegistry.detect(text));
  }

  @Test
  void onATieTheFirstHeadingDecides() {
    assertEquals(
        ContentType.MARKDOWN,
        DocumentParserRegistry.detect("# Guide\n\nBody.\n\n== Note: tie-break\n\nMore."));
    assertEquals(
        ContentType.ASCIIDOC,
        DocumentParserRegistry.detect("== Download\n\nBody.\n\n# not a heading\n\nMore."));
  }

  @Test
  void aDocumentTitleAfterANestedCommentDelimiterIsFound() {
    // Inside a ////// comment a //// line is text, it does not close the comment.
    String text = "//////\n////\n# commented\n//////\n= Title\n\n# one\n\n# two\n";
    assertEquals(ContentType.ASCIIDOC, DocumentParserRegistry.detect(text));
  }

  @Test
  void markdownTwoLineTitlesDoNotMakeItAsciiDoc() {
    String text = "Guide\n=====\n\nIntro.\n\nInstall\n-------\n\nSteps.\n\n## Detail\n";
    assertEquals(ContentType.MARKDOWN, DocumentParserRegistry.detect(text));
  }

  @Test
  void detectsHopXmlWithADoctypeInternalSubset() {
    String xml =
        "<?xml version=\"1.0\"?>\n<!DOCTYPE pipeline [\n  <!ENTITY name \"x>y\">\n"
            + "  <!-- it's ] here -->\n]>\n<pipeline><info/></pipeline>";
    assertEquals(ContentType.PIPELINE, DocumentParserRegistry.detect(xml));
  }
}
