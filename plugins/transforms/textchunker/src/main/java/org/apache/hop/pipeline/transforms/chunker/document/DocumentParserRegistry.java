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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.pipeline.transforms.chunker.document.HeadingParserSupport.Heading;
import org.apache.hop.pipeline.transforms.chunker.document.hopxml.HopPipelineXmlParser;
import org.apache.hop.pipeline.transforms.chunker.document.hopxml.HopWorkflowXmlParser;
import org.apache.hop.pipeline.transforms.chunker.document.hopxml.HopXmlSupport;
import org.apache.hop.pipeline.transforms.chunker.document.metadata.HopMetadataJsonParser;

/** Resolves {@link DocumentParser} instances and auto-detects content type from text. */
public final class DocumentParserRegistry {

  private static final Map<ContentType, DocumentParser> PARSERS = new EnumMap<>(ContentType.class);

  static {
    PARSERS.put(ContentType.PLAIN, new PlainTextDocumentParser());
    PARSERS.put(ContentType.MARKDOWN, new MarkdownDocumentParser());
    PARSERS.put(ContentType.ASCIIDOC, new AsciiDocDocumentParser());
    PARSERS.put(ContentType.PIPELINE, new HopPipelineXmlParser());
    PARSERS.put(ContentType.WORKFLOW, new HopWorkflowXmlParser());
    PARSERS.put(ContentType.METADATA, new HopMetadataJsonParser());
  }

  private DocumentParserRegistry() {}

  public static DocumentParser parserFor(ContentType contentType, String text) {
    ContentType resolved =
        contentType == null || contentType == ContentType.AUTO ? detect(text) : contentType;
    return PARSERS.getOrDefault(resolved, PARSERS.get(ContentType.PLAIN));
  }

  public static ContentType detect(String text) {
    if (text == null || text.isBlank()) {
      return ContentType.PLAIN;
    }
    if (looksLikeJsonObject(text) || text.contains("Configuration:\n{")) {
      return ContentType.METADATA;
    }
    String rootElement = HopXmlSupport.rootElementName(text);
    if ("workflow".equals(rootElement)) {
      return ContentType.WORKFLOW;
    }
    if ("pipeline".equals(rootElement)) {
      return ContentType.PIPELINE;
    }
    return detectHeadingSyntax(text);
  }

  /**
   * Markdown or AsciiDoc, by the headings each parser would find. An AsciiDoc document title
   * ({@code = Title}) as the first content line settles it. Otherwise the syntax with the most
   * headings wins, and on a tie the syntax of the first heading. A {@code #} line in an AsciiDoc
   * verbatim block (a listing, comment, ...) does not count as a Markdown heading, and a {@code =}
   * line in a Markdown code fence does not count as an AsciiDoc title. A single {@code =} line
   * further down (an AsciiDoc document title) only counts when there are no Markdown headings: next
   * to them it is more likely text such as {@code = 42}. Two-line titles do not count: Markdown has
   * them too.
   */
  private static ContentType detectHeadingSyntax(String text) {
    AsciiDocDocumentParser.Scan asciiDoc = AsciiDocDocumentParser.scan(text);
    if (asciiDoc.startsWithDocumentTitle()) {
      return ContentType.ASCIIDOC;
    }
    MarkdownDocumentParser.Scan markdown = MarkdownDocumentParser.scan(text);
    List<Heading> markdownHeadings = markdown.headings();
    if (markdownHeadings.isEmpty()) {
      return asciiDoc.oneLineTitles().isEmpty() ? ContentType.PLAIN : ContentType.ASCIIDOC;
    }
    List<Heading> asciiDocSections =
        asciiDoc.oneLineTitles().stream().filter(h -> h.level() > 1).toList();
    if (asciiDocSections.isEmpty()) {
      return ContentType.MARKDOWN;
    }
    // Both syntaxes have headings: the code blocks of each hide the headings of the other.
    markdownHeadings = outside(markdownHeadings, asciiDoc.verbatimBlocks());
    asciiDocSections = outside(asciiDocSections, markdown.fencedBlocks());
    if (asciiDocSections.size() != markdownHeadings.size()) {
      return asciiDocSections.size() > markdownHeadings.size()
          ? ContentType.ASCIIDOC
          : ContentType.MARKDOWN;
    }
    return markdownHeadings.isEmpty()
            || asciiDocSections.get(0).start() < markdownHeadings.get(0).start()
        ? ContentType.ASCIIDOC
        : ContentType.MARKDOWN;
  }

  /** The headings that are not inside one of the {@code [start, end)} blocks; both are sorted. */
  private static List<Heading> outside(List<Heading> headings, List<int[]> blocks) {
    if (blocks.isEmpty()) {
      return headings;
    }
    List<Heading> result = new ArrayList<>();
    int b = 0;
    for (Heading heading : headings) {
      while (b < blocks.size() && blocks.get(b)[1] <= heading.start()) {
        b++;
      }
      if (b == blocks.size() || heading.start() < blocks.get(b)[0]) {
        result.add(heading);
      }
    }
    return result;
  }

  /**
   * A JSON object: an opening brace followed by a quoted key or the closing brace, and a closing
   * brace at the end. A Hugo shortcode at the start of a Markdown page is not one.
   */
  private static boolean looksLikeJsonObject(String text) {
    int first = 0;
    while (first < text.length() && Character.isWhitespace(text.charAt(first))) {
      first++;
    }
    int last = text.length() - 1;
    while (last > first && Character.isWhitespace(text.charAt(last))) {
      last--;
    }
    if (last <= first || text.charAt(first) != '{' || text.charAt(last) != '}') {
      return false;
    }
    int next = first + 1;
    while (next < last && Character.isWhitespace(text.charAt(next))) {
      next++;
    }
    return text.charAt(next) == '"' || next == last;
  }
}
