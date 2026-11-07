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

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
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
   * headings wins and AsciiDoc wins a tie. A level 0 {@code = } line further down only counts when
   * there are no Markdown headings: next to them it is more likely text such as {@code = 42}.
   */
  private static ContentType detectHeadingSyntax(String text) {
    if (AsciiDocDocumentParser.startsWithDocumentTitle(text)) {
      return ContentType.ASCIIDOC;
    }
    List<HeadingParserSupport.Heading> asciiDocHeadings = AsciiDocDocumentParser.findHeadings(text);
    long asciiDocSections = asciiDocHeadings.stream().filter(h -> h.level() > 1).count();
    List<HeadingParserSupport.Heading> markdownHeadings = MarkdownDocumentParser.findHeadings(text);
    long markdown = markdownHeadings.size();
    if (asciiDocSections > 0) {
      // In AsciiDoc a # line in a ---- listing block is a code comment, not a Markdown heading.
      List<int[]> codeBlocks = AsciiDocDocumentParser.findCodeBlocks(text);
      markdown =
          markdownHeadings.stream()
              .filter(h -> codeBlocks.stream().noneMatch(b -> h.start() >= b[0] && h.end() <= b[1]))
              .count();
    }
    if (markdown == 0) {
      return asciiDocHeadings.isEmpty() ? ContentType.PLAIN : ContentType.ASCIIDOC;
    }
    return asciiDocSections >= markdown ? ContentType.ASCIIDOC : ContentType.MARKDOWN;
  }

  /**
   * A JSON object: an opening brace followed by a quoted key or the closing brace, and a closing
   * brace at the end. A Hugo shortcode at the start of a Markdown page is not one.
   */
  private static boolean looksLikeJsonObject(String text) {
    String trimmed = text.strip();
    if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
      return false;
    }
    String afterBrace = trimmed.substring(1).stripLeading();
    return afterBrace.startsWith("\"") || afterBrace.startsWith("}");
  }
}
