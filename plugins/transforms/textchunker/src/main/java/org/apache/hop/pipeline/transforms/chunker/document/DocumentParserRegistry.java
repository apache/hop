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
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.hop.pipeline.transforms.chunker.document.hopxml.HopPipelineXmlParser;
import org.apache.hop.pipeline.transforms.chunker.document.hopxml.HopWorkflowXmlParser;
import org.apache.hop.pipeline.transforms.chunker.document.hopxml.HopXmlSupport;
import org.apache.hop.pipeline.transforms.chunker.document.metadata.HopMetadataJsonParser;

/** Resolves {@link DocumentParser} instances and auto-detects content type from text. */
public final class DocumentParserRegistry {

  private static final Map<ContentType, DocumentParser> PARSERS = new EnumMap<>(ContentType.class);

  /** Must stay in sync with {@code AsciiDocDocumentParser.HEADING}. */
  private static final Pattern ASCIIDOC_HEADING = Pattern.compile("(?m)^=+[ \\t]+\\S");

  /** Must stay in sync with {@code MarkdownDocumentParser.HEADING}. */
  private static final Pattern MARKDOWN_HEADING = Pattern.compile("(?m)^#{1,6}[ \\t]+\\S");

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
    String trimmed = text.stripLeading();
    if (trimmed.startsWith("{") || text.contains("Configuration:\n{")) {
      return ContentType.METADATA;
    }
    if (HopXmlSupport.looksLikeHopXml(text)) {
      if (trimmed.contains("<workflow")) {
        return ContentType.WORKFLOW;
      }
      if (trimmed.contains("<pipeline")) {
        return ContentType.PIPELINE;
      }
    }
    // find() rather than matches(): a heading anywhere in the document counts, including one on
    // the very first line, which is the common case for a single-page document.
    if (ASCIIDOC_HEADING.matcher(text).find()) {
      return ContentType.ASCIIDOC;
    }
    if (MARKDOWN_HEADING.matcher(text).find()) {
      return ContentType.MARKDOWN;
    }
    return ContentType.PLAIN;
  }
}
