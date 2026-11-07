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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.pipeline.transforms.chunker.document.HeadingParserSupport.Heading;
import org.apache.hop.pipeline.transforms.chunker.document.HeadingParserSupport.Line;

/**
 * Splits AsciiDoc on {@code =} section titles and builds a section tree. Lines inside a delimited
 * block ({@code ====}, {@code ----}, {@code ////}, ...) are never section titles, and a delimiter
 * line is never a title either.
 */
public final class AsciiDocDocumentParser implements DocumentParser {

  /** A section title: 1-6 {@code =}, then a space or tab and the title, on a single line. */
  private static final Pattern HEADING = Pattern.compile("^(={1,6})[ \\t]+(\\S.*)$");

  /**
   * A delimited block line: 4 or more of the same delimiter character, {@code --} (open block), a
   * {@code ```} fence, or a table delimiter such as {@code |===}.
   */
  private static final Pattern DELIMITER =
      Pattern.compile("^(?:([/\\-.+=*_])\\1{3,}|--|```.*|[|,:!]={3,})$");

  /** An attribute entry such as {@code :description: ...} or {@code :!toc:}. */
  private static final Pattern ATTRIBUTE_ENTRY = Pattern.compile("^:!?[\\w-]+!?:.*$");

  @Override
  public ContentType getContentType() {
    return ContentType.ASCIIDOC;
  }

  @Override
  public DocumentNode parse(String text) {
    if (text == null || text.isEmpty()) {
      return DocumentNode.root("");
    }
    return HeadingParserSupport.parse(text, scan(text).headings());
  }

  /** The section titles of an AsciiDoc text, skipping delimited blocks. */
  static List<Heading> findHeadings(String text) {
    return scan(text).headings();
  }

  /**
   * The {@code [start, end)} offsets of the closed listing ({@code ----}) and literal ({@code
   * ....}) blocks. Used by content detection to ignore {@code #} comments in code samples.
   */
  static List<int[]> findCodeBlocks(String text) {
    return scan(text).codeBlocks();
  }

  /**
   * True when the first line with content, after comments, attribute entries and block attributes,
   * is a document title ({@code = Title}).
   */
  static boolean startsWithDocumentTitle(String text) {
    boolean inComment = false;
    for (Line line : HeadingParserSupport.lines(text)) {
      String content = line.text().strip();
      if (inComment) {
        inComment = !isCommentDelimiter(content);
        continue;
      }
      if (isCommentDelimiter(content)) {
        inComment = true;
        continue;
      }
      if (content.isEmpty()
          || content.startsWith("//")
          || ATTRIBUTE_ENTRY.matcher(content).matches()
          || (content.startsWith("[") && content.endsWith("]"))) {
        continue;
      }
      Matcher heading = HEADING.matcher(line.text());
      return heading.matches() && heading.group(1).length() == 1;
    }
    return false;
  }

  private static boolean isCommentDelimiter(String line) {
    return line.length() >= 4 && line.chars().allMatch(c -> c == '/');
  }

  private record Scan(List<Heading> headings, List<int[]> codeBlocks) {}

  private static Scan scan(String text) {
    List<Heading> headings = new ArrayList<>();
    List<int[]> codeBlocks = new ArrayList<>();
    Deque<String> openBlocks = new ArrayDeque<>();
    int codeBlockStart = -1;

    for (Line line : HeadingParserSupport.lines(text)) {
      String content = line.text().stripTrailing();
      String open = openBlocks.peek();
      if (open != null && closes(open, content)) {
        openBlocks.pop();
        if (isCodeBlock(open)) {
          codeBlocks.add(new int[] {codeBlockStart, line.end()});
        }
        continue;
      }
      if (open != null && isVerbatim(open)) {
        // Nothing nests inside a listing, literal, passthrough or comment block.
        continue;
      }
      if (DELIMITER.matcher(content).matches()) {
        if (isCodeBlock(content)) {
          codeBlockStart = line.start();
        }
        openBlocks.push(content);
        continue;
      }
      if (open != null) {
        continue;
      }
      Matcher matcher = HEADING.matcher(line.text());
      if (matcher.matches()) {
        headings.add(
            new Heading(
                matcher.group(1).length(),
                stripClosingSequence(matcher.group(2).strip(), matcher.group(1)),
                line.start(),
                line.end()));
      }
    }
    return new Scan(headings, codeBlocks);
  }

  private static boolean closes(String open, String line) {
    return open.startsWith("```") ? line.equals("```") : line.equals(open);
  }

  private static boolean isCodeBlock(String delimiter) {
    return delimiter.startsWith("----") || delimiter.startsWith("....");
  }

  private static boolean isVerbatim(String delimiter) {
    char c = delimiter.charAt(0);
    return delimiter.length() > 2 && (c == '-' || c == '.' || c == '+' || c == '/' || c == '`');
  }

  /** Drops the closing sequence of a symmetric title ({@code == Title ==}). */
  private static String stripClosingSequence(String title, String marker) {
    if (title.endsWith(marker)) {
      String rest = title.substring(0, title.length() - marker.length());
      if (rest.endsWith(" ") || rest.endsWith("\t")) {
        return rest.strip();
      }
    }
    return title;
  }
}
