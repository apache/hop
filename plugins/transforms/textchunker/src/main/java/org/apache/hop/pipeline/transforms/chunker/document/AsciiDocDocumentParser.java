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
 * Splits AsciiDoc on {@code =} section titles and two-line (setext) titles, and builds a section
 * tree. Lines inside a delimited block ({@code ====}, {@code ----}, {@code ////}, ...) are never
 * section titles, and a delimiter line is never a title either.
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

  /** The underline characters of a two-line title, from level 1 ({@code =}) to level 5. */
  private static final String SETEXT_UNDERLINES = "=-~^+";

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

  /**
   * What content detection needs from one pass over an AsciiDoc text.
   *
   * @param headings all section titles, in order
   * @param oneLineTitles the {@code =} titles only; Markdown has two-line titles too, so those do
   *     not tell the syntaxes apart
   * @param verbatimBlocks the {@code [start, end)} offsets of the closed listing, literal,
   *     passthrough, comment and fenced blocks, where a {@code #} line is not a Markdown heading
   * @param startsWithDocumentTitle the first line with content, after comments, attribute entries
   *     and block attributes, is a document title ({@code = Title})
   */
  record Scan(
      List<Heading> headings,
      List<Heading> oneLineTitles,
      List<int[]> verbatimBlocks,
      boolean startsWithDocumentTitle) {}

  static Scan scan(String text) {
    List<Heading> headings = new ArrayList<>();
    List<Heading> oneLineTitles = new ArrayList<>();
    List<int[]> verbatimBlocks = new ArrayList<>();
    Deque<String> openBlocks = new ArrayDeque<>();
    int verbatimStart = -1;
    // The document title check looks at the first line with content only.
    Boolean documentTitle = null;
    // A text line that starts a block, and so may be the first line of a two-line title.
    Line titleCandidate = null;
    boolean blockStart = true;

    for (Line line : HeadingParserSupport.lines(text)) {
      String content = line.text().stripTrailing();
      Line previous = titleCandidate;
      titleCandidate = null;
      boolean previousWasBlockStart = blockStart;
      blockStart = content.isEmpty();

      String open = openBlocks.peek();
      if (open != null && closes(open, content)) {
        openBlocks.pop();
        if (isVerbatim(open)) {
          verbatimBlocks.add(new int[] {verbatimStart, line.end()});
        }
        blockStart = true;
        continue;
      }
      if (open != null && isVerbatim(open)) {
        // Nothing nests inside a listing, literal, passthrough or comment block.
        continue;
      }
      if (previous != null && isSetextUnderline(previous.text(), content)) {
        // The underline of a two-line title is not a block delimiter.
        headings.add(
            new Heading(
                SETEXT_UNDERLINES.indexOf(content.charAt(0)) + 1,
                previous.text().strip(),
                previous.start(),
                line.end()));
        blockStart = true;
        continue;
      }
      if (DELIMITER.matcher(content).matches()) {
        if (documentTitle == null && !content.startsWith("////")) {
          documentTitle = false;
        }
        if (isVerbatim(content)) {
          verbatimStart = line.start();
        }
        openBlocks.push(content);
        blockStart = true;
        continue;
      }
      if (content.isEmpty()) {
        continue;
      }
      if (content.startsWith("//")
          || ATTRIBUTE_ENTRY.matcher(content).matches()
          || (content.startsWith("[") && content.endsWith("]"))) {
        // Comments, attribute entries and block attributes ([source], [[id]]) come before a block.
        blockStart = previousWasBlockStart;
        continue;
      }
      if (open != null) {
        continue;
      }
      Matcher matcher = HEADING.matcher(line.text());
      if (matcher.matches()) {
        Heading heading =
            new Heading(
                matcher.group(1).length(),
                stripClosingSequence(matcher.group(2).strip(), matcher.group(1)),
                line.start(),
                line.end());
        headings.add(heading);
        oneLineTitles.add(heading);
        if (documentTitle == null) {
          documentTitle = heading.level() == 1;
        }
        blockStart = true;
        continue;
      }
      if (documentTitle == null) {
        documentTitle = false;
      }
      if (previousWasBlockStart && isSetextTitle(line.text())) {
        titleCandidate = line;
      }
    }
    return new Scan(
        headings, oneLineTitles, verbatimBlocks, documentTitle != null && documentTitle);
  }

  /** A title line can not be indented or start with a period (a block title), and has a word. */
  private static boolean isSetextTitle(String line) {
    return !line.isEmpty()
        && !Character.isWhitespace(line.charAt(0))
        && line.charAt(0) != '.'
        && line.chars().anyMatch(Character::isLetterOrDigit);
  }

  /**
   * An underline is one of {@code = - ~ ^ +} repeated, within one character of the title length, as
   * in Asciidoctor.
   */
  private static boolean isSetextUnderline(String title, String line) {
    if (line.length() < 2 || SETEXT_UNDERLINES.indexOf(line.charAt(0)) < 0) {
      return false;
    }
    char c = line.charAt(0);
    return line.chars().allMatch(ch -> ch == c)
        && Math.abs(title.stripTrailing().length() - line.length()) < 2;
  }

  private static boolean closes(String open, String line) {
    return open.startsWith("```") ? line.equals("```") : line.equals(open);
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
