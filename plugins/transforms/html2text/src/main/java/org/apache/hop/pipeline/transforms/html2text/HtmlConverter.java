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

package org.apache.hop.pipeline.transforms.html2text;

import org.apache.hop.pipeline.transforms.html2text.Html2TextMeta.SafelistType;
import org.jsoup.nodes.Document;
import org.jsoup.parser.Parser;
import org.jsoup.safety.Cleaner;
import org.jsoup.safety.Safelist;

/**
 * Turns HTML into text, or into cleaned HTML, the way the HTML to text transform is set up.
 *
 * <p>The static {@code Jsoup.parseBodyFragment()} and {@code Jsoup.clean()} build a new parser, a
 * new safelist and a new cleaner for every call. Here they are made once and used for every row.
 * The outcome is the same.
 *
 * <p>One converter per transform copy, used by the thread of that copy only.
 */
final class HtmlConverter {

  private final Parser parser = Parser.htmlParser();

  /** Null when the HTML is turned into text. */
  private final Cleaner cleaner;

  private final boolean normalisedText;

  /**
   * @param cleanOnly true to keep the HTML and remove what the safelist does not allow
   * @param safelistType the safelist for cleaning; the basic one when null
   * @param normalisedText for text: true for normalised whitespace, false for the text as it is
   */
  HtmlConverter(boolean cleanOnly, SafelistType safelistType, boolean normalisedText) {
    this.cleaner = cleanOnly ? new Cleaner(safelist(safelistType)) : null;
    this.normalisedText = normalisedText;
  }

  private static Safelist safelist(SafelistType type) {
    if (type == null) {
      return Safelist.basic();
    }
    return switch (type) {
      case SIMPLE_TEXT -> Safelist.simpleText();
      case BASIC_WITH_IMAGES -> Safelist.basicWithImages();
      case NONE -> Safelist.none();
      case RELAXED -> Safelist.relaxed();
      default -> Safelist.basic();
    };
  }

  /**
   * @param html the HTML, not null
   * @return the text of the HTML, or the cleaned HTML
   */
  String convert(String html) {
    Document document = parseBodyFragment(html);
    if (cleaner != null) {
      // As Jsoup.clean(document.html(), safelist): the document as HTML, parsed again and cleaned
      return cleaner.clean(parseBodyFragment(document.html())).body().html();
    }
    return normalisedText ? document.text() : document.wholeText();
  }

  /** What Jsoup.parseBodyFragment(html) does, with the parser of this converter. */
  private Document parseBodyFragment(String html) {
    Document document = Document.createShell("");
    document.body().appendChildren(parser.parseFragmentInput(html, document.body(), ""));
    return document;
  }
}
