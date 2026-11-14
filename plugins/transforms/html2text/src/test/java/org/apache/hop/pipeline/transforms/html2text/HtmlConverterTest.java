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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.hop.pipeline.transforms.html2text.Html2TextMeta.SafelistType;
import org.jsoup.Jsoup;
import org.jsoup.safety.Safelist;
import org.junit.jupiter.api.Test;

/**
 * {@link HtmlConverter} keeps one parser and one cleaner. Its outcome has to be that of the static
 * jsoup calls the transform used to make for every row.
 */
class HtmlConverterTest {

  private static final List<String> HTML =
      List.of(
          "<html><head><title>Title</title></head><body><h1>Head</h1><p><b>bold</b> &amp; "
              + "<i>italic</i></p><ul><li>item one</li><li>item two</li></ul></body></html>",
          "plain text, no tags",
          "<p>broken <b>markup<p>second <i>paragraph",
          "<p>a script <script>alert(1)</script> and a <a href='http://example.com' "
              + "onclick='x()'>link</a></p>",
          "entities &nbsp; &lt; &gt; &eacute; &#8364; and accents é ü ñ",
          "<table><tr><td>cell<td>two</table><div><p>nested <span style='color:red'>span</span>"
              + "</div><img src='a.png' alt='picture'><br>line",
          "<pre>  keep   these\n  spaces  </pre><p>  but   not\n these  </p>",
          "<!-- a comment --><style>p { color: red }</style><p>after</p>",
          "   ",
          "<p></p>");

  /** What the transform did before, for text. */
  private static String textBefore(String html, boolean normalised) {
    return normalised
        ? Jsoup.parseBodyFragment(html).text()
        : Jsoup.parseBodyFragment(html).wholeText();
  }

  /** What the transform did before, for cleaning. */
  private static String cleanBefore(String html, Safelist safelist) {
    return Jsoup.clean(Jsoup.parseBodyFragment(html).html(), safelist);
  }

  @Test
  void wholeTextIsTheSameAsBefore() {
    HtmlConverter converter = new HtmlConverter(false, SafelistType.BASIC, false);
    for (String html : HTML) {
      assertEquals(textBefore(html, false), converter.convert(html), html);
    }
  }

  @Test
  void normalisedTextIsTheSameAsBefore() {
    HtmlConverter converter = new HtmlConverter(false, SafelistType.BASIC, true);
    for (String html : HTML) {
      assertEquals(textBefore(html, true), converter.convert(html), html);
    }
  }

  @Test
  void cleaningIsTheSameAsBeforeForEverySafelist() {
    Object[][] safelists = {
      {SafelistType.NONE, Safelist.none()},
      {SafelistType.SIMPLE_TEXT, Safelist.simpleText()},
      {SafelistType.BASIC, Safelist.basic()},
      {SafelistType.BASIC_WITH_IMAGES, Safelist.basicWithImages()},
      {SafelistType.RELAXED, Safelist.relaxed()},
      // no safelist chosen: the basic one
      {null, Safelist.basic()}
    };
    for (Object[] safelist : safelists) {
      HtmlConverter converter = new HtmlConverter(true, (SafelistType) safelist[0], false);
      for (String html : HTML) {
        assertEquals(
            cleanBefore(html, (Safelist) safelist[1]),
            converter.convert(html),
            safelist[0] + ": " + html);
      }
    }
  }

  /** One converter, so one parser, for row after row: nothing of a row stays for the next one. */
  @Test
  void oneConverterGivesEveryRowItsOwnText() {
    HtmlConverter text = new HtmlConverter(false, SafelistType.BASIC, true);
    HtmlConverter clean = new HtmlConverter(true, SafelistType.BASIC, false);
    for (int i = 0; i < 4000; i++) {
      String html = "<p>row <b>" + i + "</b></p>" + HTML.get(i % HTML.size());
      assertEquals(textBefore(html, true), text.convert(html), html);
      assertEquals(cleanBefore(html, Safelist.basic()), clean.convert(html), html);
    }
  }
}
