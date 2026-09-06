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
package org.apache.hop.ui.hopgui.notifications.providers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** What a feed offers, as text a label can show. */
public class FeedTextTest {

  @Test
  public void testTagsAreRemoved() {
    assertEquals("A release is out", FeedText.plainText("<p>A release is out</p>"));
  }

  @Test
  public void testBlocksAreSeparatedRatherThanRunTogether() {
    assertEquals("One Two", FeedText.plainText("<p>One</p><p>Two</p>"));
    assertEquals("One Two", FeedText.plainText("One<br/>Two"));
    assertEquals("One Two", FeedText.plainText("<ul><li>One</li><li>Two</li></ul>"));
  }

  @Test
  public void testAttributesDoNotLeakIntoTheText() {
    assertEquals(
        "Read the notes", FeedText.plainText("<a href=\"https://example.org\">Read the notes</a>"));
  }

  @Test
  public void testScriptAndStyleContentIsDropped() {
    assertEquals("After", FeedText.plainText("<script>var x = 1;</script>After"));
    assertEquals("After", FeedText.plainText("<style>p { color: red; }</style>After"));
  }

  @Test
  public void testEntitiesAreDecoded() {
    assertEquals("Ben & Jerry", FeedText.plainText("Ben &amp; Jerry"));
    assertEquals("a < b", FeedText.plainText("a &lt; b"));
    assertEquals("\"quoted\"", FeedText.plainText("&quot;quoted&quot;"));
    assertEquals("café", FeedText.plainText("caf&#233;"));
    assertEquals("café", FeedText.plainText("caf&#xE9;"));
  }

  @Test
  public void testNonBreakingSpacesBecomeOrdinaryOnes() {
    assertEquals("One Two", FeedText.plainText("One&nbsp;Two"));
  }

  @Test
  public void testAnUnknownEntityIsLeftAlone() {
    // Guessing at it would be worse than showing what the feed actually wrote.
    assertEquals("&frobnicate; here", FeedText.plainText("&frobnicate; here"));
  }

  @Test
  public void testDoubleEscapedHtmlIsAlsoStripped() {
    // A feed that escapes its HTML and then escapes it again is common enough to handle.
    assertEquals("A release is out", FeedText.plainText("&lt;p&gt;A release is out&lt;/p&gt;"));
  }

  @Test
  public void testALoneAngleBracketIsNotMistakenForATag() {
    assertEquals("a < b and c > d", FeedText.plainText("a < b and c > d"));
  }

  @Test
  public void testWhitespaceIsCollapsed() {
    assertEquals("One Two", FeedText.plainText("One\n\n   \tTwo   "));
  }

  @Test
  public void testNullAndEmptyAreLeftAsTheyAre() {
    assertNull(FeedText.plainText(null));
    assertEquals("", FeedText.plainText(""));
  }

  @Test
  public void testPlainTextIsUnchanged() {
    assertEquals(
        "Apache Hop 2.19.0 is now available.",
        FeedText.plainText("Apache Hop 2.19.0 is now available."));
  }

  @Test
  public void testANumericNonBreakingSpaceCollapsesLikeItsNamedTwin() {
    // Java's \s does not match U+00A0, so &#160; would otherwise survive where &nbsp; did not.
    assertEquals("One Two", FeedText.plainText("One&#160;Two"));
    assertEquals("One Two", FeedText.plainText("One&nbsp;&#160; Two"));
  }

  @Test
  public void testAnEscapedLinkFromARealFeedReadsAsASentence() {
    // What the XML parser hands over for an LWN entry: the entities are already decoded, so the
    // HTML the feed embedded is what arrives here.
    String body =
        "Python's support for multithreaded programs has improved considerably over\n"
            + "the last few years with the advent of the <a\n"
            + "href=\"https://lwn.net/Articles/1078367/\">\"free-threaded\" version of the"
            + " language</a>.  But\ntesting multithreaded programs is not";

    assertEquals(
        "Python's support for multithreaded programs has improved considerably over the last few"
            + " years with the advent of the \"free-threaded\" version of the language. But testing"
            + " multithreaded programs is not",
        FeedText.plainText(body));
  }
}
