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

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns what a feed offers into text a label can show.
 *
 * <p>An RSS {@code description} and an Atom {@code summary} or {@code content} routinely carry
 * HTML, either inside CDATA or escaped, and the XML parser hands it over as it was written. The
 * panel shows it in a plain {@code Label}, so without this the reader sees {@code <p>} and {@code
 * &nbsp;} rather than a paragraph.
 *
 * <p>Stripping belongs here rather than in the panel because the notification is truncated to a few
 * hundred characters for display: strip afterwards and an entry that opens with a long {@code <div
 * class="...">} preamble is cut down to markup and then to nothing.
 */
final class FeedText {

  /** Only a real tag: a bare {@code <} in "a < b" is text, not markup to be removed. */
  private static final Pattern TAG = Pattern.compile("</?[A-Za-z][^>]*>");

  /** Script and style carry content that is not meant to be read at all. */
  private static final Pattern UNREADABLE =
      Pattern.compile(
          "<(script|style)\\b[^>]*>.*?</\\1>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /** What separates one block of text from the next, once the markup is gone. */
  private static final Pattern BREAKS =
      Pattern.compile(
          "</(p|div|li|tr|h[1-6]|blockquote)>|<br\\s*/?>|<li\\b[^>]*>", Pattern.CASE_INSENSITIVE);

  private static final Pattern ENTITY =
      Pattern.compile("&(#[0-9]{1,7}|#[xX][0-9a-fA-F]{1,6}|[a-zA-Z]{2,10});");

  /**
   * Java's {@code \s} covers only ASCII whitespace, and a feed that writes {@code &#160;} rather
   * than {@code &nbsp;} would otherwise leave a non-breaking space behind where its named twin was
   * collapsed. The narrow and figure spaces turn up in feeds that format numbers.
   */
  private static final Pattern WHITESPACE =
      Pattern.compile("[\\s\\u00A0\\u1680\\u2000-\\u200A\\u2007\\u202F\\u205F\\u3000]+");

  private FeedText() {
    // Utility class
  }

  /**
   * @param text What the feed offered, may be null
   * @return The same thing as a single run of readable text
   */
  static String plainText(String text) {
    if (text == null || text.isEmpty()) {
      return text;
    }
    String stripped = UNREADABLE.matcher(text).replaceAll(" ");
    stripped = BREAKS.matcher(stripped).replaceAll(" ");
    stripped = TAG.matcher(stripped).replaceAll("");
    stripped = decodeEntities(stripped);
    // A feed may escape its HTML twice, so what the first pass decoded can be markup in its own
    // right. One more pass covers that without looping on text that merely mentions a tag.
    if (TAG.matcher(stripped).find()) {
      stripped = TAG.matcher(stripped).replaceAll("");
      stripped = decodeEntities(stripped);
    }
    return WHITESPACE.matcher(stripped).replaceAll(" ").trim();
  }

  private static String decodeEntities(String text) {
    if (text.indexOf('&') < 0) {
      return text;
    }
    Matcher matcher = ENTITY.matcher(text);
    StringBuilder decoded = new StringBuilder(text.length());
    while (matcher.find()) {
      String replacement = replacementFor(matcher.group(1));
      matcher.appendReplacement(
          decoded,
          replacement == null
              ? Matcher.quoteReplacement(matcher.group())
              : Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(decoded);
    return decoded.toString();
  }

  private static String replacementFor(String reference) {
    if (reference.startsWith("#")) {
      try {
        int codePoint =
            reference.charAt(1) == 'x' || reference.charAt(1) == 'X'
                ? Integer.parseInt(reference.substring(2), 16)
                : Integer.parseInt(reference.substring(1));
        // Control characters would only be invisible damage in a single-line label.
        if (codePoint == 9 || codePoint == 10 || codePoint == 13 || codePoint >= 32) {
          return new String(Character.toChars(codePoint));
        }
        return " ";
      } catch (IllegalArgumentException e) {
        // Not a code point we can render, so leave the reference as the feed wrote it.
        return null;
      }
    }
    switch (reference.toLowerCase(Locale.ROOT)) {
      case "amp":
        return "&";
      case "lt":
        return "<";
      case "gt":
        return ">";
      case "quot":
        return "\"";
      case "apos":
        return "'";
      case "nbsp":
        return " ";
      case "hellip":
        return "...";
      case "mdash":
        return "-";
      case "ndash":
        return "-";
      default:
        // A named entity we do not know is left alone rather than guessed at.
        return null;
    }
  }
}
