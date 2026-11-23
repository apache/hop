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

package org.apache.hop.pipeline.transforms.edi2xml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilderFactory;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hop.pipeline.transforms.edi2xml.grammar.FastSimpleGenericEdifactDirectXMLLexer;
import org.apache.hop.pipeline.transforms.edi2xml.grammar.FastSimpleGenericEdifactDirectXMLParser;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;

/**
 * The XML the EDIFACT parser writes: which characters of a value are escaped, what happens to the
 * release character, and the layout of segments, elements and values.
 */
class EdifactToXmlParserTest {

  private static final Pattern VALUE = Pattern.compile("<value>(.*?)</value>", Pattern.DOTALL);

  // not as unicode escapes in the source: the compiler reads those before it reads the string
  private static final char CONTROL_1 = (char) 0x01;
  private static final char DELETE = (char) 0x7f;
  private static final char HIGH_SURROGATE = (char) 0xd83d;
  private static final char LOW_SURROGATE = (char) 0xde00;

  /** Runs the lexer and parser over a message, as the transform does for the first row. */
  private static String toXml(String edi) throws Exception {
    FastSimpleGenericEdifactDirectXMLLexer lexer =
        new FastSimpleGenericEdifactDirectXMLLexer(new ANTLRStringStream(edi));
    FastSimpleGenericEdifactDirectXMLParser parser =
        new FastSimpleGenericEdifactDirectXMLParser(new CommonTokenStream(lexer));
    parser.edifact();
    return parser.buf.toString();
  }

  /** The text between the value tags, as written: still escaped. */
  private static List<String> values(String xml) {
    List<String> values = new ArrayList<>();
    Matcher matcher = VALUE.matcher(xml);
    while (matcher.find()) {
      values.add(matcher.group(1));
    }
    return values;
  }

  private static Document parse(String xml) throws Exception {
    return DocumentBuilderFactory.newInstance()
        .newDocumentBuilder()
        .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
  }

  private static String sanitize(String text) {
    FastSimpleGenericEdifactDirectXMLParser parser =
        new FastSimpleGenericEdifactDirectXMLParser(
            new CommonTokenStream(
                new FastSimpleGenericEdifactDirectXMLLexer(new ANTLRStringStream(""))));
    return parser.sanitizeText(text);
  }

  // --- sanitizeText: one value ---

  @Test
  void aValueWithoutSpecialCharactersIsLeftAsItIs() {
    assertEquals("ORDERS", sanitize("ORDERS"));
    assertEquals("12 MAIN STREET", sanitize("12 MAIN STREET"));
    assertEquals("14.58", sanitize("14.58"));
    assertEquals("", sanitize(""));
    assertEquals("tab\there", sanitize("tab\there"));
  }

  @Test
  void theFiveXmlCharactersAreEscaped() {
    assertEquals("ACME &amp; Sons &lt;Ltd&gt;", sanitize("ACME & Sons <Ltd>"));
    assertEquals("say &quot;hi&quot; it&apos;s", sanitize("say \"hi\" it's"));
    assertEquals("&amp;&amp;", sanitize("&&"));
    assertEquals("&amp;amp;", sanitize("&amp;"));
  }

  @Test
  void theReleaseCharacterIsResolvedBeforeEscaping() {
    assertEquals("a + b : c &apos; d ? e", sanitize("a ?+ b ?: c ?' d ?? e"));
    // a question mark that releases nothing stays
    assertEquals("?", sanitize("?"));
    assertEquals("a?b", sanitize("a?b"));
    // a released character that needs escaping is escaped
    assertEquals("O&apos;Brien &amp; Co", sanitize("O?'Brien & Co"));
  }

  @Test
  void textOutsideAsciiIsKept() {
    assertEquals("café € ñ", sanitize("café € ñ"));
    String emoji = "ok " + HIGH_SURROGATE + LOW_SURROGATE;
    assertEquals(emoji, sanitize(emoji));
  }

  @Test
  void charactersXmlCannotHoldAreRemovedOrWrittenAsAReference() {
    assertEquals("ctl  x", sanitize("ctl " + CONTROL_1 + " x"));
    assertEquals("del &#127; x", sanitize("del " + DELETE + " x"));
    // half of a surrogate pair is not a character
    assertEquals("lone  x", sanitize("lone " + HIGH_SURROGATE + " x"));
  }

  // --- the check that skips the escaping ---

  /** For every character: when the check says nothing needs escaping, escaping changes nothing. */
  @Test
  void aCharacterThatIsNotFlaggedIsNeverChangedByTheEscaping() {
    int passed = 0;
    for (int c = 0; c <= 0xffff; c++) {
      String text = String.valueOf((char) c);
      if (!FastSimpleGenericEdifactDirectXMLParser.needsEscaping(text)) {
        passed++;
        assertEquals(text, StringEscapeUtils.escapeXml10(text), "character " + c);
      }
    }
    // nearly every character passes: the check is not just always saying yes
    assertTrue(passed > 60_000, "characters that pass: " + passed);
  }

  @Test
  void theOutcomeIsThatOfTheEscapingForMixedTexts() {
    List<String> texts =
        List.of(
            "ORDERS",
            "ACME & Sons <Ltd>",
            "say \"hi\" it's",
            "café € ñ",
            "ok " + HIGH_SURROGATE + LOW_SURROGATE,
            "lone " + HIGH_SURROGATE + " x",
            "ctl " + CONTROL_1 + " x",
            "del " + DELETE + " x",
            "tab\there\nand a line",
            "");
    for (String text : texts) {
      assertEquals(StringEscapeUtils.escapeXml10(text), sanitize(text), text);
    }
  }

  // --- whole messages ---

  @Test
  void aSmallMessageGivesThisXml() throws Exception {
    String xml = toXml("UNH+1+ORDERS:D:96A:UN'\nBGM+220+128576'\nUNT+3+1'");

    assertEquals(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<edifact>\n"
            + "\t<UNH>\n"
            + "\t\t<element>\n"
            + "\t\t\t<value>1</value>\n"
            + "\t\t</element>\n"
            + "\t\t<element>\n"
            + "\t\t\t<value>ORDERS</value>\n"
            + "\t\t\t<value>D</value>\n"
            + "\t\t\t<value>96A</value>\n"
            + "\t\t\t<value>UN</value>\n"
            + "\t\t</element>\n"
            + "\t</UNH>\n"
            + "\t<BGM>\n"
            + "\t\t<element>\n"
            + "\t\t\t<value>220</value>\n"
            + "\t\t</element>\n"
            + "\t\t<element>\n"
            + "\t\t\t<value>128576</value>\n"
            + "\t\t</element>\n"
            + "\t</BGM>\n"
            + "\t<UNT>\n"
            + "\t\t<element>\n"
            + "\t\t\t<value>3</value>\n"
            + "\t\t</element>\n"
            + "\t\t<element>\n"
            + "\t\t\t<value>1</value>\n"
            + "\t\t</element>\n"
            + "\t</UNT>\n"
            + "</edifact>",
        xml);
  }

  @Test
  void valuesThatNeedEscapingGiveWellFormedXmlWithTheOriginalText() throws Exception {
    String xml =
        toXml(
            "UNH+1+PAORES:93:1:IA'\n"
                + "IFT+3+ACME & Sons <Ltd> ?+ more'\n"
                + "NAD+BY+++O?'Brien::\"x\"'\n"
                + "UNT+4+1'");

    assertEquals(
        List.of(
            "1",
            "PAORES",
            "93",
            "1",
            "IA",
            "3",
            "ACME &amp; Sons &lt;Ltd&gt; + more",
            "BY",
            "",
            "",
            "O&apos;Brien",
            "",
            "&quot;x&quot;",
            "4",
            "1"),
        values(xml));

    // a reader of the XML gets the text of the message back
    Document document = parse(xml);
    assertEquals(
        "ACME & Sons <Ltd> + more",
        document
            .getElementsByTagName("IFT")
            .item(0)
            .getLastChild()
            .getPreviousSibling()
            .getTextContent()
            .trim());
    assertEquals(15, document.getElementsByTagName("value").getLength());
  }

  @Test
  void emptyElementsAndComponentsKeepTheirPlace() throws Exception {
    String xml = toXml("PDI++C:3+Y::3+F::1'");

    assertEquals(List.of("", "C", "3", "Y", "", "3", "F", "", "1"), values(xml));
  }

  /** The transform uses one parser for every row. */
  @Test
  void oneParserHandlesOneMessageAfterTheOther() throws Exception {
    FastSimpleGenericEdifactDirectXMLLexer lexer =
        new FastSimpleGenericEdifactDirectXMLLexer(new ANTLRStringStream("UNH+1+A & B'"));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    FastSimpleGenericEdifactDirectXMLParser parser =
        new FastSimpleGenericEdifactDirectXMLParser(tokens);
    parser.edifact();
    assertEquals(List.of("1", "A &amp; B"), values(parser.buf.toString()));

    lexer.setCharStream(new ANTLRStringStream("BGM+220+X'"));
    tokens.setTokenSource(lexer);
    parser.setTokenStream(tokens);
    parser.edifact();

    String second = parser.buf.toString();
    assertEquals(List.of("220", "X"), values(second));
    assertFalse(second.contains("UNH"));
  }

  @Test
  void theSampleMessageOfThePluginGivesWellFormedXml() throws Exception {
    String edi =
        Files.readString(
            Paths.get("src/main/samples/files/edi-to-xml.txt"), StandardCharsets.UTF_8);

    String xml = toXml(edi);

    Document document = parse(xml);
    assertEquals("edifact", document.getDocumentElement().getTagName());
    assertTrue(values(xml).contains("NO MORE FLIGHTS"), xml);
    assertEquals(1, document.getElementsByTagName("UNB").getLength());
    assertEquals(1, document.getElementsByTagName("UNZ").getLength());
  }
}
