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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.hop.core.notifications.Notification;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;

/** Unit tests for the identity a feed entry gets, which has to survive repeated polling. */
public class RssNotificationProviderTest {

  private static final String ATOM_WITH_IDS =
      """
      <?xml version="1.0" encoding="utf-8"?>
      <feed xmlns="http://www.w3.org/2005/Atom">
        <entry>
          <id>tag:github.com,2008:Repository/1/2.19.0</id>
          <title>2.19.0</title>
          <updated>2026-02-01T10:00:00Z</updated>
          <link rel="alternate" href="https://example.com/releases/2.19.0"/>
          <summary>Release notes</summary>
        </entry>
      </feed>
      """;

  /** No id and no link: the only thing left to identify the entry by is its own content. */
  private static final String ATOM_WITHOUT_IDS =
      """
      <?xml version="1.0" encoding="utf-8"?>
      <feed xmlns="http://www.w3.org/2005/Atom">
        <entry>
          <title>Something happened</title>
          <updated>2026-02-01T10:00:00Z</updated>
          <summary>Details</summary>
        </entry>
      </feed>
      """;

  private static final String RSS_WITH_GUID =
      """
      <?xml version="1.0" encoding="utf-8"?>
      <rss version="2.0">
        <channel>
          <item>
            <guid>https://example.com/news/1</guid>
            <title>News item</title>
            <link>https://example.com/news/1</link>
            <pubDate>Sun, 01 Feb 2026 10:00:00 GMT</pubDate>
            <description>Body</description>
          </item>
        </channel>
      </rss>
      """;

  @Test
  public void testAtomEntryUsesFeedId() throws Exception {
    List<Notification> notifications = parseAtom(ATOM_WITH_IDS);

    assertEquals(1, notifications.size());
    assertEquals("tag:github.com,2008:Repository/1/2.19.0", notifications.get(0).getId());
  }

  @Test
  public void testRssItemUsesGuid() throws Exception {
    List<Notification> notifications = parseRss(RSS_WITH_GUID);

    assertEquals(1, notifications.size());
    assertEquals("https://example.com/news/1", notifications.get(0).getId());
  }

  @Test
  public void testIdIsStableAcrossPolls() throws Exception {
    // The bug this guards against: an id built from System.currentTimeMillis() made the same
    // entry look new on every poll, so it piled up and could never stay marked as read.
    String first = parseAtom(ATOM_WITHOUT_IDS).get(0).getId();
    Thread.sleep(5);
    String second = parseAtom(ATOM_WITHOUT_IDS).get(0).getId();

    assertEquals(first, second);
    assertTrue(first.startsWith("digest-"), "expected a content digest, got " + first);
  }

  @Test
  public void testDifferentEntriesGetDifferentIds() throws Exception {
    String other = ATOM_WITHOUT_IDS.replace("Something happened", "Something else happened");

    assertNotEquals(parseAtom(ATOM_WITHOUT_IDS).get(0).getId(), parseAtom(other).get(0).getId());
  }

  @Test
  public void testIdIsNotDerivedFromTheVersionInTheTitle() throws Exception {
    // Two repositories releasing the same version number must not collide. The old code turned
    // any version-shaped token in the title into "apache-hop-release-<version>".
    List<Notification> notifications = parseAtom(ATOM_WITH_IDS);

    assertNotEquals("apache-hop-release-2.19.0", notifications.get(0).getId());
  }

  @Test
  public void testEntryIdIsLocalAndNotYetQualified() throws Exception {
    // NotificationService prefixes the source; the provider must not do it a second time.
    Notification notification = parseRss(RSS_WITH_GUID).get(0);

    assertEquals("https://example.com/news/1", notification.getId());
    assertEquals("feed-source", notification.getSourceId());
  }

  @Test
  public void testDescriptionInAPrettyPrintedCdataSectionIsRead() throws Exception {
    // How most publishing tools write a description. Reading only the first child returned the
    // indentation in front of the CDATA section and dropped the content entirely.
    String rss =
        """
        <?xml version="1.0" encoding="utf-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <guid>https://example.com/news/2</guid>
              <title>News item</title>
              <link>https://example.com/news/2</link>
              <description>
                <![CDATA[The body of the item.]]>
              </description>
            </item>
          </channel>
        </rss>
        """;

    Notification notification = parseRss(rss).get(0);

    assertTrue(
        notification.getMessage().contains("The body of the item."),
        "expected the CDATA content, got: " + notification.getMessage());
  }

  @Test
  public void testDescriptionSplitAcrossTextAndCdataIsReadWhole() throws Exception {
    String rss =
        RSS_WITH_GUID.replace(
            "<description>Body</description>",
            "<description>Before <![CDATA[and after]]></description>");

    Notification notification = parseRss(rss).get(0);

    assertEquals("Before and after", notification.getMessage());
  }

  @Test
  public void testAByteOrderMarkDoesNotHideTheFeed() {
    // Feeds written by Windows tooling are commonly served with a BOM. trim() does not remove it,
    // so the sniff that decides whether the response looks like XML rejected feeds the parser
    // reads perfectly well, reporting them as "not an RSS or Atom feed".
    String bom = "\uFEFF";
    assertEquals(
        "<?xml version=\"1.0\"?>",
        RssNotificationProvider.stripByteOrderMark(bom + "<?xml version=\"1.0\"?>"));
    assertTrue(
        RssNotificationProvider.stripByteOrderMark(bom + "<rss version=\"2.0\">")
            .trim()
            .startsWith("<rss"));
  }

  @Test
  public void testContentWithoutAByteOrderMarkIsUntouched() {
    assertEquals("<rss>", RssNotificationProvider.stripByteOrderMark("<rss>"));
    assertEquals("", RssNotificationProvider.stripByteOrderMark(""));
  }

  private List<Notification> parseAtom(String xml) throws Exception {
    return newProvider().parseAtomFeed(parse(xml));
  }

  private List<Notification> parseRss(String xml) throws Exception {
    return newProvider().parseRssFeed(parse(xml));
  }

  private RssNotificationProvider newProvider() {
    return new RssNotificationProvider(
        "https://example.com/feed.atom", "feed-source", "Example Feed");
  }

  private Document parse(String xml) throws Exception {
    return XmlParserFactoryProducer.createSecureDocBuilderFactory()
        .newDocumentBuilder()
        .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
  }
}
