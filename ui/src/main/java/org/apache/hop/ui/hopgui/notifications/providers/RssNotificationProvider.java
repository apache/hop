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

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import javax.xml.parsers.DocumentBuilder;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.notifications.INotificationProvider;
import org.apache.hop.core.notifications.Notification;
import org.apache.hop.core.notifications.NotificationCategory;
import org.apache.hop.core.notifications.NotificationPriority;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * RSS/Atom feed notification provider. Supports both RSS 2.0 and Atom 1.0 feeds. Can be configured
 * with any feed URL.
 */
public class RssNotificationProvider implements INotificationProvider {
  private String feedUrl;
  private String providerId;
  private String providerName;
  private boolean enabled = true;
  private long pollInterval = 3600000; // 1 hour default
  private String username;
  private String password;

  /** What the feed last answered, so a poll that changes nothing costs a 304. */
  private final NotificationHttp.Conditional conditional = new NotificationHttp.Conditional();

  /** The entries of the last answer, replayed while the feed keeps saying "not modified". */
  private List<Notification> lastFetched = new ArrayList<>();

  /**
   * Create a new RSS notification provider
   *
   * @param feedUrl The URL of the RSS/Atom feed
   * @param providerId Unique identifier for this provider instance
   * @param providerName Human-readable name for this provider
   */
  public RssNotificationProvider(String feedUrl, String providerId, String providerName) {
    this.feedUrl = feedUrl;
    this.providerId = providerId;
    this.providerName = providerName;
  }

  @Override
  public String getId() {
    return providerId;
  }

  @Override
  public String getName() {
    return providerName;
  }

  @Override
  public String getDescription() {
    return "RSS/Atom feed provider for: " + feedUrl;
  }

  @Override
  public List<Notification> fetchNotifications() throws HopException {
    List<Notification> notifications = new ArrayList<>();

    if (feedUrl == null || feedUrl.isEmpty()) {
      return notifications;
    }

    try {
      CloseableHttpClient client = NotificationHttp.newClient(username, password);
      HttpGet request = new HttpGet(feedUrl);
      request.addHeader(
          "Accept", "application/rss+xml, application/atom+xml, application/xml, text/xml");
      conditional.applyTo(request);

      try (ClassicHttpResponse response = (ClassicHttpResponse) client.execute(request)) {
        // Check HTTP status code
        int statusCode = response.getCode();
        if (statusCode == 304) {
          // Unchanged since the last poll.
          return new ArrayList<>(lastFetched);
        }
        if (statusCode < 200 || statusCode >= 300) {
          throw new HopException("The feed at " + feedUrl + " returned HTTP " + statusCode + ".");
        }

        HttpEntity entity = response.getEntity();
        if (entity == null) {
          throw new HopException("The feed at " + feedUrl + " returned an empty response.");
        }

        try (InputStream rawInputStream = entity.getContent();
            BufferedInputStream inputStream = new BufferedInputStream(rawInputStream, 8192)) {
          // Read first few bytes to check for BOM or non-XML content
          inputStream.mark(1024);
          byte[] buffer = new byte[1024];
          int bytesRead = inputStream.read(buffer);
          inputStream.reset();

          if (bytesRead > 0) {
            String contentStart =
                stripByteOrderMark(
                    new String(
                        buffer,
                        0,
                        Math.min(bytesRead, 100),
                        java.nio.charset.StandardCharsets.UTF_8));
            // Check if it looks like HTML (common error response)
            if (contentStart.trim().startsWith("<html")
                || contentStart.trim().startsWith("<!DOCTYPE html")) {
              throw new HopException(
                  "The feed at "
                      + feedUrl
                      + " returned an HTML page instead of a feed. Check the URL, or whether a"
                      + " proxy or sign-in page is answering for it.");
            }
            // Check if it starts with XML declaration or valid XML tag
            String trimmed = contentStart.trim();
            if (!trimmed.startsWith("<?xml")
                && !trimmed.startsWith("<feed")
                && !trimmed.startsWith("<rss")
                && !trimmed.startsWith("<rdf:RDF")) {
              throw new HopException(
                  "The response from " + feedUrl + " is not an RSS or Atom feed.");
            }
          }

          DocumentBuilder builder =
              XmlParserFactoryProducer.createSecureDocBuilderFactory().newDocumentBuilder();
          Document document = builder.parse(inputStream);

          // Check if it's Atom or RSS
          Element root = document.getDocumentElement();
          if (root == null) {
            throw new HopException("The feed at " + feedUrl + " is an empty XML document.");
          }
          String rootName = root.getNodeName();

          if ("feed".equals(rootName) || rootName.contains("atom")) {
            // Atom feed
            notifications.addAll(parseAtomFeed(document));
          } else if ("rss".equals(rootName) || "rdf:RDF".equals(rootName)) {
            // RSS feed
            notifications.addAll(parseRssFeed(document));
          } else {
            throw new HopException(
                "The feed at " + feedUrl + " is in an unsupported format: <" + rootName + ">.");
          }
        }

        // Only now that the feed has been read and understood. Remembering the validator any
        // earlier means a parse that fails still arms the next request's If-None-Match: the feed
        // would answer 304 forever, this method would return the entries it never managed to read
        // (none), and because that is not a failure the error banner would clear itself.
        conditional.remember(response);
      }
      lastFetched = new ArrayList<>(notifications);
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      // Reported to the user through the panel's error banner. NotificationService catches this
      // per provider, so one unreachable feed does not stop the others.
      throw new HopException("Could not read the feed at " + feedUrl + ": " + e.getMessage(), e);
    }

    return notifications;
  }

  /**
   * Drop a leading byte order mark.
   *
   * <p>Feeds written by Windows tooling are commonly served with one. Decoded from UTF-8 it is
   * U+FEFF, which {@link String#trim()} leaves alone - it only strips characters up to U+0020 - so
   * the sniff below would find the feed starting with an invisible character rather than with
   * {@code <?xml}, and reject a feed the XML parser handles perfectly well.
   *
   * @param text The decoded start of the response
   * @return The text without its byte order mark
   */
  static String stripByteOrderMark(String text) {
    if (!text.isEmpty() && text.charAt(0) == '\uFEFF') {
      return text.substring(1);
    }
    return text;
  }

  /** Parse Atom 1.0 feed */
  List<Notification> parseAtomFeed(Document document) {
    List<Notification> notifications = new ArrayList<>();
    NodeList entries = document.getElementsByTagName("entry");

    for (int i = 0; i < entries.getLength(); i++) {
      Element entry = (Element) entries.item(i);
      try {
        String id = getElementText(entry, "id");
        String title = getElementText(entry, "title");
        String summary = getElementText(entry, "summary");
        if (summary == null || summary.isEmpty()) {
          summary = getElementText(entry, "content");
        }
        String link = getElementLink(entry);
        String publishedText = getElementText(entry, "published");
        if (publishedText == null || publishedText.isEmpty()) {
          publishedText = getElementText(entry, "updated");
        }
        Date published = parseAtomDate(publishedText);
        if (published == null) {
          published = new Date();
        }

        String localId = entryId(id, link, title, publishedText);
        if (localId == null) {
          LogChannel.UI.logDetailed(
              "Skipping an entry in the feed at " + feedUrl + " that cannot be identified");
          continue;
        }

        Notification notification =
            new Notification(
                localId,
                title != null ? title : "Untitled",
                summary != null ? summary : "",
                providerName,
                providerId,
                link,
                published,
                NotificationPriority.INFO,
                NotificationCategory.ANNOUNCEMENT);

        notifications.add(notification);
      } catch (Exception e) {
        // One malformed entry should not cost us the rest of the feed.
        LogChannel.UI.logDetailed(
            "Skipping an unreadable entry in the feed at " + feedUrl + ": " + e.getMessage());
      }
    }

    return notifications;
  }

  /** Parse RSS 2.0 feed */
  List<Notification> parseRssFeed(Document document) {
    List<Notification> notifications = new ArrayList<>();
    NodeList items = document.getElementsByTagName("item");

    for (int i = 0; i < items.getLength(); i++) {
      Element item = (Element) items.item(i);
      try {
        String guid = getElementText(item, "guid");
        String title = getElementText(item, "title");
        String description = getElementText(item, "description");
        String link = getElementText(item, "link");
        String pubDateText = getElementText(item, "pubDate");
        Date pubDate = parseRssDate(pubDateText);

        if (pubDate == null) {
          pubDate = new Date();
        }

        String localId = entryId(guid, link, title, pubDateText);
        if (localId == null) {
          LogChannel.UI.logDetailed(
              "Skipping an item in the feed at " + feedUrl + " that cannot be identified");
          continue;
        }

        Notification notification =
            new Notification(
                localId,
                title != null ? title : "Untitled",
                description != null ? description : "",
                providerName,
                providerId,
                link,
                pubDate,
                NotificationPriority.INFO,
                NotificationCategory.ANNOUNCEMENT);

        notifications.add(notification);
      } catch (Exception e) {
        // One malformed item should not cost us the rest of the feed.
        LogChannel.UI.logDetailed(
            "Skipping an unreadable item in the feed at " + feedUrl + ": " + e.getMessage());
      }
    }

    return notifications;
  }

  /**
   * A stable identifier for one feed entry.
   *
   * <p>A feed is polled over and over, and the identifier decides whether an entry is one we have
   * already shown. It therefore has to come out the same on every poll: deriving it from the clock
   * made every entry look new each time, so the same entry piled up and could never stay marked as
   * read.
   *
   * <p>Preference order is the feed's own identifier ({@code atom:id} or {@code rss:guid}, which is
   * exactly what it is for), then the link, then a digest of the parts of the entry that do not
   * change between polls. The published text is used raw rather than parsed, because an unparseable
   * date falls back to "now" and would move on every poll.
   *
   * @param feedId The entry's own identifier, may be null
   * @param link The entry's link, may be null
   * @param title The entry's title, may be null
   * @param publishedText The entry's publication date as written in the feed, may be null
   * @return A stable identifier, or null when the entry carries nothing to identify it by
   */
  private String entryId(String feedId, String link, String title, String publishedText) {
    if (feedId != null && !feedId.trim().isEmpty()) {
      return feedId.trim();
    }
    if (link != null && !link.trim().isEmpty()) {
      return link.trim();
    }
    if (title != null && !title.trim().isEmpty()) {
      return "digest-"
          + digest(title.trim() + "|" + (publishedText == null ? "" : publishedText.trim()));
    }
    return null;
  }

  /**
   * A short hex digest, used to identify a feed entry that carries no identifier of its own.
   *
   * @param value The value to digest
   * @return The first bytes of the SHA-256 of the value, in hex
   */
  private static String digest(String value) {
    try {
      byte[] hash =
          MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder();
      for (int i = 0; i < 8; i++) {
        hex.append(String.format("%02x", hash[i]));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      // Every Java platform is required to provide SHA-256.
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }

  /**
   * The text of an element, however the feed chose to write it.
   *
   * <p>Reading only the first child truncates the shapes feeds actually use. A description written
   * as {@code <description>\n <![CDATA[...]]>\n</description>} - which is how most publishing tools
   * emit one - starts with a whitespace text node, so the first child is the indentation and the
   * content is thrown away. Text broken around an entity reference loses everything after the first
   * piece the same way. Every text and CDATA child is taken instead, in order.
   *
   * @param parent The element to look inside
   * @param tagName The child element to read
   * @return The text, or null when there is no such element
   */
  private String getElementText(Element parent, String tagName) {
    NodeList nodes = parent.getElementsByTagName(tagName);
    if (nodes.getLength() == 0) {
      return null;
    }
    StringBuilder text = new StringBuilder();
    for (Node child = nodes.item(0).getFirstChild();
        child != null;
        child = child.getNextSibling()) {
      if (child.getNodeType() == Node.TEXT_NODE || child.getNodeType() == Node.CDATA_SECTION_NODE) {
        text.append(child.getNodeValue());
      }
    }
    return text.length() == 0 ? null : text.toString();
  }

  private String getElementLink(Element entry) {
    // Atom feeds can have multiple links, prefer "alternate" rel
    NodeList links = entry.getElementsByTagName("link");
    for (int i = 0; i < links.getLength(); i++) {
      Element link = (Element) links.item(i);
      String rel = link.getAttribute("rel");
      if (rel == null || rel.isEmpty() || "alternate".equals(rel)) {
        String href = link.getAttribute("href");
        if (href != null && !href.isEmpty()) {
          return href;
        }
      }
    }
    // Fallback to first link
    if (links.getLength() > 0) {
      Element link = (Element) links.item(0);
      String href = link.getAttribute("href");
      if (href != null && !href.isEmpty()) {
        return href;
      }
    }
    return null;
  }

  private Date parseAtomDate(String dateStr) {
    // Atom requires RFC 3339, which is ISO 8601 with an offset.
    return NotificationDates.parseIso(dateStr);
  }

  private Date parseRssDate(String dateStr) {
    if (dateStr == null || dateStr.isEmpty()) {
      return null;
    }
    // RSS dates are RFC 822 format: "Wed, 02 Oct 2002 08:00:00 EST"
    SimpleDateFormat[] formats = {
      new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH),
      new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z", Locale.ENGLISH),
      new SimpleDateFormat("dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH),
    };

    for (SimpleDateFormat format : formats) {
      try {
        return format.parse(dateStr);
      } catch (ParseException e) {
        // Try next format
      }
    }
    return null;
  }

  /**
   * @param username The user name sent to the feed, may be null
   * @param password The password or token sent to the feed, may be null
   */
  public void setCredentials(String username, String password) {
    this.username = username;
    this.password = password;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  @Override
  public long getPollInterval() {
    return pollInterval;
  }

  @Override
  public void setPollInterval(long interval) {
    this.pollInterval = interval;
  }

  public String getFeedUrl() {
    return feedUrl;
  }

  public void setFeedUrl(String feedUrl) {
    this.feedUrl = feedUrl;
  }

  @Override
  public void initialize() throws HopException {
    // Nothing to initialize
  }

  @Override
  public void shutdown() {
    // Nothing to clean up
  }
}
