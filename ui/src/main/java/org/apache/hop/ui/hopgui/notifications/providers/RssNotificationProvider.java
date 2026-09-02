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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import javax.xml.parsers.DocumentBuilder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.notifications.INotificationProvider;
import org.apache.hop.core.notifications.Notification;
import org.apache.hop.core.notifications.NotificationCategory;
import org.apache.hop.core.notifications.NotificationPriority;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
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
      CloseableHttpClient client = HttpClientManager.getInstance().createDefaultClient();
      HttpGet request = new HttpGet(feedUrl);
      request.addHeader(
          "Accept", "application/rss+xml, application/atom+xml, application/xml, text/xml");

      try (CloseableHttpResponse response = client.execute(request)) {
        // Check HTTP status code
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode < 200 || statusCode >= 300) {
          // Log warning and return empty list instead of throwing exception
          org.apache.hop.core.logging.LogChannel.UI.logDetailed(
              "RSS Provider '"
                  + getName()
                  + "': HTTP error "
                  + statusCode
                  + " when fetching feed from "
                  + feedUrl
                  + " - skipping");
          return notifications; // Return empty list gracefully
        }

        HttpEntity entity = response.getEntity();
        if (entity == null) {
          return notifications;
        }

        // Check content type to ensure it's XML
        org.apache.http.Header contentTypeHeader = response.getFirstHeader("Content-Type");
        String contentType =
            contentTypeHeader != null ? contentTypeHeader.getValue().toLowerCase() : "";
        if (!contentType.contains("xml")
            && !contentType.contains("atom")
            && !contentType.contains("rss")) {
          // Log warning but try to parse anyway (some feeds don't set proper content type)
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
                new String(
                    buffer, 0, Math.min(bytesRead, 100), java.nio.charset.StandardCharsets.UTF_8);
            // Check if it looks like HTML (common error response)
            if (contentStart.trim().startsWith("<html")
                || contentStart.trim().startsWith("<!DOCTYPE html")) {
              org.apache.hop.core.logging.LogChannel.UI.logDetailed(
                  "RSS Provider '"
                      + getName()
                      + "': Received HTML instead of XML from "
                      + feedUrl
                      + " (possibly an error page) - skipping");
              return notifications; // Return empty list gracefully
            }
            // Check if it starts with XML declaration or valid XML tag
            String trimmed = contentStart.trim();
            if (!trimmed.startsWith("<?xml")
                && !trimmed.startsWith("<feed")
                && !trimmed.startsWith("<rss")
                && !trimmed.startsWith("<rdf:RDF")) {
              org.apache.hop.core.logging.LogChannel.UI.logDetailed(
                  "RSS Provider '"
                      + getName()
                      + "': Response from "
                      + feedUrl
                      + " does not appear to be valid XML - skipping");
              return notifications; // Return empty list gracefully
            }
          }

          DocumentBuilder builder =
              XmlParserFactoryProducer.createSecureDocBuilderFactory().newDocumentBuilder();
          Document document = builder.parse(inputStream);

          // Check if it's Atom or RSS
          Element root = document.getDocumentElement();
          if (root == null) {
            org.apache.hop.core.logging.LogChannel.UI.logDetailed(
                "RSS Provider '"
                    + getName()
                    + "': Empty XML document received from "
                    + feedUrl
                    + " - skipping");
            return notifications; // Return empty list gracefully
          }
          String rootName = root.getNodeName();

          if ("feed".equals(rootName) || rootName.contains("atom")) {
            // Atom feed
            notifications.addAll(parseAtomFeed(document));
          } else if ("rss".equals(rootName) || "rdf:RDF".equals(rootName)) {
            // RSS feed
            notifications.addAll(parseRssFeed(document));
          } else {
            org.apache.hop.core.logging.LogChannel.UI.logDetailed(
                "RSS Provider '"
                    + getName()
                    + "': Unknown feed format: "
                    + rootName
                    + " from "
                    + feedUrl
                    + " - skipping");
            return notifications; // Return empty list gracefully
          }
        }
      }
    } catch (Exception e) {
      // Log error but return empty list instead of throwing to allow other providers to continue
      org.apache.hop.core.logging.LogChannel.UI.logDetailed(
          "RSS Provider '"
              + getName()
              + "': Error fetching feed from "
              + feedUrl
              + " - "
              + e.getMessage()
              + " - skipping");
      return notifications; // Return empty list gracefully
    }

    return notifications;
  }

  /** Parse Atom 1.0 feed */
  private List<Notification> parseAtomFeed(Document document) {
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
        Date published = parseAtomDate(getElementText(entry, "published"));
        if (published == null) {
          published = parseAtomDate(getElementText(entry, "updated"));
        }
        if (published == null) {
          published = new Date();
        }

        // Extract tag name from title (e.g., "2.16.0" from "Release 2.16.0" or just "2.16.0")
        // Use tag name for consistent ID with GitHub API provider
        String tagName = title != null ? title.trim() : null;
        // If title contains "Release", extract the version
        if (tagName != null && tagName.toLowerCase().contains("release")) {
          String[] parts = tagName.split("\\s+");
          for (String part : parts) {
            if (part.matches("\\d+\\.\\d+(\\.\\d+)?(-.*)?")) {
              tagName = part;
              break;
            }
          }
        }

        // Create consistent notification ID using tag name (same format as GitHub API provider)
        String notificationId;
        if (tagName != null && tagName.matches("\\d+\\.\\d+(\\.\\d+)?(-.*)?")) {
          // Use same ID format as GitHub API provider for deduplication
          notificationId = "apache-hop-release-" + tagName;
        } else {
          // Fallback to original ID if we can't extract tag name
          notificationId = id != null ? id : "atom-" + System.currentTimeMillis() + "-" + i;
        }

        Notification notification =
            new Notification(
                notificationId,
                title != null ? title : "Untitled",
                summary != null ? summary : "",
                providerName,
                providerId,
                link,
                published,
                NotificationPriority.INFO,
                NotificationCategory.RELEASE);

        // Set version if we extracted tag name
        if (tagName != null && tagName.matches("\\d+\\.\\d+(\\.\\d+)?(-.*)?")) {
          notification.setVersion(tagName);
        }

        notifications.add(notification);
      } catch (Exception e) {
        // Skip invalid entries
      }
    }

    return notifications;
  }

  /** Parse RSS 2.0 feed */
  private List<Notification> parseRssFeed(Document document) {
    List<Notification> notifications = new ArrayList<>();
    NodeList items = document.getElementsByTagName("item");

    for (int i = 0; i < items.getLength(); i++) {
      Element item = (Element) items.item(i);
      try {
        String guid = getElementText(item, "guid");
        String title = getElementText(item, "title");
        String description = getElementText(item, "description");
        String link = getElementText(item, "link");
        Date pubDate = parseRssDate(getElementText(item, "pubDate"));

        if (pubDate == null) {
          pubDate = new Date();
        }

        String id =
            guid != null
                ? guid
                : (link != null ? link : "rss-" + System.currentTimeMillis() + "-" + i);

        Notification notification =
            new Notification(
                id,
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
        // Skip invalid items
      }
    }

    return notifications;
  }

  private String getElementText(Element parent, String tagName) {
    NodeList nodes = parent.getElementsByTagName(tagName);
    if (nodes.getLength() > 0) {
      Node node = nodes.item(0);
      if (node.getFirstChild() != null) {
        return node.getFirstChild().getNodeValue();
      }
    }
    return null;
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
    if (dateStr == null || dateStr.isEmpty()) {
      return null;
    }
    // Atom dates are ISO 8601 format: 2003-12-13T18:30:02Z or 2003-12-13T18:30:02+01:00
    try {
      // Try ISO 8601 format
      SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
      return isoFormat.parse(dateStr);
    } catch (ParseException e) {
      try {
        SimpleDateFormat isoFormat2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        return isoFormat2.parse(dateStr);
      } catch (ParseException e2) {
        // Try RFC 822 format as fallback
        return parseRssDate(dateStr);
      }
    }
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
