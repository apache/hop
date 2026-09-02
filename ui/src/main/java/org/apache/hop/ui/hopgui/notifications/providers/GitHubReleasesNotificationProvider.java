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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.notifications.INotificationProvider;
import org.apache.hop.core.notifications.Notification;
import org.apache.hop.core.notifications.NotificationCategory;
import org.apache.hop.core.notifications.NotificationPriority;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.hop.core.util.JsonUtil;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * GitHub Releases API notification provider. Fetches releases from GitHub API and creates
 * notifications. Can filter out pre-releases if desired.
 */
public class GitHubReleasesNotificationProvider implements INotificationProvider {
  private String repositoryOwner;
  private String repositoryName;
  private String providerId;
  private String providerName;
  private boolean enabled = true;
  private long pollInterval = 3600000; // 1 hour default
  private boolean includePreReleases = false; // Default: only stable releases

  /**
   * Create a new GitHub Releases notification provider
   *
   * @param repositoryOwner GitHub repository owner (e.g., "apache")
   * @param repositoryName GitHub repository name (e.g., "hop")
   * @param providerId Unique identifier for this provider instance
   * @param providerName Human-readable name for this provider
   */
  public GitHubReleasesNotificationProvider(
      String repositoryOwner, String repositoryName, String providerId, String providerName) {
    this.repositoryOwner = repositoryOwner;
    this.repositoryName = repositoryName;
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
    return "GitHub Releases provider for: " + repositoryOwner + "/" + repositoryName;
  }

  @Override
  public List<Notification> fetchNotifications() throws HopException {
    List<Notification> notifications = new ArrayList<>();

    if (repositoryOwner == null
        || repositoryOwner.isEmpty()
        || repositoryName == null
        || repositoryName.isEmpty()) {
      return notifications;
    }

    try {
      String apiUrl =
          "https://api.github.com/repos/" + repositoryOwner + "/" + repositoryName + "/releases";
      CloseableHttpClient client = HttpClientManager.getInstance().createDefaultClient();
      HttpGet request = new HttpGet(apiUrl);
      request.addHeader("Accept", "application/vnd.github.v3+json");
      request.addHeader("User-Agent", "Apache-Hop-Notification-System");

      try (CloseableHttpResponse response = client.execute(request)) {
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200) {
          // Log warning and return empty list instead of throwing exception
          org.apache.hop.core.logging.LogChannel.UI.logDetailed(
              "GitHub Provider '"
                  + getName()
                  + "': API returned status code "
                  + statusCode
                  + " for "
                  + apiUrl
                  + " - skipping");
          return notifications; // Return empty list gracefully
        }

        HttpEntity entity = response.getEntity();
        if (entity == null) {
          return notifications;
        }

        try (InputStream inputStream = entity.getContent()) {
          JsonNode releases = JsonUtil.parse(inputStream);

          if (releases.isArray()) {
            for (JsonNode release : releases) {
              boolean isPreRelease =
                  release.has("prerelease") && release.get("prerelease").asBoolean();

              // Skip pre-releases if configured to do so
              if (!includePreReleases && isPreRelease) {
                continue;
              }

              String tagName = release.has("tag_name") ? release.get("tag_name").asText() : null;
              String name = release.has("name") ? release.get("name").asText() : null;
              // If name is just the tag name or empty, create a better title
              if (name == null || name.isEmpty() || name.equals(tagName)) {
                name = "Apache Hop " + (tagName != null ? tagName : "Release");
              }
              String body = release.has("body") ? release.get("body").asText() : null;
              // Clean up body - remove excessive whitespace
              if (body != null) {
                body = body.trim();
                if (body.isEmpty()) {
                  body = null;
                }
              }
              String htmlUrl = release.has("html_url") ? release.get("html_url").asText() : null;
              String publishedAt =
                  release.has("published_at") ? release.get("published_at").asText() : null;
              Date publishedDate = parseGitHubDate(publishedAt);
              if (publishedDate == null) {
                publishedDate = new Date();
              }

              // Create consistent notification ID using tag name (same format as RSS provider)
              // This ensures deduplication between RSS and GitHub API providers
              String notificationId;
              if (tagName != null) {
                notificationId = "apache-hop-release-" + tagName;
              } else {
                // Fallback if no tag name
                String releaseId = release.has("id") ? release.get("id").asText() : null;
                notificationId =
                    "github-release-"
                        + repositoryOwner
                        + "-"
                        + repositoryName
                        + "-"
                        + (releaseId != null ? releaseId : "unknown");
              }

              // Determine priority based on pre-release status
              NotificationPriority priority =
                  isPreRelease ? NotificationPriority.INFO : NotificationPriority.INFO;

              // Determine category
              NotificationCategory category = NotificationCategory.RELEASE;

              // Prepare message/description
              String message = null;
              if (body != null && !body.isEmpty()) {
                message = truncateBody(body);
              } else {
                // If no body, provide a default message
                message =
                    "Apache Hop " + (tagName != null ? tagName : "release") + " is now available.";
              }

              Notification notification =
                  new Notification(
                      notificationId,
                      name,
                      message,
                      providerName,
                      providerId,
                      htmlUrl,
                      publishedDate,
                      priority,
                      category);

              if (tagName != null) {
                notification.setVersion(tagName);
              }

              notifications.add(notification);
            }
          }
        }
      }
    } catch (Exception e) {
      // Log error but return empty list instead of throwing to allow other providers to continue
      org.apache.hop.core.logging.LogChannel.UI.logDetailed(
          "GitHub Provider '"
              + getName()
              + "': Error fetching releases from "
              + repositoryOwner
              + "/"
              + repositoryName
              + " - "
              + e.getMessage()
              + " - skipping");
      return notifications; // Return empty list gracefully
    }

    return notifications;
  }

  private String truncateBody(String body) {
    if (body == null || body.isEmpty()) {
      return "";
    }

    // Remove markdown formatting but preserve text content
    String cleaned = body;
    // Replace markdown links with just the link text: [text](url) -> text
    cleaned = cleaned.replaceAll("\\[([^\\]]+)\\]\\([^\\)]+\\)", "$1");
    // Remove markdown headers but keep the text
    cleaned = cleaned.replaceAll("#+\\s+", "");
    // Remove markdown bold/italic but keep text
    cleaned = cleaned.replaceAll("\\*\\*([^*]+)\\*\\*", "$1"); // **bold**
    cleaned = cleaned.replaceAll("\\*([^*]+)\\*", "$1"); // *italic*
    // Remove markdown code blocks but keep content
    cleaned = cleaned.replaceAll("```[^`]*```", "");
    cleaned = cleaned.replaceAll("`([^`]+)`", "$1");
    // Remove excessive whitespace
    cleaned = cleaned.replaceAll("\\n{3,}", "\n\n"); // Max 2 newlines
    cleaned = cleaned.replaceAll("[ \\t]+", " "); // Multiple spaces to single
    cleaned = cleaned.trim();

    // Truncate to reasonable length (250 chars for UI display)
    if (cleaned.length() > 250) {
      // Try to truncate at a sentence boundary
      int lastPeriod = cleaned.lastIndexOf('.', 250);
      int lastNewline = cleaned.lastIndexOf('\n', 250);
      int truncateAt = Math.max(lastPeriod, lastNewline);
      if (truncateAt > 100) {
        return cleaned.substring(0, truncateAt + 1) + "...";
      }
      return cleaned.substring(0, 247) + "...";
    }
    return cleaned;
  }

  private Date parseGitHubDate(String dateStr) {
    if (dateStr == null || dateStr.isEmpty()) {
      return null;
    }
    // GitHub API dates are ISO 8601 format: 2023-12-01T10:00:00Z
    try {
      SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
      return format.parse(dateStr);
    } catch (ParseException e) {
      try {
        // Try with timezone offset
        SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        return format2.parse(dateStr);
      } catch (ParseException e2) {
        return null;
      }
    }
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

  public String getRepositoryOwner() {
    return repositoryOwner;
  }

  public void setRepositoryOwner(String repositoryOwner) {
    this.repositoryOwner = repositoryOwner;
  }

  public String getRepositoryName() {
    return repositoryName;
  }

  public void setRepositoryName(String repositoryName) {
    this.repositoryName = repositoryName;
  }

  public boolean isIncludePreReleases() {
    return includePreReleases;
  }

  public void setIncludePreReleases(boolean includePreReleases) {
    this.includePreReleases = includePreReleases;
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
