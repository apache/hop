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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
import org.apache.hop.core.util.JsonUtil;
import org.apache.hop.core.util.VersionCompare;

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
  private String username;
  private String password;
  private String minimumVersion;

  /** What the API last answered, so a poll that changes nothing costs a 304. */
  private final NotificationHttp.Conditional conditional = new NotificationHttp.Conditional();

  /** The releases of the last answer, replayed while the API keeps saying "not modified". */
  private List<Notification> lastFetched = new ArrayList<>();

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
      CloseableHttpClient client = NotificationHttp.newClient(username, password);
      HttpGet request = new HttpGet(apiUrl);
      request.addHeader("Accept", "application/vnd.github.v3+json");
      request.addHeader("User-Agent", "Apache-Hop-Notification-System");
      conditional.applyTo(request);

      try (ClassicHttpResponse response = (ClassicHttpResponse) client.execute(request)) {
        int statusCode = response.getCode();
        if (statusCode == 304) {
          // Nothing has changed since the last poll. On GitHub this does not even count against
          // the rate limit.
          return new ArrayList<>(lastFetched);
        }
        if (statusCode != 200) {
          throw new HopException(describeStatus(statusCode, response, apiUrl));
        }

        HttpEntity entity = response.getEntity();
        if (entity == null) {
          throw new HopException("GitHub returned an empty response for " + apiUrl);
        }

        try (InputStream inputStream = entity.getContent()) {
          JsonNode releases = JsonUtil.parse(inputStream);

          if (releases.isArray()) {
            for (JsonNode release : releases) {
              boolean isPreRelease =
                  release.has("prerelease") && release.get("prerelease").asBoolean();

              // Skip pre-releases if configured to do so. Note this only catches what the
              // repository itself flags: Apache Hop, for one, tags every release "-rc1" and marks
              // none of them as a pre-release, so this alone will not spare you its back
              // catalogue. The version floor below is what does.
              if (!includePreReleases && isPreRelease) {
                continue;
              }

              String tagName = text(release, "tag_name");
              String releaseId = text(release, "id");

              if (isOlderThanFloor(tagName)) {
                continue;
              }

              // A release has to be identifiable across polls or it would be reported again every
              // time. The tag is the stable name for one; the numeric id covers a release that
              // has not been tagged. NotificationService qualifies this with the source, so it
              // only has to be unique within this repository.
              String localId = tagName != null ? tagName : releaseId;
              if (localId == null) {
                LogChannel.UI.logDetailed(
                    "Skipping a release of "
                        + repositoryOwner
                        + "/"
                        + repositoryName
                        + " that has neither a tag nor an id");
                continue;
              }

              String repository = repositoryOwner + "/" + repositoryName;
              String name = text(release, "name");
              // A release named after its own tag says nothing the title does not already say.
              if (name == null || name.isEmpty() || name.equals(tagName)) {
                name = repository + " " + localId;
              }
              String body = text(release, "body");
              // Clean up body - remove excessive whitespace
              if (body != null) {
                body = body.trim();
                if (body.isEmpty()) {
                  body = null;
                }
              }
              String htmlUrl = text(release, "html_url");
              String publishedAt = text(release, "published_at");
              Date publishedDate = parseGitHubDate(publishedAt);
              if (publishedDate == null) {
                publishedDate = new Date();
              }

              // Determine category
              NotificationCategory category = NotificationCategory.RELEASE;

              // Prepare message/description
              String message;
              if (body != null && !body.isEmpty()) {
                message = truncateBody(body);
              } else {
                // If no body, provide a default message
                message = repository + " " + localId + " is now available.";
              }

              Notification notification =
                  new Notification(
                      localId,
                      name,
                      message,
                      providerName,
                      providerId,
                      htmlUrl,
                      publishedDate,
                      NotificationPriority.INFO,
                      category);

              if (tagName != null) {
                notification.setVersion(tagName);
              }

              notifications.add(notification);
            }
          }
        }

        // Only now that the answer has been read and understood. Remembering the validator any
        // earlier means a parse that fails still arms the next request's If-None-Match: the source
        // would answer 304 forever, this method would return the releases it never managed to read
        // (none), and because that is not a failure the error banner would clear itself.
        conditional.remember(response);
      }
      lastFetched = new ArrayList<>(notifications);
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      // Reported to the user through the panel's error banner. NotificationService catches this
      // per provider, so one unreachable source does not stop the others.
      throw new HopException(
          "Could not read the releases of "
              + repositoryOwner
              + "/"
              + repositoryName
              + " from GitHub: "
              + e.getMessage(),
          e);
    }

    return notifications;
  }

  /**
   * Explain a non-OK GitHub response in terms the user can act on. The rate limit in particular is
   * easy to hit: unauthenticated calls are counted per IP address, so a shared office address can
   * exhaust the hourly allowance between several Hop installations.
   *
   * @param statusCode The HTTP status code
   * @param response The response, read for the rate limit headers
   * @param apiUrl The URL that was requested
   * @return A message for the notification panel
   */
  private String describeStatus(int statusCode, ClassicHttpResponse response, String apiUrl) {
    if ((statusCode == 403 || statusCode == 429) && isRateLimited(response)) {
      String hint =
          hasCredentials()
              ? " The limit resets within the hour."
              : " Unauthenticated requests are limited per IP address and shared with everyone"
                  + " behind it; configuring a token on this source raises the limit considerably.";
      return "GitHub rate limit reached for " + repositoryOwner + "/" + repositoryName + "." + hint;
    }
    if (statusCode == 404) {
      String hint =
          hasCredentials()
              ? " Check that the token grants access to it."
              : " A private repository answers the same way when no credentials are sent.";
      return "GitHub repository "
          + repositoryOwner
          + "/"
          + repositoryName
          + " was not found."
          + hint;
    }
    if (statusCode == 401) {
      return "GitHub rejected the credentials configured for "
          + repositoryOwner
          + "/"
          + repositoryName
          + ".";
    }
    return "GitHub returned HTTP " + statusCode + " for " + apiUrl;
  }

  /**
   * Whether this release is at or below the configured floor, and so not worth reporting.
   *
   * @param tagName The release tag, which is where the version lives
   * @return true when the release should be skipped
   */
  private boolean isOlderThanFloor(String tagName) {
    if (minimumVersion == null || minimumVersion.trim().isEmpty() || tagName == null) {
      return false;
    }
    return VersionCompare.compare(tagName, minimumVersion.trim()) <= 0;
  }

  /**
   * @param minimumVersion Only report releases newer than this; null or empty reports everything
   */
  public void setMinimumVersion(String minimumVersion) {
    this.minimumVersion = minimumVersion;
  }

  /**
   * Read a string field, treating a field that is present but null as absent.
   *
   * <p>GitHub answers with an explicit {@code null} rather than leaving the field out - a release
   * created from a tag alone has {@code "name": null} and {@code "body": null}. {@code has()} is
   * true for those, and {@code NullNode.asText()} is the four characters {@code null}, so reading
   * them that way titles the notification "null" instead of falling back to the repository and tag.
   *
   * @param node The release
   * @param field The field to read
   * @return The value, or null when the field is missing or null
   */
  static String text(JsonNode node, String field) {
    return node.hasNonNull(field) ? node.get(field).asText() : null;
  }

  private boolean hasCredentials() {
    return password != null && !password.trim().isEmpty();
  }

  private boolean isRateLimited(ClassicHttpResponse response) {
    org.apache.hc.core5.http.Header remaining = response.getFirstHeader("X-RateLimit-Remaining");
    return remaining != null && "0".equals(remaining.getValue().trim());
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
    // GitHub timestamps are ISO 8601 in UTC: 2023-12-01T10:00:00Z. Parsing that with a pattern
    // that quotes the Z reads it as a local time, which moved every release by the machine's
    // offset and then fed that into the "days to go back" filter.
    return NotificationDates.parseIso(dateStr);
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

  /**
   * A personal access token also lifts the rate limit: unauthenticated calls are counted per IP
   * address and shared by everyone behind it, authenticated ones per token.
   *
   * @param username The user name, may be null
   * @param password The password or token, may be null
   */
  public void setCredentials(String username, String password) {
    this.username = username;
    this.password = password;
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
