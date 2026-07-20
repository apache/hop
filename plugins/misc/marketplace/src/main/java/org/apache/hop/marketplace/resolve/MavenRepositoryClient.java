/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.marketplace.resolve;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.marketplace.config.MarketplaceRepository;

/** Downloads Maven layout artifacts over HTTP(S), with optional Basic authentication. */
public class MavenRepositoryClient {

  private static final Pattern SNAPSHOT_ZIP_VALUE =
      Pattern.compile(
          "<snapshotVersion>\\s*<extension>zip</extension>\\s*<value>([^<]+)</value>",
          Pattern.DOTALL);
  private static final Pattern SNAPSHOT_TIMESTAMP =
      Pattern.compile(
          "<snapshot>\\s*<timestamp>([^<]+)</timestamp>\\s*<buildNumber>([^<]+)</buildNumber>",
          Pattern.DOTALL);

  private final HttpClient httpClient;
  private final ILogChannel log;

  public MavenRepositoryClient(ILogChannel log) {
    this.log = log;
    this.httpClient =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
  }

  public MavenRepositoryClient(ILogChannel log, HttpClient httpClient) {
    this.log = log;
    this.httpClient = httpClient;
  }

  public Path downloadZip(
      MarketplaceRepository repository, MavenCoordinates coordinates, Path targetFile)
      throws HopException {
    String base = repository.normalizedUrl();
    String relative = resolveZipRelativePath(repository, coordinates);
    String url = base + relative;
    return download(url, repository, coordinates.gav(), targetFile);
  }

  /**
   * Try each repository in order until a zip is downloaded. Aggregates per-repo errors if all fail
   * (fallback chain).
   */
  public Path downloadZipWithFallback(
      List<MarketplaceRepository> repositories, MavenCoordinates coordinates, Path targetFile)
      throws HopException {
    if (repositories == null || repositories.isEmpty()) {
      throw new HopException("No marketplace repositories configured");
    }
    List<String> errors = new ArrayList<>();
    for (MarketplaceRepository repository : repositories) {
      if (repository == null || !repository.isEnabled()) {
        continue;
      }
      try {
        log.logBasic(
            "Trying repository '"
                + repository.displayName()
                + "' ("
                + repository.normalizedUrl()
                + ") for "
                + coordinates.gav());
        return downloadZip(repository, coordinates, targetFile);
      } catch (HopException e) {
        String msg =
            repository.getId()
                + " @ "
                + repository.normalizedUrl()
                + " → "
                + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        errors.add(msg);
        log.logBasic("Repository attempt failed: " + msg);
      }
    }
    throw new HopException(
        "Could not download "
            + coordinates.gav()
            + " from any configured repository:\n  - "
            + String.join("\n  - ", errors));
  }

  /**
   * @deprecated use {@link #downloadZip(MarketplaceRepository, MavenCoordinates, Path)}
   */
  public Path downloadZip(String repositoryBaseUrl, MavenCoordinates coordinates, Path targetFile)
      throws HopException {
    MarketplaceRepository repo = new MarketplaceRepository("adhoc", repositoryBaseUrl);
    return downloadZip(repo, coordinates, targetFile);
  }

  public void downloadArtifact(
      MarketplaceRepository repository, String relativePath, String label, Path targetFile)
      throws HopException {
    String base = repository.normalizedUrl();
    String url = base + (relativePath.startsWith("/") ? relativePath.substring(1) : relativePath);
    download(url, repository, label, targetFile);
  }

  /**
   * For release versions: {@code g/a/v/a-v.zip}. For {@code *-SNAPSHOT}, resolve the unique
   * timestamped file name from {@code maven-metadata.xml} (as deployed by Maven / Nexus).
   */
  String resolveZipRelativePath(MarketplaceRepository repository, MavenCoordinates coordinates)
      throws HopException {
    String version = coordinates.version();
    if (version == null || !version.endsWith("-SNAPSHOT")) {
      return coordinates.zipRepositoryPath();
    }

    String groupPath = coordinates.groupId().replace('.', '/');
    String metadataPath =
        groupPath + "/" + coordinates.artifactId() + "/" + version + "/maven-metadata.xml";
    String metadataUrl = repository.normalizedUrl() + metadataPath;
    log.logDetailed("Resolving SNAPSHOT zip via " + metadataUrl);
    String metadata = getText(metadataUrl, repository);
    String unique = parseSnapshotZipValue(metadata, coordinates.artifactId(), version);
    if (unique == null) {
      // Fall back to non-unique name (some repos allow it)
      log.logBasic(
          "Could not parse SNAPSHOT zip from maven-metadata.xml; trying non-unique file name");
      return coordinates.zipRepositoryPath();
    }
    return groupPath
        + "/"
        + coordinates.artifactId()
        + "/"
        + version
        + "/"
        + coordinates.artifactId()
        + "-"
        + unique
        + ".zip";
  }

  static String parseSnapshotZipValue(
      String metadataXml, String artifactId, String snapshotVersion) {
    if (metadataXml == null || metadataXml.isBlank()) {
      return null;
    }
    Matcher m = SNAPSHOT_ZIP_VALUE.matcher(metadataXml);
    if (m.find()) {
      return m.group(1).trim();
    }
    // timestamp + buildNumber → 2.19.0-20260719.204953-1
    Matcher t = SNAPSHOT_TIMESTAMP.matcher(metadataXml);
    if (t.find()) {
      String base = snapshotVersion.substring(0, snapshotVersion.length() - "-SNAPSHOT".length());
      return base + "-" + t.group(1).trim() + "-" + t.group(2).trim();
    }
    return null;
  }

  private String getText(String url, MarketplaceRepository repository) throws HopException {
    try {
      HttpRequest.Builder builder =
          HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofMinutes(2)).GET();
      applyBasicAuth(builder, repository);
      HttpResponse<String> response =
          httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() == 401 || response.statusCode() == 403) {
        throw new HopException(authFailureMessage(response.statusCode(), url, repository));
      }
      if (response.statusCode() != 200) {
        throw new HopException(
            "HTTP " + response.statusCode() + " fetching maven-metadata from " + url);
      }
      return response.body();
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new HopException("Failed to fetch " + url, e);
    }
  }

  private Path download(String url, MarketplaceRepository repository, String label, Path targetFile)
      throws HopException {
    log.logBasic("Downloading " + label + " from " + url);
    try {
      Files.createDirectories(targetFile.getParent());
      HttpRequest.Builder builder =
          HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofMinutes(30)).GET();
      applyBasicAuth(builder, repository);
      HttpResponse<InputStream> response =
          httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
      if (response.statusCode() == 401 || response.statusCode() == 403) {
        throw new HopException(authFailureMessage(response.statusCode(), url, repository));
      }
      if (response.statusCode() != 200) {
        throw new HopException(
            "HTTP " + response.statusCode() + " downloading " + label + " from " + url);
      }
      try (InputStream in = response.body()) {
        Files.copy(in, targetFile, StandardCopyOption.REPLACE_EXISTING);
      }
      log.logBasic("Downloaded " + targetFile + " (" + Files.size(targetFile) + " bytes)");
      return targetFile;
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new HopException("Failed to download " + label + " from " + url, e);
    }
  }

  static void applyBasicAuth(HttpRequest.Builder builder, MarketplaceRepository repository) {
    if (repository == null || !repository.hasCredentials()) {
      return;
    }
    String user = repository.effectiveUsername();
    String pass = repository.effectivePassword();
    if (StringUtils.isAnyBlank(user, pass)) {
      return;
    }
    String token =
        Base64.getEncoder().encodeToString((user + ":" + pass).getBytes(StandardCharsets.UTF_8));
    builder.header("Authorization", "Basic " + token);
  }

  private static String authFailureMessage(
      int status, String url, MarketplaceRepository repository) {
    StringBuilder sb = new StringBuilder();
    sb.append("HTTP ").append(status).append(" from ").append(url).append(". ");
    if (repository != null && repository.hasCredentials()) {
      sb.append("Basic auth was sent as user '")
          .append(repository.effectiveUsername())
          .append("'. Wrong credentials make Nexus reject the request even when anonymous read")
          .append(" would work. Clear username/password in hop-config marketplace.repositories")
          .append(" and unset HOP_MARKETPLACE_USERNAME / HOP_MARKETPLACE_PASSWORD, or fix the")
          .append(" password.");
    } else {
      sb.append("No credentials were sent (anonymous). Enable anonymous read on the repository")
          .append(" (docker/marketplace-nexus/start.sh does this), or set")
          .append(" HOP_MARKETPLACE_USERNAME / HOP_MARKETPLACE_PASSWORD for a private repo.");
    }
    return sb.toString();
  }
}
