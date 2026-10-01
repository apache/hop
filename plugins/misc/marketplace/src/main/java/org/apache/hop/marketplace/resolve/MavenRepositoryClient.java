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
import java.io.OutputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.marketplace.config.MarketplaceHttp;
import org.apache.hop.marketplace.config.MarketplaceRepository;

/** Downloads Maven layout artifacts over HTTP(S), with optional Basic authentication. */
public class MavenRepositoryClient {

  /**
   * Read chunk for the download loop. Small enough that cancel feels immediate on a slow link,
   * large enough not to add syscall overhead on a fast one.
   */
  private static final int TRANSFER_BUFFER_SIZE = 64 * 1024;

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
    return downloadZip(repository, coordinates, targetFile, ITransferListener.NONE);
  }

  public Path downloadZip(
      MarketplaceRepository repository,
      MavenCoordinates coordinates,
      Path targetFile,
      ITransferListener listener)
      throws HopException {
    String url;
    if (StringUtils.isNotBlank(repository.getUrlTemplate())) {
      // Non-Maven host (release assets, static files): no layout, no metadata lookup.
      url = expandUrlTemplate(repository.getUrlTemplate(), coordinates);
    } else {
      url = repository.normalizedUrl() + resolveZipRelativePath(repository, coordinates);
    }
    return download(url, repository, coordinates.gav(), targetFile, listener);
  }

  /**
   * Expand a repository {@code urlTemplate} for one artifact. Supported placeholders: {@code
   * ${groupId}}, {@code ${groupPath}}, {@code ${artifactId}} and {@code ${version}}.
   */
  static String expandUrlTemplate(String template, MavenCoordinates coordinates)
      throws HopException {
    if (StringUtils.isBlank(template)) {
      throw new HopException("Repository urlTemplate is empty");
    }
    String expanded =
        template
            .replace("${groupId}", coordinates.groupId())
            .replace("${groupPath}", coordinates.groupId().replace('.', '/'))
            .replace("${artifactId}", coordinates.artifactId())
            .replace("${version}", coordinates.version());
    if (expanded.contains("${")) {
      throw new HopException(
          "Unresolved placeholder in urlTemplate '"
              + template
              + "'. Supported: ${groupId}, ${groupPath}, ${artifactId}, ${version}");
    }
    return expanded;
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
    downloadArtifact(repository, relativePath, label, targetFile, ITransferListener.NONE);
  }

  public void downloadArtifact(
      MarketplaceRepository repository,
      String relativePath,
      String label,
      Path targetFile,
      ITransferListener listener)
      throws HopException {
    String base = repository.normalizedUrl();
    String url = base + (relativePath.startsWith("/") ? relativePath.substring(1) : relativePath);
    download(url, repository, label, targetFile, listener);
  }

  /**
   * For release versions: {@code g/a/v/a-v.zip}. For {@code *-SNAPSHOT} (including unique
   * timestamped forms like {@code 1.0.0-20260721.105615-1} from Nexus search), resolve the unique
   * file name from {@code maven-metadata.xml} under the base SNAPSHOT directory.
   */
  String resolveZipRelativePath(MarketplaceRepository repository, MavenCoordinates coordinates)
      throws HopException {
    String version = SnapshotVersions.toBaseVersion(coordinates.version());
    if (version == null || !version.endsWith("-SNAPSHOT")) {
      return coordinates.zipRepositoryPath();
    }

    // When the caller already passed a unique SNAPSHOT, place that file under the base folder
    // without requiring metadata (Nexus search already named the asset).
    if (SnapshotVersions.isUniqueSnapshot(coordinates.version())) {
      String groupPath = coordinates.groupId().replace('.', '/');
      return groupPath
          + "/"
          + coordinates.artifactId()
          + "/"
          + version
          + "/"
          + coordinates.artifactId()
          + "-"
          + coordinates.version().trim()
          + ".zip";
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
      return groupPath
          + "/"
          + coordinates.artifactId()
          + "/"
          + version
          + "/"
          + coordinates.artifactId()
          + "-"
          + version
          + ".zip";
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
      HttpResponse<String> response =
          MarketplaceHttp.send(
              httpClient,
              url,
              Duration.ofMinutes(2),
              repository,
              HttpResponse.BodyHandlers.ofString());
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

  private Path download(
      String url,
      MarketplaceRepository repository,
      String label,
      Path targetFile,
      ITransferListener listener)
      throws HopException {
    log.logBasic("Downloading " + label + " from " + url);
    ITransferListener progress = listener == null ? ITransferListener.NONE : listener;
    try {
      Files.createDirectories(targetFile.getParent());
      HttpResponse<InputStream> response =
          MarketplaceHttp.send(
              httpClient,
              url,
              Duration.ofMinutes(30),
              repository,
              HttpResponse.BodyHandlers.ofInputStream());
      if (response.statusCode() == 401 || response.statusCode() == 403) {
        throw new HopException(authFailureMessage(response.statusCode(), url, repository));
      }
      if (response.statusCode() != 200) {
        throw new HopException(
            "HTTP " + response.statusCode() + " downloading " + label + " from " + url);
      }
      long totalBytes = contentLength(response);
      progress.started(label, totalBytes);
      long written = copyWithProgress(response.body(), targetFile, totalBytes, progress);
      log.logBasic("Downloaded " + targetFile + " (" + written + " bytes)");
      return targetFile;
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      deleteQuietly(targetFile);
      throw new HopException("Failed to download " + label + " from " + url, e);
    } catch (HopException e) {
      // Cancelled or rejected mid-transfer: never leave a truncated zip behind for the unzip step.
      deleteQuietly(targetFile);
      throw e;
    }
  }

  /**
   * Stream the response body to {@code targetFile}, reporting every chunk so the caller can move a
   * progress bar, and honouring cancellation between chunks.
   *
   * <p>This is why {@code Files.copy(in, target)} is not used: it consumes the whole stream in one
   * call, leaving no hook for progress or cancel.
   *
   * @return the number of bytes written
   */
  private static long copyWithProgress(
      InputStream body, Path targetFile, long totalBytes, ITransferListener listener)
      throws IOException, HopException {
    byte[] buffer = new byte[TRANSFER_BUFFER_SIZE];
    long written = 0;
    try (InputStream in = body;
        OutputStream out = Files.newOutputStream(targetFile)) {
      int read;
      while ((read = in.read(buffer)) != -1) {
        if (listener.isCancelled()) {
          throw new HopException("Download cancelled");
        }
        out.write(buffer, 0, read);
        written += read;
        listener.transferred(written, totalBytes);
      }
    }
    // One last callback carrying the exact byte count. A throttling listener must let this one
    // through rather than coalesce it away, or its bar freezes just short of the total.
    listener.transferred(written, totalBytes);
    return written;
  }

  /**
   * Size of the body from {@code Content-Length}, or -1 when the server does not say — chunked
   * transfer encoding, or a compressed response whose header describes the encoded length rather
   * than the file. Callers must treat -1 as "show an indeterminate bar", not as an error.
   */
  private static long contentLength(HttpResponse<InputStream> response) {
    if (response.headers().firstValue("content-encoding").isPresent()) {
      // Content-Length then counts encoded bytes, which would not match what we write to disk.
      return -1L;
    }
    return response.headers().firstValueAsLong("content-length").orElse(-1L);
  }

  private void deleteQuietly(Path file) {
    try {
      Files.deleteIfExists(file);
    } catch (IOException e) {
      log.logDetailed("Unable to remove partial download " + file + ": " + e.getMessage());
    }
  }

  static void applyBasicAuth(HttpRequest.Builder builder, MarketplaceRepository repository) {
    MarketplaceHttp.applyAuth(builder, repository);
  }

  /**
   * Both call sites are guarded on 401/403, so {@link MarketplaceHttp#authHint} always has
   * something to say. It knows which authentication type was used and whether the credentials came
   * from the environment, which this method used to restate for Basic only.
   */
  private static String authFailureMessage(
      int status, String url, MarketplaceRepository repository) {
    return "HTTP " + status + " from " + url + MarketplaceHttp.authHint(status, repository);
  }
}
