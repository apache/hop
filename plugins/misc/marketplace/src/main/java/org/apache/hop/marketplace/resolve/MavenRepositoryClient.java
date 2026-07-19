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
import java.util.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.marketplace.config.MarketplaceRepository;

/** Downloads Maven layout artifacts over HTTP(S), with optional Basic authentication. */
public class MavenRepositoryClient {

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
    String url = base + coordinates.zipRepositoryPath();
    return download(url, repository, coordinates.gav(), targetFile);
  }

  /**
   * @deprecated use {@link #downloadZip(MarketplaceRepository, MavenCoordinates, Path)}
   */
  public Path downloadZip(String repositoryBaseUrl, MavenCoordinates coordinates, Path targetFile)
      throws HopException {
    MarketplaceRepository repo = new MarketplaceRepository("adhoc", repositoryBaseUrl);
    return downloadZip(repo, coordinates, targetFile);
  }

  public Path downloadArtifact(
      MarketplaceRepository repository, String relativePath, String label, Path targetFile)
      throws HopException {
    String base = repository.normalizedUrl();
    String url = base + (relativePath.startsWith("/") ? relativePath.substring(1) : relativePath);
    return download(url, repository, label, targetFile);
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
        throw new HopException(
            "HTTP "
                + response.statusCode()
                + " downloading "
                + label
                + " from "
                + url
                + ". Set repository username/password in hop-config.json marketplace.repositories,"
                + " or export HOP_MARKETPLACE_USERNAME / HOP_MARKETPLACE_PASSWORD"
                + " (or ARTIFACTORY_USER / ARTIFACTORY_PASSWORD).");
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
}
