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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;

/** Downloads Maven layout artifacts over HTTP(S). */
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

  public Path downloadZip(String repositoryBaseUrl, MavenCoordinates coordinates, Path targetFile)
      throws HopException {
    String base = repositoryBaseUrl.endsWith("/") ? repositoryBaseUrl : repositoryBaseUrl + "/";
    String url = base + coordinates.zipRepositoryPath();
    log.logBasic("Downloading " + coordinates.gav() + " from " + url);
    try {
      Files.createDirectories(targetFile.getParent());
      HttpRequest request =
          HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofMinutes(30)).GET().build();
      HttpResponse<InputStream> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
      if (response.statusCode() != 200) {
        throw new HopException(
            "HTTP " + response.statusCode() + " downloading plugin zip from " + url);
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
      throw new HopException("Failed to download plugin zip from " + url, e);
    }
  }
}
