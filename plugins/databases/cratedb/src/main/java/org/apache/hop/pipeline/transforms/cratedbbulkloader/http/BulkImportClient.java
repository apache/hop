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

package org.apache.hop.pipeline.transforms.cratedbbulkloader.http;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.transforms.cratedbbulkloader.http.exceptions.CrateDBHopException;
import org.apache.hop.pipeline.transforms.cratedbbulkloader.http.exceptions.UnauthorizedCrateDBAccessException;

public class BulkImportClient {

  private final String httpEndpoint;
  private final String authorizationHeader;

  public BulkImportClient(String httpEndpoint, String username, String password) {
    this.httpEndpoint = httpEndpoint;
    this.authorizationHeader = header(username, password);
  }

  private String header(String username, String password) {
    String auth = username + ":" + password;
    String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
    return "Basic " + encodedAuth;
  }

  public HttpBulkImportResponse batchInsert(
      String schema, String table, String[] columns, List<Object[]> args)
      throws HopException, CrateDBHopException, IOException {
    ObjectMapper mapper = new ObjectMapper();
    BulkImportRequest body = new BulkImportRequest(schema + "." + table, List.of(columns), args);
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(httpEndpoint))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("Authorization", authorizationHeader)
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    mapper.writeValueAsString(body), StandardCharsets.UTF_8))
            .build();

    HttpClient client = HttpClient.newHttpClient();

    final HttpResponse<String> httpResponse;
    try {
      httpResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      throw new HopException("Couldn't process the request", e);
    }
    // For crateDB there's no difference between 401 and 403, it always returns 401, and
    // then the internal error code helps to distinguish between the two.
    return switch (httpResponse.statusCode()) {
      case 200 -> HttpBulkImportResponse.fromHttpResponse(httpResponse);
      case 401 -> throw new UnauthorizedCrateDBAccessException(httpResponse.body());
      default -> throw new CrateDBHopException(httpResponse.statusCode(), httpResponse.body());
    };
  }

  private static class BulkImportRequest {
    private String stmt;
    private List<Object[]> bulkArgs;

    public BulkImportRequest(String table, List<String> columns, List<Object[]> bulkArgs) {
      final String argsPlaceholders =
          columns.stream().map(c -> "?").collect(Collectors.joining(", "));
      this.stmt =
          "INSERT INTO "
              + table
              + " ("
              + String.join(", ", columns)
              + ") VALUES ("
              + argsPlaceholders
              + ")";
      this.bulkArgs = bulkArgs;
    }

    public String getStmt() {
      return stmt;
    }

    @JsonProperty("bulk_args")
    public List<Object[]> getBulkArgs() {
      return bulkArgs;
    }
  }
}
