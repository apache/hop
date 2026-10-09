/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.beam.transforms.elasticsearch;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;

/** Shared configuration; HTTPS keeps the client's verified default trust configuration. */
public final class BeamElasticsearchConfig {
  /**
   * Jackson 2.21.5 parses JSON numbers as {@code double} unless told otherwise, and {@code double}
   * turns values past about 1e308 into {@code Infinity} and drops digits past the 53-bit mantissa.
   * BigDecimal and BigInteger keep the token's value. {@code writeValueAsString} on this mapper
   * writes those nodes back; {@code JsonNode.toString()} uses a different mapper.
   */
  private static final ObjectMapper JSON_MAPPER =
      new ObjectMapper()
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
          .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN);

  private BeamElasticsearchConfig() {}

  public static ElasticsearchIO.ConnectionConfiguration connection(
      IVariables variables,
      String hosts,
      String index,
      String type,
      String username,
      String password)
      throws HopException {
    String[] addresses = required(variables, hosts, "Hosts").split(",", -1);
    for (int i = 0; i < addresses.length; i++) {
      addresses[i] = addresses[i].trim();
      try {
        URI uri = new URI(addresses[i]);
        if (!("https".equalsIgnoreCase(uri.getScheme()) || "http".equalsIgnoreCase(uri.getScheme()))
            || uri.getHost() == null
            || uri.getUserInfo() != null
            || uri.getQuery() != null
            || uri.getFragment() != null
            || (uri.getPath() != null && !uri.getPath().isEmpty() && !"/".equals(uri.getPath()))
            || uri.getPort() > 65535
            || uri.getPort() == 0) {
          throw new HopException(
              "Hosts must be HTTP(S) origins without credentials, paths, queries or fragments");
        }
      } catch (java.net.URISyntaxException e) {
        // Do not include a URL: it might contain credentials.
        throw new HopException("Invalid Elasticsearch host URI");
      }
    }
    String resolvedIndex = required(variables, index, "Index");
    String resolvedType = optional(variables, type, "Document type");
    if (resolvedIndex.matches(".*[/\\?#\\s].*") || resolvedType.matches(".*[/\\?#\\s].*")) {
      throw new HopException("Index and document type must not contain paths or whitespace");
    }
    String user = optional(variables, username, "Username");
    String secret = password == null ? "" : variables.resolve(password);
    if (secret == null) {
      secret = "";
    }
    checkResolved(secret, "Password");
    if (user.isBlank() != secret.isEmpty()) {
      throw new HopException("Basic authentication requires both username and password");
    }
    ElasticsearchIO.ConnectionConfiguration connection =
        ElasticsearchIO.ConnectionConfiguration.create(addresses, resolvedIndex, resolvedType);
    if (!user.isBlank()) {
      connection =
          connection
              .withUsername(user)
              .withPassword(Encr.decryptPasswordOptionallyEncrypted(secret));
    }
    return connection;
  }

  public static long positiveLong(IVariables variables, String value, String label)
      throws HopException {
    try {
      long result = Long.parseLong(required(variables, value, label));
      if (result > 0) {
        return result;
      }
    } catch (NumberFormatException e) {
      /* Fall through to the configuration error. */
    }
    throw new HopException(label + " must be a positive integer");
  }

  public static String required(IVariables variables, String value, String label)
      throws HopException {
    String resolved = optional(variables, value, label);
    if (resolved.isBlank()) {
      throw new HopException(label + " is required");
    }
    return resolved;
  }

  public static String optional(IVariables variables, String value, String label)
      throws HopException {
    String resolved = value == null ? "" : variables.resolve(value);
    if (resolved == null) {
      resolved = "";
    }
    checkResolved(resolved, label);
    return resolved.trim();
  }

  public static JsonNode jsonObject(String value, String label) throws HopException {
    try {
      JsonNode json = JSON_MAPPER.readTree(value);
      if (json == null || !json.isObject()) {
        throw new HopException(label + " must be a JSON object");
      }
      return json;
    } catch (IOException e) {
      // Parser exceptions include document contents; do not disclose them in errors.
      throw new HopException(label + " must be a valid JSON object");
    }
  }

  /** One compact JSON object. Numeric tokens keep their value; only whitespace is normalized. */
  public static String compactJsonObject(String value, String label) throws HopException {
    try {
      return JSON_MAPPER.writeValueAsString(jsonObject(value, label));
    } catch (IOException e) {
      throw new HopException(label + " must be a valid JSON object");
    }
  }

  private static void checkResolved(String value, String label) throws HopException {
    if (value.contains("${") || value.matches(".*%%[^%]+%%.*")) {
      throw new HopException(label + " contains an unresolved variable");
    }
  }
}
