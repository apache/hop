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
package org.apache.hop.pipeline.transforms.chunker.document.metadata;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hop.pipeline.transforms.chunker.document.DocumentNode;
import org.apache.hop.pipeline.transforms.chunker.document.SecretRedaction;
import org.junit.jupiter.api.Test;

class HopMetadataJsonParserTest {

  private static final String RDBMS_JSON =
      """
            {
              "rdbms": {
                "H2": {
                  "databaseName": "hop-samples",
                  "pluginId": "H2",
                  "hostname": "localhost",
                  "port": "8082",
                  "username": "hop"
                }
              },
              "name": "hop-samples"
            }
            """;

  @Test
  void parsesRdbmsMetadataIntoSections() {
    DocumentNode root = new HopMetadataJsonParser().parse(RDBMS_JSON);
    List<DocumentNode.DocumentSection> sections = root.flattenSections();

    assertTrue(sections.stream().anyMatch(s -> s.breadcrumb().contains("Overview")));
    assertTrue(
        sections.stream()
            .anyMatch(
                s ->
                    s.breadcrumb().contains("Connection: H2")
                        && s.getBody().contains("hop-samples")));
  }

  @Test
  void parsesTextCorpusFormatWithConfigurationBlock() {
    String text =
        """
                Hop metadata item 'hop-samples'
                Category: rdbms
                Path: metadata/rdbms/hop-samples.json
                Configuration:
                """
            + RDBMS_JSON.strip();

    DocumentNode root = new HopMetadataJsonParser().parse(text);
    List<DocumentNode.DocumentSection> sections = root.flattenSections();

    assertTrue(sections.stream().anyMatch(s -> s.breadcrumb().contains("Connection: H2")));
  }

  @Test
  void connectionChunksDoNotCarryCredentials() {
    String json =
        """
        {
          "rdbms": {
            "PG": {
              "databaseName": "hop",
              "pluginId": "POSTGRESQL",
              "hostname": "localhost",
              "username": "hop",
              "password": "Encrypted 2be98afc86aa7f2e4bb18bd63c99dbdde"
            }
          },
          "name": "pg"
        }
        """;
    String all =
        new HopMetadataJsonParser()
            .parse(json).flattenSections().stream()
                .map(section -> String.join(" > ", section.getPath()) + " " + section.getBody())
                .collect(Collectors.joining("\n"));

    assertFalse(all.contains("2be98afc86aa7f2e4bb18bd63c99dbdde"), all);
    assertFalse(all.contains("Encrypted "), all);
    assertTrue(all.contains("localhost"), all);
  }

  @Test
  void genericMetadataChunksDoNotCarryCredentials() {
    // Not an rdbms connection, so this goes through the generic pretty-printed leaf path.
    String json =
        """
        {
          "name": "prod-rest",
          "restConnection": {
            "baseUrl": "https://api.example.com",
            "authType": "BEARER",
            "bearerToken": "tok-abcdef123456",
            "proxyPassword": "hunter2"
          }
        }
        """;
    String all =
        new HopMetadataJsonParser()
            .parse(json).flattenSections().stream()
                .map(section -> String.join(" > ", section.getPath()) + " " + section.getBody())
                .collect(Collectors.joining("\n"));

    assertFalse(all.contains("tok-abcdef123456"), all);
    assertFalse(all.contains("hunter2"), all);
    assertTrue(all.contains("https://api.example.com"), all);
  }

  @Test
  void variableValuesWithCredentialNamesAreRedacted() {
    // Project configs, environment files and run configurations all store variables this way.
    String json =
        """
        {
          "name": "prod",
          "variables": [
            { "name": "API_TOKEN", "value": "tok123", "description": "" },
            { "name": "DB_PASSWORD", "value": "s3cr3t", "description": "" },
            { "name": "REGION", "value": "eu-west-1", "description": "" }
          ]
        }
        """;
    String all =
        new HopMetadataJsonParser()
            .parse(json).flattenSections().stream()
                .map(section -> String.join(" > ", section.getPath()) + " " + section.getBody())
                .collect(Collectors.joining("\n"));

    assertFalse(all.contains("tok123"), all);
    assertFalse(all.contains("s3cr3t"), all);
    assertTrue(all.contains("API_TOKEN"), all);
    assertTrue(all.contains("eu-west-1"), all);
  }

  @Test
  void unitTestVariableValuesWithCredentialNamesAreRedacted() {
    // Pipeline unit tests store their variables as key/value pairs.
    String json =
        """
        {
          "name": "0001 UNIT",
          "variableValues": [
            { "key": "DB_PASSWORD", "value": "unit-test-s3cr3t" },
            { "key": "REGION", "value": "eu-west-1" }
          ]
        }
        """;
    String all =
        new HopMetadataJsonParser()
            .parse(json).flattenSections().stream()
                .map(section -> String.join(" > ", section.getPath()) + " " + section.getBody())
                .collect(Collectors.joining("\n"));

    assertFalse(all.contains("unit-test-s3cr3t"), all);
    assertTrue(all.contains("DB_PASSWORD"), all);
    assertTrue(all.contains("eu-west-1"), all);
  }

  @Test
  void topLevelCredentialValuesAreRedacted() {
    // Flat connection metadata, such as a mail server connection, keeps its secrets at the top.
    String json =
        """
        {
          "name": "smtp",
          "serverHost": "smtp.example.com",
          "username": "hop",
          "password": "Encrypted 2be98afc86aa7f2e4cb79ce10df90acde",
          "accessToken": "ghp_realLookingToken123"
        }
        """;
    String all =
        new HopMetadataJsonParser()
            .parse(json).flattenSections().stream()
                .map(section -> String.join(" > ", section.getPath()) + " " + section.getBody())
                .collect(Collectors.joining("\n"));

    assertFalse(all.contains("2be98afc86aa"), all);
    assertFalse(all.contains("ghp_realLookingToken123"), all);
    assertTrue(all.contains("smtp.example.com"), all);
  }

  @Test
  void containerValueOfASecretEntryIsRedacted() {
    String json =
        """
        {
          "name": "prod",
          "variables": [
            { "name": "API_TOKENS", "value": ["tok-1", "tok-2"] },
            { "name": "DB_PASSWORD", "value": { "primary": "s3cr3t" } }
          ]
        }
        """;
    String all = allSections(json);

    assertFalse(all.contains("tok-1"), all);
    assertFalse(all.contains("tok-2"), all);
    assertFalse(all.contains("s3cr3t"), all);
  }

  @Test
  void nullsBooleansAndVariableReferencesAreKept() {
    String json =
        """
        {
          "name": "prod",
          "usePassword": false,
          "password": null,
          "proxyPassword": "${PROXY_PASSWORD}",
          "variables": [
            { "name": "DB_PASSWORD", "value": null },
            { "name": "USE_TOKEN", "value": true },
            { "name": "API_TOKEN", "value": "${VAULT_API_TOKEN}" }
          ],
          "connection": { "useSecret": true, "secret": null }
        }
        """;
    String all = allSections(json);

    assertFalse(all.contains(SecretRedaction.REDACTED), all);
    assertTrue(all.contains("${PROXY_PASSWORD}"), all);
    assertTrue(all.contains("${VAULT_API_TOKEN}"), all);
  }

  @Test
  void restConnectionCredentialsAreRedacted() {
    // A rest-connection as Hop stores it, with plain values as in a hand-edited file.
    String json =
        """
        {
          "name": "openai",
          "baseUrl": "https://api.example.com",
          "auth_type": "API_KEY",
          "auth_header_name": "X-API-Key",
          "auth_header_prefix": "Bearer",
          "auth_header_value": "sk-live-123",
          "oauth2_authorization_url": "https://auth.example.com/authorize",
          "headers": [
            { "name": "Authorization", "value": "Bearer abc" },
            { "name": "Proxy-Authorization", "value": "Basic xyz" },
            { "name": "Accept", "value": "application/json" }
          ]
        }
        """;
    String all = allSections(json);

    assertFalse(all.contains("sk-live-123"), all);
    assertFalse(all.contains("Bearer abc"), all);
    assertFalse(all.contains("Basic xyz"), all);
    assertTrue(all.contains("X-API-Key"), all);
    assertTrue(all.contains("application/json"), all);
    assertTrue(all.contains("https://auth.example.com/authorize"), all);
  }

  @Test
  void aesEncodedValuesAreRedactedWhateverTheFieldIsCalled() {
    String json =
        """
        {
          "name": "openai",
          "auth_header_value": "AES2 q83vEjRWeJCrze8SNFZ4kA==",
          "headers": [ { "name": "X-Custom", "value": "AES q83vEjRWeJCrze8SNFZ4kA==" } ]
        }
        """;
    String all = allSections(json);

    assertFalse(all.contains("q83vEjRWeJCrze8SNFZ4kA=="), all);
  }

  private static String allSections(String json) {
    return new HopMetadataJsonParser()
        .parse(json).flattenSections().stream()
            .map(section -> String.join(" > ", section.getPath()) + " " + section.getBody())
            .collect(Collectors.joining("\n"));
  }
}
