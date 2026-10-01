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
import org.apache.hop.pipeline.transforms.chunker.document.DocumentNode;
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
                .collect(java.util.stream.Collectors.joining("\n"));

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
                .collect(java.util.stream.Collectors.joining("\n"));

    assertFalse(all.contains("tok-abcdef123456"), all);
    assertFalse(all.contains("hunter2"), all);
    assertTrue(all.contains("https://api.example.com"), all);
  }
}
