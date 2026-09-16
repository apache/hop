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
}
