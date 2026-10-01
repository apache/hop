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

import static org.junit.jupiter.api.Assertions.*;

import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class BeamElasticsearchSerializationTest {
  @BeforeAll
  static void init() throws Exception {
    BeamHop.init();
  }

  @Test
  void sourceFixtureRoundTripsAllOptions() throws Exception {
    BeamElasticsearchInputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/beam-elasticsearch-input-transform.xml", BeamElasticsearchInputMeta.class);
    assertEquals("${ES_HOSTS}", meta.getHosts());
    assertEquals("documents", meta.getIndex());
    assertEquals("legacy", meta.getDocumentType());
    assertEquals("reader", meta.getUsername());
    assertEquals("${ES_PASSWORD}", meta.getPassword());
    assertEquals("payload", meta.getJsonField());
    assertEquals("{\"query\":{\"match_all\":{}}}", meta.getQuery());
    assertEquals("2m", meta.getScrollKeepalive());
  }

  @Test
  void sinkFixtureRoundTripsAllOptions() throws Exception {
    BeamElasticsearchOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/beam-elasticsearch-output-transform.xml", BeamElasticsearchOutputMeta.class);
    assertEquals("${ES_HOSTS}", meta.getHosts());
    assertEquals("documents", meta.getIndex());
    assertEquals("legacy", meta.getDocumentType());
    assertEquals("writer", meta.getUsername());
    assertEquals("${ES_PASSWORD}", meta.getPassword());
    assertEquals("payload", meta.getJsonField());
    assertEquals("17", meta.getMaxBatchSize());
    assertEquals("4096", meta.getMaxBatchBytes());
  }

  @Test
  void serializesLiteralPasswordsEncryptedAndVariablesUnchanged() throws Exception {
    BeamElasticsearchInputMeta input = new BeamElasticsearchInputMeta();
    input.setPassword("test-password");
    String xml = input.getXml();
    assertFalse(xml.contains("test-password"));
    assertTrue(xml.contains(Encr.encryptPasswordIfNotUsingVariables("test-password")));
    BeamElasticsearchOutputMeta output = new BeamElasticsearchOutputMeta();
    output.setPassword("test-password");
    assertFalse(output.getXml().contains("test-password"));
    output.setPassword("${ES_PASSWORD}");
    assertTrue(output.getXml().contains("${ES_PASSWORD}"));
  }
}
