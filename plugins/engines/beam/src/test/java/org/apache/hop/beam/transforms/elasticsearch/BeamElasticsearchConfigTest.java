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

import java.lang.reflect.InvocationTargetException;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

class BeamElasticsearchConfigTest {
  @org.junit.jupiter.api.BeforeAll
  static void init() throws Exception {
    org.apache.hop.beam.core.BeamHop.init();
  }

  @Test
  void connectionResolvesVariablesAndPreservesSafeTlsAndBasicAuth() throws Exception {
    BeamElasticsearchInputMeta meta = new BeamElasticsearchInputMeta();
    meta.setHosts("${HOST}, https://second.example:9200");
    meta.setIndex("${INDEX}");
    BeamElasticsearchIOTest.set(meta, "DocumentType", "${TYPE}");
    BeamElasticsearchIOTest.set(meta, "Username", "${USER}");
    BeamElasticsearchIOTest.set(meta, "Password", "${PASS}");
    Variables variables = new Variables();
    variables.setVariable("HOST", "https://first.example:9200");
    variables.setVariable("INDEX", "documents");
    variables.setVariable("TYPE", "");
    variables.setVariable("USER", "reader");
    variables.setVariable("PASS", "test-secret");
    ElasticsearchIO.ConnectionConfiguration connection = connection(meta, variables);
    assertEquals(
        java.util.List.of("https://first.example:9200", "https://second.example:9200"),
        connection.getAddresses());
    assertEquals("documents", connection.getIndex());
    assertEquals("", connection.getType());
    assertEquals("reader", connection.getUsername());
    assertEquals("test-secret", connection.getPassword());
    assertFalse(connection.isTrustSelfSignedCerts());
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("invalidConnections")
  void rejectsInvalidConnectionBeforeGraphSubmission(
      String hosts, String index, String username, String password) {
    BeamElasticsearchInputMeta meta = new BeamElasticsearchInputMeta();
    meta.setHosts(hosts);
    meta.setIndex(index);
    meta.setUsername(username);
    meta.setPassword(password);
    assertThrows(HopException.class, () -> meta.connectionConfiguration(new Variables()));
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> invalidConnections() {
    return java.util.stream.Stream.of(
        org.junit.jupiter.params.provider.Arguments.of(null, "documents", null, null),
        org.junit.jupiter.params.provider.Arguments.of("", "documents", null, null),
        org.junit.jupiter.params.provider.Arguments.of(
            "http://localhost:9200,", "documents", null, null),
        org.junit.jupiter.params.provider.Arguments.of("file:///etc", "documents", null, null),
        org.junit.jupiter.params.provider.Arguments.of(
            "http://user:secret@localhost:9200", "documents", null, null),
        org.junit.jupiter.params.provider.Arguments.of(
            "http://localhost:9200/path", "documents", null, null),
        org.junit.jupiter.params.provider.Arguments.of("http://localhost:9200", " ", null, null),
        org.junit.jupiter.params.provider.Arguments.of(
            "http://localhost:9200", "${MISSING}", null, null),
        org.junit.jupiter.params.provider.Arguments.of(
            "http://localhost:9200", "documents/_bulk", null, null),
        org.junit.jupiter.params.provider.Arguments.of(
            "http://localhost:9200", "documents", "reader", null),
        org.junit.jupiter.params.provider.Arguments.of(
            "http://localhost:9200", "documents", null, "secret"));
  }

  static ElasticsearchIO.ConnectionConfiguration connection(Object meta, Variables variables)
      throws Exception {
    try {
      return (ElasticsearchIO.ConnectionConfiguration)
          meta.getClass()
              .getMethod("connectionConfiguration", org.apache.hop.core.variables.IVariables.class)
              .invoke(meta, variables);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof HopException cause) {
        throw cause;
      }
      throw e;
    }
  }
}
