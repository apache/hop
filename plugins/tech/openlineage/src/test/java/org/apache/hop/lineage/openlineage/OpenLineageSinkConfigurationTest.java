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

package org.apache.hop.lineage.openlineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.Test;

class OpenLineageSinkConfigurationTest {

  private static IVariables variablesOf(Map<String, String> settings) {
    Variables variables = new Variables();
    settings.forEach(variables::setVariable);
    return variables;
  }

  @Test
  void theSettingsAreTheOnesTheSinkReads() throws Exception {
    OpenLineageSinkConfiguration configuration = new OpenLineageSinkConfiguration();
    configuration.setUrl("${MARQUEZ}/api/v1/lineage");
    configuration.setApiKey("s3cr3t");
    configuration.setNamespace("hop-dev");
    configuration.setTransformJobs(true);
    configuration.setParentRuns(false);
    configuration.setSpoolDirectory("${SPOOL}");
    configuration.setMaxRetries("7");
    configuration.setOverflowPolicy(OpenLineageDeliveryConfiguration.Overflow.BLOCK);
    IVariables variables = new Variables();
    variables.setVariable("MARQUEZ", "http://marquez:5000");
    variables.setVariable("SPOOL", "/var/spool/hop");

    Map<String, String> settings =
        configuration.toSettings(variables, new MemoryMetadataProvider());
    OpenLineageSinkConfig config = OpenLineageSinkConfig.fromVariables(variablesOf(settings));

    assertEquals("http://marquez:5000/api/v1/lineage", config.getCollectorUrl());
    assertEquals("s3cr3t", config.getApiKey());
    assertEquals("hop-dev", config.getNamespace());
    assertTrue(config.isTransformJobs());
    assertFalse(config.isParentRunsEnabled());
    assertTrue(config.isFileSpecNaming(), "a new sink names files by the spec");
    assertTrue(config.isSqlParseEnabled());
    assertEquals("/var/spool/hop", config.getSpoolDirectory());
    assertEquals(7, config.getMaxRetries());
    assertEquals(OpenLineageAsyncDispatcher.OverflowPolicy.BLOCK, config.getOverflowPolicy());
  }

  @Test
  void anOptionLeftEmptyKeepsTheSinksDefault() throws Exception {
    OpenLineageSinkConfiguration configuration = new OpenLineageSinkConfiguration();
    configuration.setUrl("http://marquez:5000/api/v1/lineage");
    configuration.setReadTimeoutMs("${NOT_SET}");

    Map<String, String> settings =
        configuration.toSettings(new Variables(), new MemoryMetadataProvider());
    OpenLineageSinkConfig config = OpenLineageSinkConfig.fromVariables(variablesOf(settings));

    assertFalse(settings.containsKey(OpenLineageSinkConfig.VAR_NAMESPACE));
    assertEquals(OpenLineageSinkConfig.DEFAULT_NAMESPACE, config.getNamespace());
    assertNull(config.getSpoolDirectory());
    assertEquals(30_000, config.getReadTimeoutMs(), "an unresolved variable is not a timeout");
  }

  @Test
  void aSinkWithoutAUrlSaysSo() {
    OpenLineageSinkConfiguration configuration = new OpenLineageSinkConfiguration();

    HopException e =
        assertThrows(
            HopException.class,
            () -> configuration.toSettings(new Variables(), new MemoryMetadataProvider()));
    assertTrue(e.getMessage().contains("URL"), e.getMessage());
  }

  @Test
  void theSinkHandsOutANewConfigurationEachTime() {
    OpenLineageLineageSink sink = new OpenLineageLineageSink();

    OpenLineageSinkConfiguration first =
        assertInstanceOf(OpenLineageSinkConfiguration.class, sink.newConfiguration());

    assertNotSame(first, sink.newConfiguration());
    OpenLineageSinkConfiguration copy = first.clone();
    copy.setUrl("http://other");
    assertNull(first.getUrl());
  }
}
