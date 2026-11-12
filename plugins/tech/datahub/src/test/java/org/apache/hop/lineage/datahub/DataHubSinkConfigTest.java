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

package org.apache.hop.lineage.datahub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.lineage.datahub.DataHubSinkConfig.SchemaPolicy;
import org.apache.hop.lineage.openlineage.OpenLineageSinkConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class DataHubSinkConfigTest {

  private static Variables variables(String url) {
    Variables variables = new Variables();
    variables.setVariable(DataHubSinkConfig.HOP_LINEAGE_DATAHUB_URL, url);
    return variables;
  }

  @Test
  void theUrlIsRequired() {
    assertThrows(
        IllegalArgumentException.class, () -> DataHubSinkConfig.fromVariables(new Variables()));
    assertThrows(
        IllegalArgumentException.class,
        () -> DataHubSinkConfig.fromVariables(variables("localhost:8080")));
  }

  // DataHub Cloud serves GMS under /gms: a path of its own is kept. The OpenLineage endpoint an
  // earlier version of the sink was pointed at is dropped.
  @Test
  void keepsAGmsPathAndDropsTheOldOpenLineageEndpoint() {
    assertEquals(
        "https://acme.acryl.io/gms",
        DataHubSinkConfig.fromVariables(variables("https://acme.acryl.io/gms/")).getGmsUrl());
    assertEquals(
        "http://localhost:8080",
        DataHubSinkConfig.fromVariables(
                variables("http://localhost:8080/openapi/openlineage/api/v1/lineage"))
            .getGmsUrl());
  }

  @Test
  void defaults() {
    DataHubSinkConfig config = DataHubSinkConfig.fromVariables(variables("http://localhost:8080"));
    assertEquals("PROD", config.getEnv());
    assertEquals("prod", config.getCluster());
    assertEquals(SchemaPolicy.MISSING, config.getSchemaPolicy());
    assertFalse(config.isLifecycleEnabled());
  }

  @Test
  void readsItsOwnSettings() {
    Variables variables = variables("http://localhost:8080");
    variables.setVariable(DataHubSinkConfig.HOP_LINEAGE_DATAHUB_ENV, "dev");
    variables.setVariable(DataHubSinkConfig.HOP_LINEAGE_DATAHUB_DATASET_SCHEMA, "never");
    variables.setVariable(DataHubSinkConfig.HOP_LINEAGE_DATAHUB_LIFECYCLE, "Y");
    variables.setVariable(DataHubSinkConfig.HOP_LINEAGE_DATAHUB_PLATFORM_INSTANCE, "emea");
    DataHubSinkConfig config = DataHubSinkConfig.fromVariables(variables);
    assertEquals("DEV", config.getEnv());
    assertEquals(SchemaPolicy.NEVER, config.getSchemaPolicy());
    assertTrue(config.isLifecycleEnabled());
    assertEquals("emea", config.getPlatformInstance());

    variables.setVariable(DataHubSinkConfig.HOP_LINEAGE_DATAHUB_DATASET_SCHEMA, "sometimes");
    assertThrows(IllegalArgumentException.class, () -> DataHubSinkConfig.fromVariables(variables));
  }

  // The delivery settings shared with the OpenLineage sink pass through; the mapping is fixed.
  @Test
  void passesTheSharedDeliverySettingsThrough() {
    Variables variables = variables("http://localhost:8080");
    variables.setVariable("HOP_LINEAGE_DATAHUB_SPOOL_DIR", "/tmp/spool");
    variables.setVariable("HOP_LINEAGE_DATAHUB_MAX_RETRIES", "7");
    variables.setVariable("HOP_LINEAGE_DATAHUB_TRANSFORM_JOBS", "N");
    variables.setVariable("HOP_LINEAGE_DATAHUB_FILE_SPEC_NAMING", "N");

    OpenLineageSinkConfig delivery =
        DataHubSinkConfig.fromVariables(variables).toOpenLineageConfig();
    assertEquals("/tmp/spool", delivery.getSpoolDirectory());
    assertEquals(7, delivery.getMaxRetries());
    assertTrue(delivery.isTransformJobs());
    assertTrue(delivery.isParentRunsEnabled());
    // DataHub names files by platform and path, whatever the OpenLineage sink's naming default.
    assertTrue(delivery.isFileSpecNaming());
  }

  // A token can be stored the way database passwords are: encoded with hop-encrypt.
  @Test
  void decryptsAnEncryptedToken() {
    Variables variables = variables("http://localhost:8080");
    variables.setVariable(
        DataHubSinkConfig.HOP_LINEAGE_DATAHUB_TOKEN,
        Encr.encryptPasswordIfNotUsingVariables("secret-token"));
    assertTrue(
        variables
            .getVariable(DataHubSinkConfig.HOP_LINEAGE_DATAHUB_TOKEN)
            .startsWith(Encr.PASSWORD_ENCRYPTED_PREFIX));

    assertEquals("secret-token", DataHubSinkConfig.fromVariables(variables).getToken());

    variables.setVariable(DataHubSinkConfig.HOP_LINEAGE_DATAHUB_TOKEN, "plain-token");
    assertEquals("plain-token", DataHubSinkConfig.fromVariables(variables).getToken());
  }
}
