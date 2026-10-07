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

package org.apache.hop.ai.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.advisor.AiProposalValidation;
import org.apache.hop.ai.engine.AiAdvisorMetadataContextTest.TestMetadataProvider;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AiMetadataProposalSupportTest {

  @BeforeEach
  void allowTheTestType() {
    Set<String> keys = new HashSet<>(AiMetadataProposalSupport.SAVABLE_TYPE_KEYS);
    keys.add("test-connection");
    AiMetadataProposalSupport.savableTypeKeys = keys;
  }

  @AfterEach
  void restoreTheAllowedTypes() {
    AiMetadataProposalSupport.savableTypeKeys = AiMetadataProposalSupport.SAVABLE_TYPE_KEYS;
  }

  @Test
  void typesOffTheListCannotBeSaved() {
    AiMetadataProposalSupport.savableTypeKeys = AiMetadataProposalSupport.SAVABLE_TYPE_KEYS;
    AiProposalValidation validation =
        AiMetadataProposalSupport.validate(
            proposal(
                "SAVE_METADATA",
                Map.of("typeKey", "test-connection", "json", "{\"name\":\"crm\"}")),
            new TestMetadataProvider());
    assertTrue(validation.isBlocked());
    assertTrue(validation.getReason().contains("test-connection"), validation.getReason());
  }

  @Test
  void aFailingSaveUndoesTheSavesBeforeIt() throws Exception {
    TestMetadataProvider provider = new TestMetadataProvider();
    AiProposal first =
        proposal(
            "SAVE_METADATA", Map.of("typeKey", "test-connection", "json", "{\"name\":\"first\"}"));
    // Valid JSON, but no name: the save itself fails.
    AiProposal nameless =
        proposal(
            "SAVE_METADATA", Map.of("typeKey", "test-connection", "json", "{\"hostname\":\"db\"}"));

    assertThrows(
        HopException.class,
        () -> AiMetadataProposalSupport.saveAll(List.of(first, nameless), provider));
    assertFalse(provider.getSerializer(TestConnection.class).exists("first"));
  }

  @Test
  void savesJsonAndHonorsNameOverride() throws Exception {
    TestMetadataProvider provider = new TestMetadataProvider();
    AiProposal proposal =
        proposal(
            "SAVE_METADATA",
            Map.of(
                "typeKey",
                "test-connection",
                "name",
                "from-param",
                "json",
                "{\"name\":\"from-json\"}"));
    AiProposalValidation validation = AiMetadataProposalSupport.validate(proposal, provider);
    assertFalse(validation.isBlocked(), validation.getReason());
    assertEquals(1, AiMetadataProposalSupport.saveAll(List.of(proposal), provider).size());
    TestConnection loaded = provider.getSerializer(TestConnection.class).load("from-param");
    assertNotNull(loaded);
    assertEquals("from-param", loaded.getName());
  }

  @Test
  void unwrapsContentEnvelope() throws Exception {
    TestMetadataProvider provider = new TestMetadataProvider();
    AiProposal proposal =
        proposal(
            "SAVE_METADATA",
            Map.of("typeKey", "test-connection", "json", "{\"content\":{\"name\":\"wrapped\"}}"));
    assertFalse(AiMetadataProposalSupport.validate(proposal, provider).isBlocked());
    AiMetadataProposalSupport.save(proposal, provider);
    assertNotNull(provider.getSerializer(TestConnection.class).load("wrapped"));
  }

  @Test
  void typesThatRedirectDataCannotBeSaved() {
    TestMetadataProvider provider = new TestMetadataProvider();
    AiProposal proposal =
        proposal(
            "SAVE_METADATA",
            Map.of(
                "typeKey",
                "ai-provider",
                "json",
                "{\"name\":\"default\",\"baseUrl\":\"https://elsewhere.example\"}"));
    AiProposalValidation validation = AiMetadataProposalSupport.validate(proposal, provider);
    assertTrue(validation.isBlocked());
    assertTrue(validation.getReason().contains("Metadata perspective"), validation.getReason());
    assertThrows(
        HopException.class, () -> AiMetadataProposalSupport.checkAll(List.of(proposal), provider));
  }

  @Test
  void overwritingIsOptInAndCanBeUndone() throws Exception {
    TestMetadataProvider provider = new TestMetadataProvider();
    TestConnection existing = new TestConnection();
    existing.setName("crm");
    existing.setHostname("db-prod");
    provider.getSerializer(TestConnection.class).save(existing);

    AiProposal overwrite =
        proposal(
            "SAVE_METADATA",
            Map.of(
                "typeKey", "test-connection", "json", "{\"name\":\"crm\",\"hostname\":\"db-ai\"}"));
    AiProposal create =
        proposal(
            "SAVE_METADATA",
            Map.of("typeKey", "test-connection", "json", "{\"name\":\"new-one\"}"));

    AiProposalValidation validation = AiMetadataProposalSupport.validate(overwrite, provider);
    assertFalse(validation.isBlocked());
    assertTrue(validation.isOptIn());
    assertTrue(validation.getWarning().contains("Overwrites"), validation.getWarning());
    assertFalse(AiMetadataProposalSupport.validate(create, provider).isOptIn());

    List<AiMetadataBackup> backups =
        AiMetadataProposalSupport.saveAll(List.of(overwrite, create), provider);
    assertEquals("db-ai", provider.getSerializer(TestConnection.class).load("crm").getHostname());
    assertTrue(provider.getSerializer(TestConnection.class).exists("new-one"));

    AiMetadataProposalSupport.revert(backups, provider);
    assertEquals("db-prod", provider.getSerializer(TestConnection.class).load("crm").getHostname());
    assertFalse(provider.getSerializer(TestConnection.class).exists("new-one"));
  }

  @Test
  void missingTypeKeyIsBlocked() {
    TestMetadataProvider provider = new TestMetadataProvider();
    AiProposal proposal = proposal("SAVE_METADATA", Map.of("json", "{\"name\":\"x\"}"));
    AiProposalValidation validation = AiMetadataProposalSupport.validate(proposal, provider);
    assertTrue(validation.isBlocked());
    assertTrue(validation.getReason().contains("typeKey"));
  }

  @Test
  void wrapsFlatRdbmsJsonWithPluginId() {
    AiProposal proposal =
        proposal(
            "SAVE_METADATA",
            Map.of(
                "typeKey",
                "rdbms",
                "json",
                "{\"name\":\"test_edw\",\"hostname\":\"localhost\",\"port\":\"54320\","
                    + "\"databaseName\":\"test_edw\",\"username\":\"test\","
                    + "\"pluginId\":\"PostgreSQL\",\"password\":\"${DB_PASSWORD}\"}"));
    String json = AiMetadataProposalSupport.jsonParam(proposal);
    assertTrue(json.contains("\"POSTGRESQL\""), json);
    assertTrue(json.contains("\"hostname\":\"localhost\""), json);
    assertTrue(json.contains("\"port\":\"54320\""), json);
    assertFalse(json.contains("\"rdbms\":{\"hostname\""), json);
  }

  @Test
  void wrapsFlatObjectInsideRdbmsField() {
    AiProposal proposal =
        proposal(
            "SAVE_METADATA",
            Map.of(
                "typeKey",
                "rdbms",
                "name",
                "test_edw",
                "json",
                "{\"name\":\"test_edw\",\"rdbms\":{\"pluginId\":\"POSTGRESQL\","
                    + "\"hostname\":\"localhost\",\"databaseName\":\"test_edw\"}}"));
    String json = AiMetadataProposalSupport.jsonParam(proposal);
    assertTrue(json.contains("\"rdbms\":{\"POSTGRESQL\""), json);
    assertTrue(json.contains("\"pluginId\":\"POSTGRESQL\""), json);
    assertTrue(json.contains("\"pluginName\":\"PostgreSQL\""), json);
  }

  @Test
  void synthesizesRdbmsJsonFromDescriptionWhenJsonMissing() {
    AiProposal proposal = proposal("SAVE_METADATA", Map.of("typeKey", "rdbms", "name", "test_edw"));
    proposal.setDescription("Save PostgreSQL RDBMS connection test_edw (localhost:54320)");
    String json = AiMetadataProposalSupport.jsonParam(proposal);
    assertTrue(json.contains("POSTGRESQL"), json);
    assertTrue(json.contains("\"pluginName\":\"PostgreSQL\""), json);
    assertTrue(json.contains("\"hostname\":\"localhost\""), json);
    assertTrue(json.contains("\"port\":\"54320\""), json);
    assertTrue(json.contains("\"databaseName\":\"test_edw\""), json);
  }

  @Test
  void synthesizesRdbmsJsonFromFlatFields() {
    AiProposal proposal =
        proposal(
            "SAVE_METADATA",
            Map.of(
                "typeKey",
                "rdbms",
                "name",
                "test_edw",
                "pluginId",
                "POSTGRESQL",
                "hostname",
                "localhost",
                "port",
                "54320",
                "databaseName",
                "test_edw",
                "username",
                "test",
                "password",
                "${DB_PASSWORD}"));
    String json = AiMetadataProposalSupport.jsonParam(proposal);
    assertTrue(json.contains("\"hostname\":\"localhost\""));
    assertTrue(json.contains("\"port\":\"54320\""));
    assertTrue(json.contains("POSTGRESQL"));
    assertTrue(json.contains("\"pluginName\":\"PostgreSQL\""), json);
    assertTrue(json.contains("${DB_PASSWORD}"));
  }

  @Test
  void clipboardMetadataIsNotSaved() throws Exception {
    TestMetadataProvider provider = new TestMetadataProvider();
    AiProposal proposal =
        proposal(
            "CLIPBOARD_METADATA",
            Map.of("typeKey", "ai-provider", "name", "clip", "json", "{\"name\":\"clip\"}"));
    assertFalse(AiMetadataProposalSupport.validate(proposal, provider).isBlocked());
    assertEquals(0, AiMetadataProposalSupport.saveAll(List.of(proposal), provider).size());
  }

  private static AiProposal proposal(String type, Map<String, String> parameters) {
    AiProposal proposal = new AiProposal();
    proposal.setType(type);
    proposal.setParameters(parameters);
    return proposal;
  }
}
