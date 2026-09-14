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
package org.apache.hop.naming.gui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.function.UnaryOperator;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.imp.HopImportBase;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.naming.engine.NamingEngine;
import org.apache.hop.naming.metadata.NamingCaseStyle;
import org.apache.hop.naming.metadata.NamingScheme;
import org.apache.hop.naming.metadata.NamingSchemeType;
import org.apache.hop.naming.metadata.NamingWordSeparator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class NamingSchemeImportExtensionTest {

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void uniqueHopMetadataSchemeBecomesTheMapper() throws Exception {
    NamingScheme scheme = lowerUnderscore("conn-names");
    scheme.setType(NamingSchemeType.HOP_METADATA.getCode());

    MemoryMetadataProvider memory = new MemoryMetadataProvider();
    memory.getSerializer(NamingScheme.class).save(scheme);

    HopImportBase hopImport = new TestImport();
    hopImport.setMetadataProvider(
        new MultiMetadataProvider(null, java.util.List.of(memory), hopImport.getVariables()));

    new NamingSchemeImportExtension()
        .callExtensionPoint(LogChannel.GENERAL, hopImport.getVariables(), hopImport);

    assertEquals("conn-names", hopImport.getAppliedNamingSchemeName());
    UnaryOperator<String> mapper = hopImport.getConnectionNameMapper();
    assertEquals(NamingEngine.apply(scheme, "My DB", "hop-metadata"), mapper.apply("My DB"));
  }

  @Test
  void explicitNameWinsOverType() throws Exception {
    NamingScheme general = lowerUnderscore("general-one");
    general.setType(NamingSchemeType.GENERAL.getCode());
    NamingScheme metadata = lowerUnderscore("meta-one");
    metadata.setType(NamingSchemeType.HOP_METADATA.getCode());
    metadata.setCaseStyle(NamingCaseStyle.UPPER.getCode());

    MemoryMetadataProvider memory = new MemoryMetadataProvider();
    memory.getSerializer(NamingScheme.class).save(general);
    memory.getSerializer(NamingScheme.class).save(metadata);

    HopImportBase hopImport = new TestImport();
    hopImport.setMetadataProvider(
        new MultiMetadataProvider(null, java.util.List.of(memory), hopImport.getVariables()));
    hopImport.setNamingSchemeName("general-one");

    new NamingSchemeImportExtension()
        .callExtensionPoint(LogChannel.GENERAL, hopImport.getVariables(), hopImport);

    assertEquals("general-one", hopImport.getAppliedNamingSchemeName());
    assertEquals("my_db", hopImport.getConnectionNameMapper().apply("My DB"));
  }

  @Test
  void skippedWhenApplyDisabled() throws Exception {
    NamingScheme scheme = lowerUnderscore("conn-names");
    scheme.setType(NamingSchemeType.HOP_METADATA.getCode());
    MemoryMetadataProvider memory = new MemoryMetadataProvider();
    memory.getSerializer(NamingScheme.class).save(scheme);

    HopImportBase hopImport = new TestImport();
    hopImport.setMetadataProvider(
        new MultiMetadataProvider(null, java.util.List.of(memory), hopImport.getVariables()));
    hopImport.setApplyNamingSchemes(false);

    new NamingSchemeImportExtension()
        .callExtensionPoint(LogChannel.GENERAL, hopImport.getVariables(), hopImport);

    assertEquals(null, hopImport.getAppliedNamingSchemeName());
    assertEquals("My DB", hopImport.getConnectionNameMapper().apply("My DB"));
  }

  @Test
  void noSchemeLeavesIdentityMapper() throws Exception {
    HopImportBase hopImport = new TestImport();
    hopImport.setMetadataProvider(
        new MultiMetadataProvider(
            null, java.util.List.of(new MemoryMetadataProvider()), hopImport.getVariables()));

    new NamingSchemeImportExtension()
        .callExtensionPoint(LogChannel.GENERAL, hopImport.getVariables(), hopImport);

    assertEquals("My DB", hopImport.getConnectionNameMapper().apply("My DB"));
    assertNotEquals("x", hopImport.getConnectionNameMapper().apply("My DB"));
  }

  private static NamingScheme lowerUnderscore(String name) {
    NamingScheme scheme = new NamingScheme(name);
    scheme.setCaseStyle(NamingCaseStyle.LOWER.getCode());
    scheme.setWordSeparator(NamingWordSeparator.UNDERSCORE.getCode());
    scheme.setRemoveSpecialCharacters(true);
    scheme.setCollapseRepeatedSeparators(true);
    scheme.setTrimEdgeSeparators(true);
    return scheme;
  }

  private static final class TestImport extends HopImportBase {
    @Override
    public void importFiles() {}

    @Override
    public void findFilesToImport() {}

    @Override
    public void importConnections() {}

    @Override
    public void importVariables() {}

    @Override
    public String getImportReport() {
      return "";
    }
  }
}
