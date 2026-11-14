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

package org.apache.hop.pipeline.transforms.maskfields;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.datacatalog.CatalogAnswer;
import org.apache.hop.datacatalog.CatalogDatasetType;
import org.apache.hop.datacatalog.CatalogEntityId;
import org.apache.hop.datacatalog.DataCatalog;
import org.apache.hop.datacatalog.DatasetRef;
import org.apache.hop.datacatalog.IDataCatalog;
import org.apache.hop.datacatalog.ISchemaCapability;
import org.apache.hop.datacatalog.ISchemaCapability.CatalogField;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class MaskFieldsTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<MaskFieldsMeta, MaskFieldsData> mockHelper;

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>("Mask fields", MaskFieldsMeta.class, MaskFieldsData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
    when(mockHelper.transformMeta.getCopies(any())).thenReturn(1);
    when(mockHelper.transformMeta.isPartitioned()).thenReturn(false);
    when(mockHelper.transformMeta.isDoingErrorHandling()).thenReturn(false);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void processRowRemembersANameForThisRun() throws Exception {
    MaskingPattern pattern = new MaskingPattern();
    pattern.setName("First name");
    pattern.setValueSource(MaskingValueSource.SYNTHETIC);
    pattern.setToken(MaskingToken.SEQUENCE);
    pattern.setStorage(MaskingStorage.MEMORY);
    pattern.setPrefix("first-name-");
    pattern.setSequenceStart("1");
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(MaskingPattern.class).save(pattern);

    MaskFieldsMeta meta = new MaskFieldsMeta();
    meta.getFields().add(new MaskField("name", "First name"));
    MaskFields transform = createTransform(meta);
    transform.setMetadataProvider(provider);
    assertTrue(transform.init());

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    BlockingRowSet input =
        input(
            "people", rowMeta, new Object[] {"Matt"}, new Object[] {"Ann"}, new Object[] {"Matt"});
    BlockingRowSet output = output(rowMeta);
    transform.addRowSetToInputRowSets(input);
    transform.addRowSetToOutputRowSets(output);

    assertTrue(transform.processRow());
    assertTrue(transform.processRow());
    assertTrue(transform.processRow());
    assertFalse(transform.processRow());

    assertArrayEquals(new Object[] {"first-name-1"}, output.getRowWait(1, TimeUnit.SECONDS));
    assertArrayEquals(new Object[] {"first-name-2"}, output.getRowWait(1, TimeUnit.SECONDS));
    assertArrayEquals(new Object[] {"first-name-1"}, output.getRowWait(1, TimeUnit.SECONDS));
    transform.dispose();
  }

  @Test
  void processRowLeavesAFieldWithoutARuleUnchanged() throws Exception {
    MaskingPattern pattern = new MaskingPattern();
    pattern.setName("First name");
    pattern.setValueSource(MaskingValueSource.SYNTHETIC);
    pattern.setToken(MaskingToken.SEQUENCE);
    pattern.setStorage(MaskingStorage.NONE);
    pattern.setPrefix("first-name-");
    pattern.setSequenceStart("1");
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(MaskingPattern.class).save(pattern);

    MaskFieldsMeta meta = new MaskFieldsMeta();
    meta.getFields().add(new MaskField("name", "First name"));
    meta.getFields().add(new MaskField("city", ""));
    MaskFields transform = createTransform(meta);
    transform.setMetadataProvider(provider);
    assertTrue(transform.init());

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaString("city"));
    BlockingRowSet input = input("people", rowMeta, new Object[] {"Matt", "Gent"});
    BlockingRowSet output = output(rowMeta);
    transform.addRowSetToInputRowSets(input);
    transform.addRowSetToOutputRowSets(output);

    assertTrue(transform.processRow());
    assertArrayEquals(
        new Object[] {"first-name-1", "Gent"}, output.getRowWait(1, TimeUnit.SECONDS));
    transform.dispose();
  }

  @Test
  void processRowRejectsIncompatibleFieldTypeAtRuntime() throws Exception {
    MaskingPattern pattern = new MaskingPattern();
    pattern.setName("Uuid");
    pattern.setValueSource(MaskingValueSource.SYNTHETIC);
    pattern.setToken(MaskingToken.UUID);
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(MaskingPattern.class).save(pattern);

    MaskFieldsMeta meta = new MaskFieldsMeta();
    meta.getFields().add(new MaskField("birth_date", "Uuid"));
    MaskFields transform = createTransform(meta);
    transform.setMetadataProvider(provider);
    assertTrue(transform.init());

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaDate("birth_date"));
    BlockingRowSet input = input("people", rowMeta, new Object[] {new Date()});
    BlockingRowSet output = output(rowMeta);
    transform.addRowSetToInputRowSets(input);
    transform.addRowSetToOutputRowSets(output);

    HopException e = assertThrows(HopException.class, transform::processRow);
    assertTrue(e.getMessage().contains("birth_date"));
    transform.dispose();
  }

  private MaskFields createTransform(MaskFieldsMeta meta) {
    when(mockHelper.transformMeta.getTransform()).thenReturn(meta);
    return new MaskFields(
        mockHelper.transformMeta,
        meta,
        new MaskFieldsData(),
        0,
        mockHelper.pipelineMeta,
        mockHelper.pipeline);
  }

  private static BlockingRowSet input(String origin, IRowMeta rowMeta, Object[]... rows) {
    BlockingRowSet rowSet = new BlockingRowSet(10);
    rowSet.setThreadNameFromToCopy(origin, 0, "Mask fields", 0);
    rowSet.setRowMeta(rowMeta);
    for (Object[] row : rows) {
      rowSet.putRow(rowMeta, row);
    }
    rowSet.setDone();
    return rowSet;
  }

  private static BlockingRowSet output(IRowMeta rowMeta) {
    BlockingRowSet rowSet = new BlockingRowSet(10);
    rowSet.setThreadNameFromToCopy("Mask fields", 0, "next", 0);
    rowSet.setRowMeta(rowMeta);
    return rowSet;
  }

  /** A data catalog that answers with the schema each test sets. */
  @Getter
  @Setter
  public static class SchemaCatalog implements IDataCatalog, ISchemaCapability {
    static CatalogAnswer<CatalogEntityId> resolved;
    static CatalogAnswer<List<CatalogField>> schema;

    private String pluginId = "schema";
    private String pluginName = "Schema catalog";

    @Override
    public SchemaCatalog clone() {
      return new SchemaCatalog();
    }

    @Override
    public void initialize(IVariables variables, IHopMetadataProvider metadataProvider) {
      // Nothing to prepare
    }

    @Override
    public CatalogAnswer<String> ping() {
      return CatalogAnswer.known("schema");
    }

    @Override
    public CatalogAnswer<CatalogEntityId> resolve(DatasetRef dataset) {
      return resolved;
    }

    @Override
    public CatalogAnswer<List<CatalogField>> schema(CatalogEntityId id) {
      return schema;
    }

    @Override
    public void close() {
      // Nothing to release
    }
  }

  private static CatalogField field(String name, Set<String> tags, Set<String> terms) {
    return new CatalogField(name, null, "STRING", null, tags, terms);
  }

  /** Patterns, a catalog whose customers table tags email PII and terms name Personal. */
  private MemoryMetadataProvider catalogProvider() throws Exception {
    SchemaCatalog.resolved = CatalogAnswer.known(new CatalogEntityId("urn:customers"));
    SchemaCatalog.schema =
        CatalogAnswer.known(
            List.of(
                field("email", Set.of("PII", "urn:li:tag:PII"), Set.of()),
                field("name", Set.of(), Set.of("Personal")),
                field("city", Set.of(), Set.of())));
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    MaskingPattern email = new MaskingPattern();
    email.setName("Masked email");
    email.setValueSource(MaskingValueSource.SYNTHETIC);
    email.setToken(MaskingToken.SEQUENCE);
    email.setStorage(MaskingStorage.NONE);
    email.setPrefix("email-");
    email.setSequenceStart("1");
    provider.getSerializer(MaskingPattern.class).save(email);
    MaskingPattern hidden = new MaskingPattern();
    hidden.setName("Hidden");
    hidden.setValueSource(MaskingValueSource.SET_NULL);
    provider.getSerializer(MaskingPattern.class).save(hidden);
    provider
        .getSerializer(DataCatalog.class)
        .save(new DataCatalog("catalog", null, new SchemaCatalog()));
    return provider;
  }

  private static MaskFieldsMeta catalogMeta() {
    MaskFieldsMeta meta = new MaskFieldsMeta();
    meta.setCatalogName("catalog");
    meta.setDatasetType(CatalogDatasetType.CATALOG_ID);
    meta.setDatasetName("urn:customers");
    meta.getTagRules().add(new MaskTagRule("pii", "Masked email"));
    meta.getTagRules().add(new MaskTagRule("Personal", "Hidden"));
    return meta;
  }

  private Object[] maskOneRow(MaskFieldsMeta meta, MemoryMetadataProvider provider)
      throws Exception {
    MaskFields transform = createTransform(meta);
    transform.setMetadataProvider(provider);
    assertTrue(transform.init());
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("EMAIL"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaString("city"));
    BlockingRowSet output = output(rowMeta);
    transform.addRowSetToInputRowSets(
        input("people", rowMeta, new Object[] {"ann@example.com", "Ann", "Gent"}));
    transform.addRowSetToOutputRowSets(output);
    assertTrue(transform.processRow());
    Object[] row = output.getRowWait(1, TimeUnit.SECONDS);
    transform.dispose();
    return row;
  }

  @Test
  void aFieldTheCatalogLabelsIsMaskedWithThePatternOfItsTagOrTerm() throws Exception {
    Object[] row = maskOneRow(catalogMeta(), catalogProvider());

    // EMAIL matches email whatever the case; city carries no tag and is left alone
    assertArrayEquals(new Object[] {"email-1", null, "Gent"}, row);
  }

  @Test
  void aFieldWithARowOfItsOwnKeepsThatPattern() throws Exception {
    MaskFieldsMeta meta = catalogMeta();
    meta.getFields().add(new MaskField("name", "Masked email"));

    Object[] row = maskOneRow(meta, catalogProvider());

    // name keeps its own pattern instead of the Hidden of its term; each field counts on its own
    assertArrayEquals(new Object[] {"email-1", "email-1", "Gent"}, row);
  }

  @Test
  void aCatalogThatCannotAnswerStopsTheTransform() throws Exception {
    MemoryMetadataProvider provider = catalogProvider();
    SchemaCatalog.schema = CatalogAnswer.unknown(CatalogAnswer.Reason.UNREACHABLE, "refused");
    MaskFields transform = createTransform(catalogMeta());
    transform.setMetadataProvider(provider);

    assertFalse(transform.init());
  }

  @Test
  void aDatasetTheCatalogDoesNotHaveStopsTheTransform() throws Exception {
    MemoryMetadataProvider provider = catalogProvider();
    SchemaCatalog.resolved = CatalogAnswer.notFound();
    MaskFields transform = createTransform(catalogMeta());
    transform.setMetadataProvider(provider);

    assertFalse(transform.init());
  }

  @Test
  void aTagWhoseVariableIsNotSetStopsTheTransform() throws Exception {
    MaskFieldsMeta meta = catalogMeta();
    meta.getTagRules().add(new MaskTagRule("${PII_TAG}", "Hidden"));
    MaskFields transform = createTransform(meta);
    transform.setMetadataProvider(catalogProvider());

    assertFalse(transform.init());
  }

  @Test
  void tagRulesWithoutACatalogStopTheTransform() throws Exception {
    MaskFieldsMeta meta = catalogMeta();
    meta.setCatalogName("");
    MaskFields transform = createTransform(meta);
    transform.setMetadataProvider(catalogProvider());

    assertFalse(transform.init());
  }
}
