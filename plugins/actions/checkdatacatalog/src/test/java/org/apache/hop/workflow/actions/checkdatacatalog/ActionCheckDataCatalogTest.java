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

package org.apache.hop.workflow.actions.checkdatacatalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.datacatalog.CatalogAnswer;
import org.apache.hop.datacatalog.CatalogAnswer.Reason;
import org.apache.hop.datacatalog.CatalogDataset;
import org.apache.hop.datacatalog.CatalogDatasetType;
import org.apache.hop.datacatalog.CatalogEntityId;
import org.apache.hop.datacatalog.DataCatalog;
import org.apache.hop.datacatalog.DatasetRef;
import org.apache.hop.datacatalog.IAssertionCapability;
import org.apache.hop.datacatalog.IDataCatalog;
import org.apache.hop.datacatalog.IDeprecationCapability;
import org.apache.hop.datacatalog.IFreshnessCapability;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActionCheckDataCatalogTest {

  private static final CatalogEntityId ORDERS = new CatalogEntityId("urn:orders");

  /** A catalog whose answers each test sets; copies share them, as a real catalog would. */
  @Getter
  @Setter
  public static class FakeCatalog
      implements IDataCatalog, IDeprecationCapability, IFreshnessCapability, IAssertionCapability {
    static CatalogAnswer<CatalogEntityId> resolved;
    static CatalogAnswer<Deprecation> deprecation;
    static CatalogAnswer<Instant> lastUpdated;
    static CatalogAnswer<List<AssertionResult>> assertions;
    static final List<DatasetRef> asked = new ArrayList<>();

    private String pluginId = "fake";
    private String pluginName = "Fake catalog";

    @Override
    public FakeCatalog clone() {
      return new FakeCatalog();
    }

    @Override
    public void initialize(IVariables variables, IHopMetadataProvider metadataProvider) {
      // Nothing to prepare
    }

    @Override
    public CatalogAnswer<String> ping() {
      return CatalogAnswer.known("fake");
    }

    @Override
    public CatalogAnswer<CatalogEntityId> resolve(DatasetRef dataset) {
      asked.add(dataset);
      return resolved;
    }

    @Override
    public CatalogAnswer<Deprecation> deprecation(CatalogEntityId id) {
      return deprecation;
    }

    @Override
    public CatalogAnswer<Instant> lastUpdated(CatalogEntityId id) {
      return lastUpdated;
    }

    @Override
    public CatalogAnswer<List<AssertionResult>> latestAssertionResults(CatalogEntityId id) {
      return assertions;
    }

    @Override
    public void close() {
      // Nothing to release
    }
  }

  /** A catalog that only knows which datasets it has. */
  @Getter
  @Setter
  public static class MinimalCatalog implements IDataCatalog {
    private String pluginId = "minimal";
    private String pluginName = "Minimal catalog";

    @Override
    public MinimalCatalog clone() {
      return new MinimalCatalog();
    }

    @Override
    public void initialize(IVariables variables, IHopMetadataProvider metadataProvider) {
      // Nothing to prepare
    }

    @Override
    public CatalogAnswer<String> ping() {
      return CatalogAnswer.known("minimal");
    }

    @Override
    public CatalogAnswer<CatalogEntityId> resolve(DatasetRef dataset) {
      return CatalogAnswer.known(ORDERS);
    }

    @Override
    public void close() {
      // Nothing to release
    }
  }

  private MemoryMetadataProvider provider;

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    FakeCatalog.resolved = CatalogAnswer.known(ORDERS);
    FakeCatalog.deprecation = CatalogAnswer.notFound();
    FakeCatalog.lastUpdated = CatalogAnswer.known(Instant.now().minus(Duration.ofMinutes(5)));
    FakeCatalog.assertions = CatalogAnswer.notFound();
    FakeCatalog.asked.clear();

    provider = new MemoryMetadataProvider();
    provider
        .getSerializer(DataCatalog.class)
        .save(new DataCatalog("prod", null, new FakeCatalog()));
    provider
        .getSerializer(DataCatalog.class)
        .save(new DataCatalog("minimal", null, new MinimalCatalog()));
    DatabaseMeta warehouse = new DatabaseMeta();
    warehouse.setName("warehouse");
    warehouse.setHostname("db.example.com");
    warehouse.setPort("5432");
    warehouse.setDBName("sales");
    provider.getSerializer(DatabaseMeta.class).save(warehouse);
  }

  private ActionCheckDataCatalog action(String catalog, CatalogDataset... datasets) {
    ActionCheckDataCatalog action = new ActionCheckDataCatalog("check");
    action.setCatalogName(catalog);
    action.setMaxAge("1");
    action.setMaxAgeUnit(CatalogAgeUnit.HOURS);
    action.getDatasets().addAll(List.of(datasets));
    action.setMetadataProvider(provider);
    action.setParentWorkflowMeta(new WorkflowMeta());
    return action;
  }

  private ActionCheckDataCatalog action() {
    return action("prod", new CatalogDataset(CatalogDatasetType.CATALOG_ID, null, null, "urn:x"));
  }

  @Test
  void aDatasetThatPassesEveryCheckGivesTrue() {
    Result result = action().execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(0, result.getNrErrors());
  }

  @Test
  void aDatasetTheCatalogDoesNotHaveFailsTheCheck() {
    FakeCatalog.resolved = CatalogAnswer.notFound();

    Result result = action().execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(0, result.getNrErrors());
  }

  @Test
  void aDeprecatedDatasetFailsOnlyWhenThatIsChecked() {
    FakeCatalog.deprecation =
        CatalogAnswer.known(new IDeprecationCapability.Deprecation(true, "Use v2", null));
    assertFalse(action().execute(new Result(), 0).getResult());

    ActionCheckDataCatalog action = action();
    action.setCheckDeprecation(false);
    assertTrue(action.execute(new Result(), 0).getResult());
  }

  @Test
  void dataOlderThanTheMaximumAgeFailsTheCheck() {
    FakeCatalog.lastUpdated = CatalogAnswer.known(Instant.now().minus(Duration.ofHours(2)));
    assertFalse(action().execute(new Result(), 0).getResult());

    ActionCheckDataCatalog action = action();
    action.setMaxAge("");
    assertTrue(action.execute(new Result(), 0).getResult());
  }

  @Test
  void unknownFreshnessFailsUnlessUnknownAnswersAreIgnored() {
    FakeCatalog.lastUpdated = CatalogAnswer.notFound();
    assertFalse(action().execute(new Result(), 0).getResult());

    ActionCheckDataCatalog action = action();
    action.setUnknownAnswer(CatalogUnknownAnswer.IGNORE);
    assertTrue(action.execute(new Result(), 0).getResult());
  }

  @Test
  void aFailedOrErroredAssertionFailsTheCheckAndOneThatNeverRanDoesNot() {
    FakeCatalog.assertions =
        CatalogAnswer.known(
            List.of(
                new IAssertionCapability.AssertionResult(
                    "a", "No null ids", IAssertionCapability.Outcome.PASSED, Instant.now()),
                new IAssertionCapability.AssertionResult(
                    "b", null, IAssertionCapability.Outcome.UNKNOWN, null)));
    assertTrue(action().execute(new Result(), 0).getResult());

    FakeCatalog.assertions =
        CatalogAnswer.known(
            List.of(
                new IAssertionCapability.AssertionResult(
                    "c", "Volume", IAssertionCapability.Outcome.ERROR, Instant.now())));
    assertFalse(action().execute(new Result(), 0).getResult());

    ActionCheckDataCatalog action = action();
    action.setCheckAssertions(false);
    assertTrue(action.execute(new Result(), 0).getResult());
  }

  @Test
  void anUnreachableCatalogFailsTheCheckOrIsIgnored() {
    FakeCatalog.resolved = CatalogAnswer.unknown(Reason.UNREACHABLE, "connection refused");
    Result failed = action().execute(new Result(), 0);
    assertFalse(failed.getResult());
    assertEquals(0, failed.getNrErrors());

    ActionCheckDataCatalog action = action();
    action.setUnknownAnswer(CatalogUnknownAnswer.IGNORE);
    assertTrue(action.execute(new Result(), 0).getResult());
  }

  @Test
  void aCheckTheCatalogTypeDoesNotSupportIsAnUnknownAnswer() {
    ActionCheckDataCatalog action =
        action("minimal", new CatalogDataset(CatalogDatasetType.CATALOG_ID, null, null, "urn:x"));
    assertFalse(action.execute(new Result(), 0).getResult());

    action.setUnknownAnswer(CatalogUnknownAnswer.IGNORE);
    assertTrue(action.execute(new Result(), 0).getResult());
  }

  @Test
  void aTableIsAskedForWithTheIdentityLineageGivesIt() {
    ActionCheckDataCatalog action =
        action(
            "prod", new CatalogDataset(CatalogDatasetType.TABLE, "warehouse", "public", "orders"));

    assertTrue(action.execute(new Result(), 0).getResult());

    DatasetRef asked = FakeCatalog.asked.get(0);
    assertTrue(asked.namespace().endsWith("://db.example.com:5432"), asked.namespace());
    assertEquals("sales.public.orders", asked.name());
  }

  @Test
  void aMissingCatalogOrConnectionIsAnError() {
    Result noCatalog = action("not-there").execute(new Result(), 0);
    assertFalse(noCatalog.getResult());
    assertEquals(1, noCatalog.getNrErrors());

    Result noConnection =
        action("prod", new CatalogDataset(CatalogDatasetType.TABLE, "nowhere", null, "orders"))
            .execute(new Result(), 0);
    assertFalse(noConnection.getResult());
    assertEquals(1, noConnection.getNrErrors());
  }

  @Test
  void theOptionsSurviveASaveAndLoad() throws Exception {
    ActionCheckDataCatalog action =
        ActionSerializationTestUtil.testSerialization(
            "/check-data-catalog-action.xml", ActionCheckDataCatalog.class, provider);

    assertEquals("prod-datahub", action.getCatalogName());
    assertTrue(action.isCheckDeprecation());
    assertEquals("26", action.getMaxAge());
    assertEquals(CatalogAgeUnit.HOURS, action.getMaxAgeUnit());
    assertFalse(action.isCheckAssertions());
    assertEquals(CatalogUnknownAnswer.IGNORE, action.getUnknownAnswer());
    assertEquals(3, action.getDatasets().size());
    CatalogDataset table = action.getDatasets().get(0);
    assertEquals(CatalogDatasetType.TABLE, table.getType());
    assertEquals("warehouse", table.getConnection());
    assertEquals("public", table.getSchema());
    assertEquals("orders", table.getName());
    assertEquals(CatalogDatasetType.CATALOG_ID, action.getDatasets().get(2).getType());
  }

  @Test
  void aCloneHasItsOwnDatasets() {
    ActionCheckDataCatalog action = action();

    ActionCheckDataCatalog copy = (ActionCheckDataCatalog) action.clone();
    copy.getDatasets().get(0).setName("urn:changed");

    assertEquals("urn:x", action.getDatasets().get(0).getName());
  }

  @Test
  void theCheckReportsWhatIsMissing() {
    ActionCheckDataCatalog action = new ActionCheckDataCatalog("check");
    action.setMaxAge("soon");
    List<ICheckResult> remarks = new ArrayList<>();

    action.check(remarks, new WorkflowMeta(), new Variables(), provider);

    assertEquals(3, remarks.size());
    assertTrue(remarks.stream().allMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  void skippedChecksAreCounted() {
    ActionCheckDataCatalog action = action();
    action.setUnknownAnswer(CatalogUnknownAnswer.IGNORE);
    DatasetRef ref = DatasetRef.ofCatalogId("urn:x");

    FakeCatalog.lastUpdated = CatalogAnswer.notFound();
    FakeCatalog.assertions = CatalogAnswer.unknown(Reason.TIMEOUT, "no answer");
    assertEquals(List.of(), action.check(new FakeCatalog(), ref, Duration.ofHours(1)));
    assertEquals(2, action.skippedChecks());

    // Not knowing the dataset leaves nothing else to ask: all four checks are skipped
    FakeCatalog.resolved = CatalogAnswer.unknown(Reason.UNREACHABLE, "refused");
    assertEquals(List.of(), action.check(new FakeCatalog(), ref, Duration.ofHours(1)));
    assertEquals(4, action.skippedChecks());

    action.setCheckAssertions(false);
    action.check(new FakeCatalog(), ref, null);
    assertEquals(2, action.skippedChecks());
  }

  @Test
  void aTableOfAConnectionWithoutAHostIsAnError() throws Exception {
    DatabaseMeta embedded = new DatabaseMeta();
    embedded.setName("embedded");
    embedded.setDBName("/tmp/local.db");
    provider.getSerializer(DatabaseMeta.class).save(embedded);
    ActionCheckDataCatalog action =
        action("prod", new CatalogDataset(CatalogDatasetType.TABLE, "embedded", null, "orders"));
    action.setUnknownAnswer(CatalogUnknownAnswer.IGNORE);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
    assertTrue(FakeCatalog.asked.isEmpty());
  }

  @Test
  void aStoppedWorkflowStopsTheChecks() {
    @SuppressWarnings("unchecked")
    IWorkflowEngine<WorkflowMeta> workflow = mock(IWorkflowEngine.class);
    when(workflow.isStopped()).thenReturn(true);
    when(workflow.getLogLevel()).thenReturn(LogLevel.BASIC);
    ActionCheckDataCatalog action = action();
    action.setParentWorkflow(workflow);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(0, result.getNrErrors());
    assertTrue(FakeCatalog.asked.isEmpty());
  }

  @Test
  void theProblemsReadWell() {
    FakeCatalog catalog = new FakeCatalog();
    ActionCheckDataCatalog action = action();
    DatasetRef ref = DatasetRef.ofCatalogId("urn:x");

    FakeCatalog.resolved = CatalogAnswer.notFound();
    assertEquals(
        List.of("the catalog doesn't have this dataset"),
        action.check(catalog, ref, Duration.ofHours(1)));

    FakeCatalog.resolved = CatalogAnswer.known(ORDERS);
    FakeCatalog.deprecation =
        CatalogAnswer.known(new IDeprecationCapability.Deprecation(true, "Use v2", null));
    FakeCatalog.lastUpdated = CatalogAnswer.known(Instant.parse("2026-01-01T00:00:00Z"));
    assertEquals(
        List.of(
            "the dataset is deprecated: Use v2",
            "the data last changed at 2026-01-01T00:00:00Z, more than 1 hours ago"),
        action.check(catalog, ref, Duration.ofHours(1)));
  }

  @Test
  void evaluatesSuccessAndFailureHops() {
    assertTrue(new ActionCheckDataCatalog().isEvaluation());
  }
}
