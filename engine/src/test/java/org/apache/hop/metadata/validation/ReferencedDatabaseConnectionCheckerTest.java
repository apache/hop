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
package org.apache.hop.metadata.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.ActionMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReferencedDatabaseConnectionCheckerTest {

  private IHopMetadataProvider metadataProvider;
  private Variables variables;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() throws Exception {
    variables = new Variables();
    metadataProvider = mock(IHopMetadataProvider.class);
    IHopMetadataSerializer<DatabaseMeta> serializer = mock(IHopMetadataSerializer.class);
    when(metadataProvider.getSerializer(DatabaseMeta.class)).thenReturn(serializer);
    when(serializer.exists(anyString())).thenAnswer(inv -> "sales-db".equals(inv.getArgument(0)));
  }

  static class ConnMeta {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String connection;

    ConnMeta(String connection) {
      this.connection = connection;
    }
  }

  static class NestedItem {
    @HopMetadataProperty(
        key = "name",
        hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String name;

    NestedItem(String name) {
      this.name = name;
    }
  }

  static class NestedMeta {
    @HopMetadataProperty List<NestedItem> items;

    NestedMeta(String... names) {
      items = java.util.Arrays.stream(names).map(NestedItem::new).toList();
    }
  }

  static class TwoConnectionsMeta {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String referenceConnection;

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String compareConnection;
  }

  static class UnannotatedMeta {
    @HopMetadataProperty(key = "connection")
    String connection = "missing";
  }

  static class TestAction extends ActionBase {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String connection;

    TestAction(String name, String connection) {
      super(name, "");
      this.connection = connection;
    }

    @Override
    public org.apache.hop.core.Result execute(org.apache.hop.core.Result prevResult, int nr) {
      return prevResult;
    }
  }

  private List<ICheckResult> check(Object meta, String ownerName) {
    return ReferencedDatabaseConnectionChecker.checkObject(
        meta, "Transform", ownerName, null, variables, metadataProvider);
  }

  @Test
  void missingLiteralNameIsAWarning() {
    List<ICheckResult> remarks = check(new ConnMeta("missing-db"), "Read sales");

    assertEquals(1, remarks.size());
    assertEquals(
        ReferencedDatabaseConnectionChecker.ERROR_DOES_NOT_EXIST, remarks.get(0).getErrorCode());
    assertTrue(remarks.get(0).getText().contains("missing-db"));
    assertTrue(remarks.get(0).getText().contains("Read sales"));
  }

  @Test
  void existingLiteralNameIsSilent() {
    assertTrue(check(new ConnMeta("sales-db"), "Read sales").isEmpty());
  }

  @Test
  void unresolvedVariableIsSkipped() {
    assertTrue(check(new ConnMeta("${CONNECTION}"), "Read sales").isEmpty());
  }

  @Test
  void resolvedVariableToExistingNameIsSilent() {
    variables.setVariable("CONNECTION", "sales-db");
    assertTrue(check(new ConnMeta("${CONNECTION}"), "Read sales").isEmpty());
  }

  @Test
  void resolvedVariableToMissingNameWarns() {
    variables.setVariable("CONNECTION", "missing-db");
    List<ICheckResult> remarks = check(new ConnMeta("${CONNECTION}"), "Read sales");

    assertEquals(1, remarks.size());
    assertEquals(
        ReferencedDatabaseConnectionChecker.ERROR_DOES_NOT_EXIST, remarks.get(0).getErrorCode());
    assertTrue(remarks.get(0).getText().contains("missing-db"));
  }

  @Test
  void mixedUnresolvedVariableIsSkipped() {
    assertTrue(check(new ConnMeta("db_${ENV}"), "Read sales").isEmpty());
  }

  @Test
  void emptyConnectionIsAWarning() {
    List<ICheckResult> remarks = check(new ConnMeta(""), "Read sales");

    assertEquals(1, remarks.size());
    assertEquals(
        ReferencedDatabaseConnectionChecker.ERROR_NOT_ASSIGNED, remarks.get(0).getErrorCode());
  }

  @Test
  void nestedListConnectionsAreChecked() {
    List<ICheckResult> remarks =
        check(new NestedMeta("sales-db", "missing-db"), "Check connections");

    assertEquals(1, remarks.size());
    assertTrue(remarks.get(0).getText().contains("missing-db"));
  }

  @Test
  void twoConnectionFieldsOnOneObject() {
    TwoConnectionsMeta meta = new TwoConnectionsMeta();
    meta.referenceConnection = "sales-db";
    meta.compareConnection = "other-db";

    List<ICheckResult> remarks = check(meta, "Compare tables");

    assertEquals(1, remarks.size());
    assertTrue(remarks.get(0).getText().contains("other-db"));
  }

  @Test
  void unannotatedConnectionFieldIsIgnored() {
    assertTrue(check(new UnannotatedMeta(), "Legacy").isEmpty());
  }

  @Test
  void checksAPipelineTransform() {
    ConnTransformMeta transform = new ConnTransformMeta();
    transform.connection = "missing-db";
    TransformMeta transformMeta = new TransformMeta("Read sales", transform);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(transformMeta);

    List<ICheckResult> remarks =
        ReferencedDatabaseConnectionChecker.checkPipeline(
            pipelineMeta, variables, metadataProvider);

    assertEquals(1, remarks.size());
    assertEquals(transformMeta, remarks.get(0).getSourceInfo());
  }

  @Test
  void checksAWorkflowAction() {
    TestAction action = new TestAction("Run SQL", "missing-db");
    ActionMeta actionMeta = new ActionMeta(action);

    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.addAction(actionMeta);

    List<ICheckResult> remarks =
        ReferencedDatabaseConnectionChecker.checkWorkflow(
            workflowMeta, variables, metadataProvider);

    assertEquals(1, remarks.size());
    assertEquals(
        ReferencedDatabaseConnectionChecker.ERROR_DOES_NOT_EXIST, remarks.get(0).getErrorCode());
  }

  /** A Dummy transform that also carries an annotated connection name, for pipeline-level tests. */
  static class ConnTransformMeta extends DummyMeta {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String connection;
  }
}
