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

package org.apache.hop.neo4j.transforms.output;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.neo4j.bolt.MemgraphGraphDialect;
import org.apache.hop.neo4j.bolt.Neo4jGraphDialect;
import org.apache.hop.neo4j.shared.FakeGraphDatabase;
import org.apache.hop.neo4j.shared.NamedGraphConnection;
import org.apache.hop.neo4j.shared.NeoConnectionUtils;
import org.apache.hop.neo4j.transforms.output.fields.LabelField;
import org.apache.hop.neo4j.transforms.output.fields.NodeField;
import org.apache.hop.neo4j.transforms.output.fields.PropertyField;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Neo4j Output creates the indexes of its fixed labels once per run (on the Spark driver, not in
 * every partition), and the ones of labels from a field on the first row.
 */
class Neo4JOutputPrepareOnceTest {

  private TransformMockHelper<Neo4JOutputMeta, Neo4JOutputData> helper;

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>("Neo4j Output", Neo4JOutputMeta.class, Neo4JOutputData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  private static LabelField fixed(String label) {
    LabelField field = new LabelField();
    field.setLabel(label);
    return field;
  }

  private static LabelField fromField(String fieldName) {
    LabelField field = new LabelField();
    field.setLabelField(fieldName);
    return field;
  }

  private static PropertyField property(String name, boolean primary) {
    PropertyField field = new PropertyField();
    field.setPropertyName(name);
    field.setPropertyValue(name);
    field.setPropertyPrimary(primary);
    return field;
  }

  private static Neo4JOutputMeta meta(LabelField... fromLabels) {
    Neo4JOutputMeta meta = new Neo4JOutputMeta();
    meta.setDefault();
    meta.setConnection("graph");
    meta.setCreatingIndexes(true);
    meta.getNodeFromField().getLabels().addAll(List.of(fromLabels));
    meta.getNodeFromField().getProperties().add(property("id", true));
    meta.getNodeFromField().getProperties().add(property("name", false));
    return meta;
  }

  @Test
  void onlyFixedLabelsWithAPrimaryPropertyArePreparedOnce() {
    Variables variables = new Variables();
    assertTrue(meta(fixed("Customer")).needsPrepareOnce(variables));
    assertFalse(
        meta(fromField("label")).needsPrepareOnce(variables), "the label is in the first row");

    Neo4JOutputMeta noIndexes = meta(fixed("Customer"));
    noIndexes.setCreatingIndexes(false);
    assertFalse(noIndexes.needsPrepareOnce(variables));

    Neo4JOutputMeta returningGraph = meta(fixed("Customer"));
    returningGraph.setReturningGraph(true);
    assertFalse(returningGraph.needsPrepareOnce(variables), "writes nothing to Neo4j");

    NodeField noPrimary = new NodeField();
    noPrimary.getLabels().add(fixed("Customer"));
    noPrimary.getProperties().add(property("name", false));
    assertFalse(Neo4JOutput.hasFixedLabelIndex(noPrimary));
    assertFalse(Neo4JOutput.hasFixedLabelIndex(null));
  }

  @Test
  void prepareOnceCreatesTheIndexesOfTheFixedLabels() throws Exception {
    FakeGraphDatabase database = new FakeGraphDatabase(Neo4jGraphDialect.INSTANCE, null);
    Neo4JOutputMeta meta = meta(fixed("Customer"), fromField("label"));
    Neo4JOutputData data = new Neo4JOutputData();
    data.graphConnection =
        new NamedGraphConnection("graph", null, new GraphDatabaseMeta("graph", database));
    Neo4JOutput output =
        new Neo4JOutput(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);

    output.prepareOnce();

    assertEquals(1, database.executed.size(), String.valueOf(database.executed));
    assertTrue(database.executed.get(0).contains("Customer"), database.executed.get(0));
  }

  @Test
  void anIndexIsCreatedInAWriteTransactionTheDriverRetriesOnADeadlock() throws Exception {
    IGraphConnection connection = mock(IGraphConnection.class);
    when(connection.getGraphDialect()).thenReturn(Neo4jGraphDialect.INSTANCE);
    when(connection.isSupportingTransactions()).thenReturn(true);
    NeoConnectionUtils.createNodeIndex(
        new LogChannel("test"), connection, List.of("Country"), List.of("id"));
    verify(connection).executeSchemaStatement(anyString(), eq(true));
    verify(connection, never()).execute(anyString(), any());

    // Memgraph doesn't change its schema in a transaction: the statement runs on its own
    IGraphConnection memgraph = mock(IGraphConnection.class);
    when(memgraph.getGraphDialect()).thenReturn(MemgraphGraphDialect.INSTANCE);
    when(memgraph.isSupportingTransactions()).thenReturn(true);
    NeoConnectionUtils.createNodeIndex(
        new LogChannel("test"), memgraph, List.of("Country"), List.of("id"));
    verify(memgraph, never()).executeSchemaStatement(anyString(), eq(true));
    verify(memgraph).executeSchemaStatement(anyString(), anyBoolean());
  }
}
