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

package org.apache.hop.lineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lineage.api.RelationalLineage;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineagePayload;
import org.apache.hop.lineage.model.RelationalIoLineagePayload;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.apache.hop.lineage.model.RelationalTable;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** A table a transform declares it reads, such as Database lookup's lookup table. */
class LineageDeclaredReadTest {

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  /** Transform metadata that reads a table, as Database lookup's does. */
  @RelationalLineage(operation = RelationalIoOperation.READ)
  public abstract static class LooksUpATable implements ITransformMeta {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    private String connection = "warehouse";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
    private String schemaName = "public";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
    private String tableName = "customers";

    @Override
    public Object clone() {
      return this;
    }
  }

  private static LooksUpATable lookup() {
    return mock(
        LooksUpATable.class, withSettings().useConstructor().defaultAnswer(CALLS_REAL_METHODS));
  }

  private static DatabaseMeta warehouse() {
    DatabaseMeta db = mock(DatabaseMeta.class);
    when(db.getPluginId()).thenReturn("POSTGRESQL");
    when(db.getHostname()).thenReturn("db");
    when(db.getPort()).thenReturn("5432");
    when(db.getDatabaseName()).thenReturn("sales");
    return db;
  }

  private static void assertReadsCustomers(LineagePayload payload) {
    RelationalIoLineagePayload read = (RelationalIoLineagePayload) payload;
    assertEquals(RelationalIoOperation.READ, read.getOperation());
    assertEquals("postgres://db:5432", read.getDatasetNamespace());
    assertEquals(1, read.getInputs().size());
    RelationalTable table = read.getInputs().get(0);
    assertEquals("sales", table.getDatabase());
    assertEquals("public", table.getSchema());
    assertEquals("customers", table.getTable());
    assertTrue(read.getOutputs().isEmpty(), "a lookup writes nothing");
    assertNull(read.getOutputSchema(), "the stream fields are not the table's columns");
    assertTrue(read.getWriteColumns().isEmpty());
  }

  @Test
  void aRunReportsTheTableAsARead() {
    LineageHub hub = mock(LineageHub.class);
    ITransform transform = mock(ITransform.class);
    ILogChannel log = mock(ILogChannel.class);
    when(log.getLogChannelId()).thenReturn("lookup-run");
    when(transform.getLogChannel()).thenReturn(log);
    when(transform.getTransformName()).thenReturn("Look up customer");
    when(transform.resolve(anyString())).thenAnswer(inv -> inv.getArgument(0));
    ITransformMeta meta = lookup();
    when(transform.getMeta()).thenReturn(meta);
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    DatabaseMeta db = warehouse();
    when(pipelineMeta.findDatabase(eq("warehouse"), any(IVariables.class))).thenReturn(db);
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    when(pipeline.getPipelineMeta()).thenReturn(pipelineMeta);
    when(pipeline.getLogChannelId()).thenReturn("pipeline-run");
    when(transform.getPipeline()).thenReturn(pipeline);

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      LineageRelationalIoEmitter.emitDeclaredRelationalIo(transform, true);
    }

    ArgumentCaptor<LineageEvent> event = ArgumentCaptor.forClass(LineageEvent.class);
    verify(hub).emit(event.capture());
    assertReadsCustomers(event.getValue().getPayload());
  }

  @Test
  void aPublicationDescribesTheTableAsARead() throws Exception {
    IVariables variables = new Variables();
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    DatabaseMeta db = warehouse();
    when(pipelineMeta.findDatabase("warehouse", variables)).thenReturn(db);
    TransformMeta transformMeta =
        new TransformMeta("Look up customer", "Look up customer", lookup());

    List<LineagePayload> payloads =
        LineageDefinitions.declaredIo(pipelineMeta, transformMeta, variables, true);

    assertEquals(1, payloads.size());
    assertReadsCustomers(payloads.get(0));
  }
}
