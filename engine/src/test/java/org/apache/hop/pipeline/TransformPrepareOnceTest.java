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

package org.apache.hop.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * The once-per-run preparation of transforms: once per transform, whatever the number of copies.
 */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class TransformPrepareOnceTest {

  @BeforeEach
  void setUp() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            PrepareOnceTestMeta.class.getName(), TransformPluginType.class, Transform.class);
    PrepareOnceTestTransform.reset();
  }

  /** src (Injector with fields id and name) -> prepared (3 copies). */
  private static PipelineMeta pipeline(int copies) {
    InjectorMeta injector = new InjectorMeta();
    injector.getInjectorFields().add(new InjectorField("id", "Integer", "-1", "-1"));
    injector.getInjectorFields().add(new InjectorField("name", "String", "-1", "-1"));
    TransformMeta src = new TransformMeta("src", injector);
    src.setTransformPluginId("Injector");
    TransformMeta prepared = new TransformMeta("prepared", new PrepareOnceTestMeta());
    prepared.setTransformPluginId(PrepareOnceTestMeta.ID);
    prepared.setCopies(copies);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("prepare-once");
    pipelineMeta.addTransform(src);
    pipelineMeta.addTransform(prepared);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(src, prepared));
    return pipelineMeta;
  }

  private static void run(LocalPipelineEngine pipeline) throws Exception {
    pipeline.setMetadataProvider(new MemoryMetadataProvider());
    pipeline.prepareExecution();
    RowProducer producer = pipeline.addRowProducer("src", 0);
    pipeline.startThreads();
    producer.finished();
    pipeline.waitUntilFinished();
    assertEquals(0, pipeline.getErrors());
  }

  @Test
  void theLocalEnginePreparesATransformOnceForAllItsCopies() throws Exception {
    run(new LocalPipelineEngine(pipeline(3), new Variables(), null));
    assertEquals(1, PrepareOnceTestTransform.PREPARED.get());
    assertEquals(List.of(true, true, true), PrepareOnceTestTransform.FLAGGED);
    // Before the first row: the fields the previous transform declares
    assertEquals(
        List.of("id", "name"), List.of(PrepareOnceTestTransform.preparedWith.getFieldNames()));
  }

  /** Nothing to race: the transform does the work on its first row, from the real row layout. */
  @Test
  void aSingleCopyLeavesTheWorkToTheFirstRow() throws Exception {
    run(new LocalPipelineEngine(pipeline(1), new Variables(), null));
    assertEquals(0, PrepareOnceTestTransform.PREPARED.get());
    assertEquals(List.of(false), PrepareOnceTestTransform.FLAGGED);
  }

  /**
   * The declared layout is only for the preparation. Rows can arrive with another one (Get rows
   * from result declares its fields for design time only), and copy 0 reads them with that one like
   * every other copy.
   */
  @Test
  void everyCopyReadsTheRowsWithTheirOwnLayout() throws Exception {
    LocalPipelineEngine pipeline = new LocalPipelineEngine(pipeline(3), new Variables(), null);
    pipeline.setMetadataProvider(new MemoryMetadataProvider());
    pipeline.prepareExecution();
    RowProducer producer = pipeline.addRowProducer("src", 0);
    pipeline.startThreads();
    IRowMeta arriving = new RowMeta();
    arriving.addValueMeta(new ValueMetaString("name"));
    arriving.addValueMeta(new ValueMetaInteger("id"));
    arriving.addValueMeta(new ValueMetaString("extra"));
    // Distributed round robin: one row for every copy
    for (long id = 1; id <= 3; id++) {
      producer.putRow(arriving, new Object[] {"row " + id, id, "x"});
    }
    producer.finished();
    pipeline.waitUntilFinished();
    assertEquals(0, pipeline.getErrors());

    assertEquals(
        List.of("id", "name"), List.of(PrepareOnceTestTransform.preparedWith.getFieldNames()));
    assertEquals(3, PrepareOnceTestTransform.READ_WITH.size());
    for (List<String> fields : PrepareOnceTestTransform.READ_WITH.values()) {
      assertEquals(List.of("name", "id", "extra"), fields);
    }
  }

  @Test
  void aTransactionalPipelineLeavesTheWorkToTheFirstRow() throws Exception {
    LocalPipelineEngine pipeline = new LocalPipelineEngine(pipeline(2), new Variables(), null);
    pipeline.getExtensionDataMap().put(Const.CONNECTION_GROUP, "one transaction");
    run(pipeline);
    assertEquals(0, PrepareOnceTestTransform.PREPARED.get());
    assertEquals(List.of(false, false), PrepareOnceTestTransform.FLAGGED);
  }

  @Test
  void aPerPartitionPipelineOnlyFlagsWhatTheDriverPrepared() throws Exception {
    Variables prepared = new Variables();
    prepared.setVariable(
        Const.INTERNAL_VARIABLE_PIPELINE_PREPARED_ONCE_TRANSFORMS,
        TransformPrepareOnce.preparedOnDriverValue(List.of("other", "prepared")));
    LocalPipelineEngine pipeline = new LocalPipelineEngine(pipeline(2), prepared, null);
    pipeline.setPreparingTransformsOnce(false);
    run(pipeline);
    assertEquals(0, PrepareOnceTestTransform.PREPARED.get());
    assertEquals(List.of(true, true), PrepareOnceTestTransform.FLAGGED);

    // Not named by the driver: the transform does its own work, as before
    PrepareOnceTestTransform.reset();
    LocalPipelineEngine unprepared = new LocalPipelineEngine(pipeline(1), new Variables(), null);
    unprepared.setPreparingTransformsOnce(false);
    run(unprepared);
    assertEquals(0, PrepareOnceTestTransform.PREPARED.get());
    assertEquals(List.of(false), PrepareOnceTestTransform.FLAGGED);
  }

  @Test
  void aDriverPreparesEveryConnectedTransformOnce() throws Exception {
    PipelineMeta pipelineMeta = pipeline(4);
    // A transform no enabled hop connects doesn't run: not prepared either
    TransformMeta lonely = new TransformMeta("lonely", new PrepareOnceTestMeta());
    lonely.setTransformPluginId(PrepareOnceTestMeta.ID);
    pipelineMeta.addTransform(lonely);

    List<String> names =
        TransformPrepareOnce.prepareOnDriver(
            pipelineMeta,
            new Variables(),
            new MemoryMetadataProvider(),
            new LoggingObject("prepare-once-test"));
    assertEquals(List.of("prepared"), names);
    assertEquals(1, PrepareOnceTestTransform.PREPARED.get());
    assertEquals(
        List.of("id", "name"), List.of(PrepareOnceTestTransform.preparedWith.getFieldNames()));
    assertTrue(TransformPrepareOnce.preparedOnDriver("a\nb\n").containsAll(List.of("a", "b")));
  }
}
