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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lineage.api.IDeclaresLineage;
import org.apache.hop.lineage.model.FileIoLineagePayload;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.apache.hop.lineage.model.RunLifecycleLineagePayload;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class LineageDefinitionsTest {

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  private static TransformMeta transform(String name, IDeclaresLineage.Declaration... declared) {
    ITransformMeta meta =
        mock(ITransformMeta.class, withSettings().extraInterfaces(IDeclaresLineage.class));
    when(((IDeclaresLineage) meta).declaredLineage(any(), any())).thenReturn(List.of(declared));
    TransformMeta transformMeta = new TransformMeta(name, name, meta);
    transformMeta.setTransformPluginId("Fake");
    return transformMeta;
  }

  /** read -> write, listed write first: the reader is described first, as a run finishes it. */
  private static PipelineMeta pipeline(IVariables variables) {
    PipelineMeta meta = new PipelineMeta();
    meta.setName("orders");
    meta.setFilename(variables.resolve("${PROJECT_HOME}/flows/orders.hpl"));
    TransformMeta write =
        transform(
            "Write",
            new IDeclaresLineage.DeclaredFile(FileIoOperation.WRITE, "${OUT}/orders.csv", null));
    TransformMeta read =
        transform(
            "Read",
            new IDeclaresLineage.DeclaredFile(FileIoOperation.READ, "/data/in.csv", null),
            new IDeclaresLineage.DeclaredSql(
                RelationalIoOperation.READ, "no such connection", "SELECT 1"));
    meta.addTransform(write);
    meta.addTransform(read);
    meta.addPipelineHop(new PipelineHopMeta(read, write));
    return meta;
  }

  @Test
  void aPipelineIsDescribedAsARunWouldDescribeIt() throws Exception {
    IVariables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "/projects/sales");
    variables.setVariable("OUT", "/data/out");

    List<LineageEvent> events = LineageDefinitions.ofPipeline(pipeline(variables), variables, null);

    List<String> shape = new ArrayList<>();
    for (LineageEvent event : events) {
      String subject =
          event.getContext().getTransformName() != null
              ? event.getContext().getTransformName()
              : event.getContext().getPipelineName();
      String what =
          event.getPayload() instanceof RunLifecycleLineagePayload lifecycle
              ? lifecycle.getPhase().name()
              : event.getKind().name();
      shape.add(subject + " " + what);
      assertEquals(
          "true", event.getContext().getAttributes().get(LineageDefinitions.ATTR_DEFINITION));
      assertEquals(
          "${PROJECT_HOME}/flows/orders.hpl", event.getContext().getHopFilenamePortableKey());
    }
    assertEquals(
        List.of(
            "orders STARTED",
            "orders PIPELINE_GRAPH",
            "Read STARTED",
            "Read FILE_IO",
            "Read FINISHED",
            "Write STARTED",
            "Write FILE_IO",
            "Write FINISHED",
            "orders FINISHED"),
        shape,
        "the SQL on a connection that doesn't exist is left out, the rest is published");

    FileIoLineagePayload read = (FileIoLineagePayload) events.get(3).getPayload();
    assertEquals("file:///data/in.csv", read.getSourceUri());
    assertNull(read.getTargetUri());
    FileIoLineagePayload written = (FileIoLineagePayload) events.get(6).getPayload();
    assertEquals("file:///data/out/orders.csv", written.getTargetUri());
    LineageEvent end = events.get(events.size() - 1);
    assertEquals(
        RunLifecyclePhase.FINISHED, ((RunLifecycleLineagePayload) end.getPayload()).getPhase());
    assertEquals(
        LineageRunLifecycleEmitter.SUCCESS,
        end.getContext().getAttributes().get(LineageRunLifecycleEmitter.OUTCOME));
  }

  @Test
  void theTransformsOfALoopFollowInPipelineOrder() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta a = transform("A");
    TransformMeta b = transform("B");
    TransformMeta c = transform("C");
    meta.addTransform(a);
    meta.addTransform(b);
    meta.addTransform(c);
    meta.addPipelineHop(new PipelineHopMeta(b, c));
    meta.addPipelineHop(new PipelineHopMeta(c, b));

    List<String> order = new ArrayList<>();
    for (TransformMeta transform : LineageDefinitions.inGraphOrder(meta)) {
      order.add(transform.getName());
    }

    assertEquals(List.of("A", "B", "C"), order);
    assertTrue(order.containsAll(List.of("A", "B", "C")));
  }

  @Test
  void eachTransformHasItsOwnRun() {
    IVariables variables = new Variables();
    List<LineageEvent> events = LineageDefinitions.ofPipeline(pipeline(variables), variables, null);

    String pipelineRun = events.get(0).getContext().getLogChannelId();
    String readRun = events.get(2).getContext().getLogChannelId();
    assertEquals(
        pipelineRun, events.get(2).getContext().getAttributes().get("pipelineLogChannelId"));
    assertTrue(!pipelineRun.equals(readRun));
    assertEquals(LineageEventKind.FILE_IO, events.get(3).getKind());
    assertEquals(readRun, events.get(3).getContext().getLogChannelId());
  }
}
