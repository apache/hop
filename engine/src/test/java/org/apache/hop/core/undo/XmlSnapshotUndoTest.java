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
package org.apache.hop.core.undo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class XmlSnapshotUndoTest {

  private final MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void pipelineRoundTripUndoRedoAndRedoClearedOnNewChange() throws Exception {
    PipelineMeta pipelineMeta = samplePipeline("before");
    pipelineMeta.setNameSynchronizedWithFilename(false);
    pipelineMeta.setFilename("/tmp/sample.hpl");
    XmlSnapshotUndo<PipelineMeta> undo = pipelineUndo(10);

    undo.markChange(pipelineMeta, metadataProvider);
    pipelineMeta.setName("after");
    pipelineMeta.getTransform(0).setLocation(200, 200);

    assertTrue(undo.canUndo());
    assertFalse(undo.canRedo());

    assertTrue(undo.undo(pipelineMeta, metadataProvider, "/tmp/sample.hpl"));
    assertEquals("before", pipelineMeta.getName());
    assertEquals(50, pipelineMeta.getTransform(0).getLocation().x);
    assertEquals("/tmp/sample.hpl", pipelineMeta.getFilename());
    assertEquals("A", pipelineMeta.getTransform(0).getName());
    assertEquals("B", pipelineMeta.getTransform(1).getName());
    assertEquals("A", pipelineMeta.getPipelineHop(0).getFromTransform().getName());
    assertEquals("B", pipelineMeta.getPipelineHop(0).getToTransform().getName());
    assertEquals("note", pipelineMeta.getNote(0).getNote());
    assertTrue(undo.canRedo());

    assertTrue(undo.redo(pipelineMeta, metadataProvider, "/tmp/sample.hpl"));
    assertEquals("after", pipelineMeta.getName());
    assertEquals(200, pipelineMeta.getTransform(0).getLocation().x);
    assertEquals("/tmp/sample.hpl", pipelineMeta.getFilename());

    undo.markChange(pipelineMeta, metadataProvider);
    pipelineMeta.setName("newer");
    assertFalse(undo.canRedo());
    assertTrue(undo.undo(pipelineMeta, metadataProvider, "/tmp/sample.hpl"));
    assertEquals("after", pipelineMeta.getName());
  }

  @Test
  void workflowRoundTripRestoresHopsAndNotes() throws Exception {
    WorkflowMeta workflowMeta = sampleWorkflow("wf-before");
    workflowMeta.setNameSynchronizedWithFilename(false);
    workflowMeta.setFilename("/tmp/sample.hwf");
    XmlSnapshotUndo<WorkflowMeta> undo = workflowUndo(10);

    undo.markChange(workflowMeta, metadataProvider);
    workflowMeta.setName("wf-after");
    workflowMeta.removeWorkflowHop(0);

    assertTrue(undo.undo(workflowMeta, metadataProvider, "/tmp/sample.hwf"));
    assertEquals("wf-before", workflowMeta.getName());
    assertEquals(1, workflowMeta.nrWorkflowHops());
    assertEquals("start", workflowMeta.getWorkflowHop(0).getFromAction().getName());
    assertEquals("dummy", workflowMeta.getWorkflowHop(0).getToAction().getName());
    assertEquals("hello", workflowMeta.getNote(0).getNote());
    assertEquals("/tmp/sample.hwf", workflowMeta.getFilename());
  }

  @Test
  void trimHonorsMaxUndo() throws Exception {
    PipelineMeta pipelineMeta = samplePipeline("v0");
    XmlSnapshotUndo<PipelineMeta> undo = pipelineUndo(2);

    undo.markChange(pipelineMeta, metadataProvider);
    pipelineMeta.setName("v1");
    undo.markChange(pipelineMeta, metadataProvider);
    pipelineMeta.setName("v2");
    undo.markChange(pipelineMeta, metadataProvider);
    pipelineMeta.setName("v3");

    assertEquals(2, undo.getUndoSize());
    assertTrue(undo.undo(pipelineMeta, metadataProvider, null));
    assertEquals("v2", pipelineMeta.getName());
    assertTrue(undo.undo(pipelineMeta, metadataProvider, null));
    assertEquals("v1", pipelineMeta.getName());
    assertFalse(undo.undo(pipelineMeta, metadataProvider, null));
  }

  @Test
  void applyingSnapshotDoesNotRecord() throws Exception {
    PipelineMeta pipelineMeta = samplePipeline("orig");
    AtomicInteger restoreCalls = new AtomicInteger();
    XmlSnapshotUndo<PipelineMeta> undo =
        new XmlSnapshotUndo<>(
            PipelineMeta.class,
            PipelineMeta.XML_TAG,
            (target, node, provider, filename) -> {
              restoreCalls.incrementAndGet();
              target.restoreContentFromXml(node, filename, provider);
            },
            () -> 10);

    undo.markChange(pipelineMeta, metadataProvider);
    pipelineMeta.setName("changed");
    assertTrue(undo.undo(pipelineMeta, metadataProvider, null));
    assertEquals(1, restoreCalls.get());
    // Nested mark during restore is skipped because applyingSnapshot is true.
    assertEquals(1, undo.getRedoSize());
    assertEquals(0, undo.getUndoSize());
  }

  @Test
  void sameXmlContentIgnoresGzipHeaderTimestamp() throws Exception {
    PipelineMeta pipelineMeta = samplePipeline("same");
    XmlSnapshotUndo<PipelineMeta> undo = pipelineUndo(5);
    byte[] first = undo.captureSnapshot(pipelineMeta, metadataProvider);
    byte[] second = undo.captureSnapshot(pipelineMeta, metadataProvider);
    assertTrue(XmlSnapshotUndo.sameXmlContent(first, second));
  }

  @Test
  void restoreDoesNotRequireClone() throws Exception {
    PipelineMeta pipelineMeta = samplePipeline("clone-free");
    XmlSnapshotUndo<PipelineMeta> undo = pipelineUndo(5);
    undo.markChange(pipelineMeta, metadataProvider);
    pipelineMeta.addTransform(new TransformMeta("C", new DummyMeta()));
    assertTrue(undo.undo(pipelineMeta, metadataProvider, null));
    assertEquals(2, pipelineMeta.nrTransforms());
  }

  private static XmlSnapshotUndo<PipelineMeta> pipelineUndo(int max) {
    return new XmlSnapshotUndo<>(
        PipelineMeta.class,
        PipelineMeta.XML_TAG,
        (target, node, provider, filename) ->
            target.restoreContentFromXml(node, filename, provider),
        () -> max);
  }

  private static XmlSnapshotUndo<WorkflowMeta> workflowUndo(int max) {
    return new XmlSnapshotUndo<>(
        WorkflowMeta.class,
        WorkflowMeta.XML_TAG,
        (target, node, provider, filename) ->
            target.restoreContentFromXml(node, filename, provider),
        () -> max);
  }

  private static PipelineMeta samplePipeline(String name) {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName(name);
    TransformMeta a = new TransformMeta("A", new DummyMeta());
    a.setLocation(50, 50);
    TransformMeta b = new TransformMeta("B", new DummyMeta());
    b.setLocation(150, 50);
    pipelineMeta.addTransform(a);
    pipelineMeta.addTransform(b);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(a, b));
    pipelineMeta.addNote(new NotePadMeta("note", 10, 10, 80, 40));
    return pipelineMeta;
  }

  private static WorkflowMeta sampleWorkflow(String name) {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName(name);
    ActionMeta start = new ActionMeta(new ActionDummy("start"));
    start.setLocation(50, 50);
    ActionMeta dummy = new ActionMeta(new ActionDummy("dummy"));
    dummy.setLocation(150, 50);
    workflowMeta.addAction(start);
    workflowMeta.addAction(dummy);
    workflowMeta.addWorkflowHop(new WorkflowHopMeta(start, dummy));
    workflowMeta.addNote(new NotePadMeta("hello", 10, 10, 80, 40));
    return workflowMeta;
  }
}
