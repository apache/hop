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

package org.apache.hop.ui.hopgui.file.pipeline;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.hop.core.gui.Point;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.IRowDistribution;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.apache.hop.ui.hopgui.delegates.HopGuiUndoDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineHopContext;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineTransformDelegate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HopGuiPipelineHopContextTest {

  private HopGuiPipelineGraph graph;
  private HopGuiUndoDelegate undoDelegate;
  private PipelineMeta pipelineMeta;
  private TransformMeta fromTransform;
  private TransformMeta toTransform;
  private PipelineHopMeta hopMeta;
  private HopGuiPipelineHopContext context;

  @BeforeEach
  void setUp() {
    graph = mock(HopGuiPipelineGraph.class, org.mockito.Mockito.CALLS_REAL_METHODS);
    undoDelegate = mock(HopGuiUndoDelegate.class);
    graph.setUndoDelegate(undoDelegate);
    pipelineMeta = mock(PipelineMeta.class);
    fromTransform = new TransformMeta("From", null);
    toTransform = new TransformMeta("To", null);
    hopMeta = new PipelineHopMeta(fromTransform, toTransform);
    context = new HopGuiPipelineHopContext(pipelineMeta, hopMeta, graph, new Point(0, 0));
  }

  @Test
  void testFilterHopActions_EnableDisable() {
    hopMeta.setEnabled(true);
    assertFalse(
        graph.filterHopActions(HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_ENABLE, context));
    assertTrue(
        graph.filterHopActions(HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_DISABLE, context));

    hopMeta.setEnabled(false);
    assertTrue(
        graph.filterHopActions(HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_ENABLE, context));
    assertFalse(
        graph.filterHopActions(HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_DISABLE, context));
  }

  @Test
  void testFilterHopActions_DistributeAndCopy() {
    // When from-transform is distributing rows
    fromTransform.setDistributes(true);
    assertTrue(
        graph.filterHopActions(
            HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_ROWS_COPY, context));
    assertFalse(
        graph.filterHopActions(
            HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_ROWS_DISTRIBUTE, context));

    // When from-transform is copying rows
    fromTransform.setDistributes(false);
    assertFalse(
        graph.filterHopActions(
            HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_ROWS_COPY, context));
    assertTrue(
        graph.filterHopActions(
            HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_ROWS_DISTRIBUTE, context));
  }

  @Test
  void testFilterHopActions_Partitioning() {
    // When to-transform is not partitioned
    toTransform.setTransformPartitioningMeta(new TransformPartitioningMeta());
    assertFalse(toTransform.isPartitioned());
    assertTrue(
        graph.filterHopActions(
            HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_SET_PARTITIONING, context));
    assertFalse(
        graph.filterHopActions(
            HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_REMOVE_PARTITIONING, context));

    // When to-transform is partitioned
    TransformPartitioningMeta partMeta = new TransformPartitioningMeta();
    partMeta.setMethodType(TransformPartitioningMeta.PARTITIONING_METHOD_MIRROR);
    partMeta.setPartitionSchema(new PartitionSchema("schema1", new java.util.ArrayList<>()));
    toTransform.setTransformPartitioningMeta(partMeta);
    assertTrue(toTransform.isPartitioned());
    assertFalse(
        graph.filterHopActions(
            HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_SET_PARTITIONING, context));
    assertTrue(
        graph.filterHopActions(
            HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_REMOVE_PARTITIONING, context));
  }

  @Test
  void testFilterHopActions_NullHopAndTransforms() {
    HopGuiPipelineHopContext nullHopContext =
        new HopGuiPipelineHopContext(pipelineMeta, null, graph, new Point(0, 0));
    assertFalse(
        graph.filterHopActions(
            HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_ENABLE, nullHopContext));
    assertFalse(
        graph.filterHopActions(
            HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_ROWS_COPY, nullHopContext));
    assertFalse(
        graph.filterHopActions(
            HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_SET_PARTITIONING, nullHopContext));

    PipelineHopMeta emptyHop = new PipelineHopMeta((TransformMeta) null, (TransformMeta) null);
    HopGuiPipelineHopContext emptyHopContext =
        new HopGuiPipelineHopContext(pipelineMeta, emptyHop, graph, new Point(0, 0));
    assertFalse(
        graph.filterHopActions(
            HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_ROWS_COPY, emptyHopContext));
    assertFalse(
        graph.filterHopActions(
            HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_ROWS_DISTRIBUTE, emptyHopContext));
    assertFalse(
        graph.filterHopActions(
            HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_SET_PARTITIONING, emptyHopContext));
    assertFalse(
        graph.filterHopActions(
            HopGuiPipelineGraph.ACTION_ID_PIPELINE_GRAPH_HOP_REMOVE_PARTITIONING, emptyHopContext));
  }

  @Test
  void testFilterHopActions_DefaultAction() {
    assertTrue(graph.filterHopActions("some-other-action", context));
  }

  @Test
  void testSetHopDistributes() {
    fromTransform.setDistributes(false);
    fromTransform.setRowDistribution(mock(IRowDistribution.class));
    doNothing().when(graph).redraw();
    doNothing().when(graph).updateGui();

    graph.setHopDistributes(context);

    assertTrue(fromTransform.isDistributes());
    assertNull(fromTransform.getRowDistribution());
    verify(graph).redraw();
    verify(graph).updateGui();
    verify(undoDelegate).addUndoChange(any(), any(), any(), any());
  }

  @Test
  void testSetHopCopies() {
    fromTransform.setDistributes(true);
    fromTransform.setRowDistribution(mock(IRowDistribution.class));
    doNothing().when(graph).redraw();
    doNothing().when(graph).updateGui();

    graph.setHopCopies(context);

    assertFalse(fromTransform.isDistributes());
    assertNull(fromTransform.getRowDistribution());
    verify(graph).redraw();
    verify(graph).updateGui();
    verify(undoDelegate).addUndoChange(any(), any(), any(), any());
  }

  @Test
  void testSetHopPartitioning() {
    HopGuiPipelineTransformDelegate delegate = mock(HopGuiPipelineTransformDelegate.class);
    graph.pipelineTransformDelegate = delegate;

    graph.setHopPartitioning(context);

    verify(delegate).editTransformPartitioning(pipelineMeta, toTransform);
  }

  @Test
  void testRemoveHopPartitioning() {
    TransformPartitioningMeta partMeta = new TransformPartitioningMeta();
    partMeta.setMethodType(TransformPartitioningMeta.PARTITIONING_METHOD_MIRROR);
    partMeta.setPartitionSchema(new PartitionSchema("schema1", new java.util.ArrayList<>()));
    toTransform.setTransformPartitioningMeta(partMeta);
    toTransform.setChanged(false);

    doNothing().when(graph).redraw();
    doNothing().when(graph).updateGui();

    graph.removeHopPartitioning(context);

    assertFalse(toTransform.isPartitioned());
    assertNull(toTransform.getTargetTransformPartitioningMeta());
    assertTrue(toTransform.hasChanged());
    verify(graph).redraw();
    verify(graph).updateGui();
    verify(undoDelegate).addUndoChange(any(), any(), any(), any());
  }
}
