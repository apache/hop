/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.layout.LayeredGraphLayout;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Layered (Sugiyama-style) DAG auto-layout for a {@link PipelineMeta}.
 *
 * <p>It rewrites the x/y coordinates of every transform so the graph reads cleanly left-to-right
 * with no overlaps. This is a thin adapter over {@link LayeredGraphLayout}; see that class for the
 * algorithm details. Robust against empty pipelines, a single transform, cycles and disconnected
 * nodes.
 */
public final class PipelineMetaLayout {

  public static final int DEFAULT_X_SPACING = LayeredGraphLayout.DEFAULT_X_SPACING;
  public static final int DEFAULT_Y_SPACING = LayeredGraphLayout.DEFAULT_Y_SPACING;
  public static final int MARGIN_X = LayeredGraphLayout.MARGIN_X;
  public static final int MARGIN_Y = LayeredGraphLayout.MARGIN_Y;

  private PipelineMetaLayout() {}

  /** Layout the whole pipeline with default options. */
  public static void layout(PipelineMeta pipelineMeta) {
    layout(pipelineMeta, new LayeredGraphLayout.Options());
  }

  /**
   * Layout the whole pipeline with explicit options.
   *
   * @param pipelineMeta the pipeline to lay out
   * @param options the layout tuning options
   */
  public static void layout(PipelineMeta pipelineMeta, LayeredGraphLayout.Options options) {
    layout(pipelineMeta, options, null);
  }

  /**
   * Layout the given pipeline. When {@code subset} is non-empty, only those transforms are arranged
   * (using the hops between them) and the result is anchored to the top-left of the area the subset
   * originally occupied, so the rest of the graph stays put. When {@code subset} is null or empty,
   * the whole pipeline is laid out.
   *
   * @param pipelineMeta the pipeline to lay out
   * @param options the layout tuning options
   * @param subset the transforms to arrange, or null/empty for the whole pipeline
   */
  public static void layout(
      PipelineMeta pipelineMeta, LayeredGraphLayout.Options options, List<TransformMeta> subset) {
    if (pipelineMeta == null) {
      return;
    }
    if (options == null) {
      options = new LayeredGraphLayout.Options();
    }

    final List<TransformMeta> transforms = new ArrayList<>();
    Map<TransformMeta, Integer> index = new HashMap<>();
    if (subset == null || subset.isEmpty()) {
      for (int i = 0; i < pipelineMeta.nrTransforms(); i++) {
        TransformMeta t = pipelineMeta.getTransform(i);
        index.put(t, transforms.size());
        transforms.add(t);
      }
    } else {
      for (TransformMeta t : subset) {
        if (t != null && !index.containsKey(t)) {
          index.put(t, transforms.size());
          transforms.add(t);
        }
      }
    }
    int n = transforms.size();
    if (n == 0) {
      return;
    }

    List<int[]> edges = new ArrayList<>();
    for (int h = 0; h < pipelineMeta.nrPipelineHops(); h++) {
      PipelineHopMeta hop = pipelineMeta.getPipelineHop(h);
      TransformMeta from = hop.getFromTransform();
      TransformMeta to = hop.getToTransform();
      if (from == null || to == null) {
        continue;
      }
      Integer fi = index.get(from);
      Integer ti = index.get(to);
      if (fi == null || ti == null) {
        continue;
      }
      edges.add(new int[] {fi, ti});
    }

    boolean anchor = subset != null && !subset.isEmpty();
    LayeredGraphLayout.layoutPositioned(
        transforms, edges, pipelineMeta.getNotes(), options, anchor);
  }
}
