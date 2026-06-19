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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.LineageFieldSchema;
import org.apache.hop.lineage.model.TransformSchemaDirection;
import org.apache.hop.lineage.model.TransformSchemaLineagePayload;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;

/**
 * Emits {@link LineageEventKind#TRANSFORM_SCHEMA} with observed input/output row shapes. Called
 * after a transform finishes so {@link BaseTransform#getInputRowMeta()} and output {@link IRowSet}
 * metadata are typically populated.
 *
 * <p>Limitations: multi-input transforms may only expose a single merged input row meta; transforms
 * that never read or write rows may omit INPUT and/or OUTPUT events.
 *
 * <p>When runtime metadata is missing, fields are taken from the pipeline graph:
 *
 * <ul>
 *   <li>Output: {@link PipelineMeta#getTransformFields} when no {@code putRow} occurred (e.g.
 *       {@code DetectEmptyStream} with a non-empty stream).
 *   <li>Input: {@link PipelineMeta#getPrevTransformFields} when no row was ever read (e.g. {@code
 *       Abort} when the first {@code getRow()} returns null, so {@link
 *       BaseTransform#getInputRowMeta()} stays unset).
 * </ul>
 *
 * Those events include context attribute {@code transformSchemaSource=DESIGN_GRAPH}.
 */
public final class LineageTransformSchemaEmitter {

  /**
   * Context attribute: input or output fields were derived from the pipeline graph, not from
   * observed rowsets.
   */
  public static final String ATTR_TRANSFORM_SCHEMA_SOURCE = "transformSchemaSource";

  public static final String SCHEMA_SOURCE_DESIGN_GRAPH = "DESIGN_GRAPH";

  private LineageTransformSchemaEmitter() {}

  public static void emitTransformBoundarySchemas(TransformMetaDataCombi combi) {
    if (combi == null || combi.transform == null) {
      return;
    }
    LineageContext ctxRuntime = buildRuntimeTransformSchemaContext(combi);
    if (ctxRuntime == null) {
      return;
    }
    ITransform transform = combi.transform;

    List<LineageFieldSchema> inputFields = fieldsFromRowMeta(resolveInputRowMetaRuntime(transform));
    boolean designInput = false;
    if (inputFields.isEmpty()) {
      IRowMeta design = resolveInputRowMetaFromGraph(transform, combi.transformMeta);
      inputFields = fieldsFromRowMeta(design);
      designInput = !inputFields.isEmpty();
    }
    if (!inputFields.isEmpty()) {
      LineageContext inCtx =
          designInput ? buildDesignGraphTransformSchemaContext(combi) : ctxRuntime;
      if (inCtx == null) {
        inCtx = ctxRuntime;
      }
      LineageHub.getInstance()
          .emit(
              LineageEvent.of(
                  LineageEventKind.TRANSFORM_SCHEMA,
                  inCtx,
                  new TransformSchemaLineagePayload(TransformSchemaDirection.INPUT, inputFields)));
    }

    IRowMeta outRuntime = resolveOutputRowMetaRuntime(transform);
    List<LineageFieldSchema> outputFields = fieldsFromRowMeta(outRuntime);
    boolean designOutput = false;
    if (outputFields.isEmpty()) {
      IRowMeta design = resolveOutputRowMetaFromGraph(transform, combi.transformMeta);
      outputFields = fieldsFromRowMeta(design);
      designOutput = !outputFields.isEmpty();
    }
    if (!outputFields.isEmpty()) {
      LineageContext outCtx =
          designOutput ? buildDesignGraphTransformSchemaContext(combi) : ctxRuntime;
      if (outCtx == null) {
        outCtx = ctxRuntime;
      }
      LineageHub.getInstance()
          .emit(
              LineageEvent.of(
                  LineageEventKind.TRANSFORM_SCHEMA,
                  outCtx,
                  new TransformSchemaLineagePayload(
                      TransformSchemaDirection.OUTPUT, outputFields)));
    }
  }

  private static LineageContext buildRuntimeTransformSchemaContext(TransformMetaDataCombi combi) {
    LineageContext.Builder ctxBuilder = LineageRunLifecycleEmitter.transformContextBuilder(combi);
    if (ctxBuilder == null) {
      return null;
    }
    String pluginId = combi.transform.getTransformPluginId();
    if (!Utils.isEmpty(pluginId)) {
      ctxBuilder.putAttribute("transformPluginId", pluginId);
    }
    return ctxBuilder.build();
  }

  private static LineageContext buildDesignGraphTransformSchemaContext(
      TransformMetaDataCombi combi) {
    LineageContext.Builder b = LineageRunLifecycleEmitter.transformContextBuilder(combi);
    if (b == null) {
      return null;
    }
    String pluginId = combi.transform.getTransformPluginId();
    if (!Utils.isEmpty(pluginId)) {
      b.putAttribute("transformPluginId", pluginId);
    }
    b.putAttribute(ATTR_TRANSFORM_SCHEMA_SOURCE, SCHEMA_SOURCE_DESIGN_GRAPH);
    return b.build();
  }

  /** Converts a row layout to neutral field schema records (empty if {@code rowMeta} is null). */
  public static List<LineageFieldSchema> fieldsFromRowMeta(IRowMeta rowMeta) {
    if (rowMeta == null || rowMeta.isEmpty()) {
      return List.of();
    }
    List<LineageFieldSchema> fields = new ArrayList<>(rowMeta.size());
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta vm = rowMeta.getValueMeta(i);
      String typeName = ValueMetaFactory.getValueMetaName(vm.getType());
      fields.add(new LineageFieldSchema(vm.getName(), typeName, vm.getLength(), vm.getPrecision()));
    }
    return List.copyOf(fields);
  }

  private static IRowMeta resolveInputRowMetaRuntime(ITransform transform) {
    if (transform instanceof BaseTransform base) {
      IRowMeta meta = base.getInputRowMeta();
      if (meta != null && !meta.isEmpty()) {
        return meta;
      }
    }
    for (IRowSet rowSet : transform.getInputRowSets()) {
      if (rowSet != null && rowSet.getRowMeta() != null && !rowSet.getRowMeta().isEmpty()) {
        return rowSet.getRowMeta();
      }
    }
    return null;
  }

  /**
   * Fields entering this transform from upstream hops (when no input row was ever read, so runtime
   * row meta was never set).
   */
  private static IRowMeta resolveInputRowMetaFromGraph(
      ITransform transform, TransformMeta transformMeta) {
    if (transformMeta == null) {
      return null;
    }
    IPipelineEngine<PipelineMeta> pipeline = transform.getPipeline();
    if (pipeline == null) {
      return null;
    }
    PipelineMeta pipelineMeta = pipeline.getPipelineMeta();
    if (pipelineMeta == null) {
      return null;
    }
    try {
      return pipelineMeta.getPrevTransformFields(pipeline, transformMeta);
    } catch (HopException ignored) {
      return null;
    }
  }

  private static IRowMeta resolveOutputRowMetaRuntime(ITransform transform) {
    for (IRowSet rowSet : transform.getOutputRowSets()) {
      if (rowSet != null && rowSet.getRowMeta() != null && !rowSet.getRowMeta().isEmpty()) {
        return rowSet.getRowMeta();
      }
    }
    return null;
  }

  /**
   * Declared output fields for this transform (from the pipeline graph), when runtime rowsets never
   * received a {@code putRow} (so they have no row meta).
   */
  private static IRowMeta resolveOutputRowMetaFromGraph(
      ITransform transform, TransformMeta transformMeta) {
    if (transformMeta == null) {
      return null;
    }
    IPipelineEngine<PipelineMeta> pipeline = transform.getPipeline();
    if (pipeline == null) {
      return null;
    }
    PipelineMeta pipelineMeta = pipeline.getPipelineMeta();
    if (pipelineMeta == null) {
      return null;
    }
    try {
      return pipelineMeta.getTransformFields(pipeline, transformMeta);
    } catch (HopException ignored) {
      return null;
    }
  }
}
