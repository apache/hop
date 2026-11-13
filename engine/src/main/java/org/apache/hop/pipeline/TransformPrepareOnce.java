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
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;

/**
 * The once-per-run preparation of transforms (truncating a table, creating or changing it, ...)
 * that would otherwise run in every copy, Spark partition or Beam bundle, racing each other.
 *
 * <p>The local engine calls {@link #prepare(Pipeline)} after every copy is initialized. The native
 * Spark and Beam engines call {@link #prepareOnDriver} before the job starts and hand the names of
 * the prepared transforms to their partitions in {@link
 * Const#INTERNAL_VARIABLE_PIPELINE_PREPARED_ONCE_TRANSFORMS}; the per-partition pipelines then only
 * mark those transforms as prepared.
 */
public final class TransformPrepareOnce {

  private TransformPrepareOnce() {}

  /**
   * Prepare the transforms of an initialized pipeline that run in several copies once each, on copy
   * 0, and flag every copy. A transform in a single copy keeps doing that work on its first row. A
   * pipeline that doesn't prepare itself (a per-partition pipeline of a distributed engine) only
   * flags the transforms its engine prepared. A transactional pipeline (a connection group) leaves
   * the work to the transforms' first row, inside the transaction.
   */
  public static void prepare(Pipeline pipeline) throws HopException {
    List<TransformMetaDataCombi> combis = pipeline.getTransforms();
    if (combis == null) {
      return;
    }
    if (!pipeline.isPreparingTransformsOnce()) {
      Set<String> prepared =
          preparedOnDriver(
              pipeline.getVariable(Const.INTERNAL_VARIABLE_PIPELINE_PREPARED_ONCE_TRANSFORMS));
      for (TransformMetaDataCombi combi : combis) {
        if (prepared.contains(combi.transformName)) {
          combi.transform.setPreparedOnce(true);
        }
      }
      return;
    }
    if (pipeline.getExtensionDataMap().get(Const.CONNECTION_GROUP) != null) {
      return;
    }
    Map<String, List<TransformMetaDataCombi>> copies = new LinkedHashMap<>();
    for (TransformMetaDataCombi combi : combis) {
      copies.computeIfAbsent(combi.transformName, n -> new ArrayList<>()).add(combi);
    }
    for (List<TransformMetaDataCombi> transformCopies : copies.values()) {
      TransformMetaDataCombi first = transformCopies.get(0);
      for (TransformMetaDataCombi combi : transformCopies) {
        if (combi.copy == 0) {
          first = combi;
        }
      }
      IVariables variables = first.transform instanceof IVariables v ? v : pipeline;
      // A single copy races nobody: it does the work on its first row, from the real row layout,
      // exactly as before
      if (transformCopies.size() < 2 || !first.meta.needsPrepareOnce(variables)) {
        continue;
      }
      prepare(first.transform, pipeline.getPipelineMeta(), first.transformMeta, variables);
      for (TransformMetaDataCombi combi : transformCopies) {
        combi.transform.setPreparedOnce(true);
      }
    }
  }

  /**
   * On the driver of a distributed engine: prepare every transform of {@code transforms} that needs
   * it, each in a pipeline of its own that only initializes that transform.
   *
   * @return the names of the prepared transforms, to hand to the partitions with {@link
   *     #preparedOnDriverValue(Collection)}
   */
  public static List<String> prepareOnDriver(
      PipelineMeta pipelineMeta,
      Collection<TransformMeta> transforms,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      ILoggingObject parent)
      throws HopException {
    List<String> prepared = new ArrayList<>();
    for (TransformMeta transformMeta : transforms) {
      if (transformMeta.getTransform() == null
          || !transformMeta.getTransform().needsPrepareOnce(variables)) {
        continue;
      }
      TransformMeta copy = (TransformMeta) transformMeta.clone();
      copy.setCopiesString("1");
      PipelineMeta single = new PipelineMeta();
      single.setName(pipelineMeta.getName());
      single.setFilename(pipelineMeta.getFilename());
      single.setMetadataProvider(metadataProvider);
      single.addTransform(copy);
      LocalPipelineEngine pipeline = new LocalPipelineEngine(single, variables, parent);
      pipeline.setMetadataProvider(metadataProvider);
      pipeline.setPreparingTransformsOnce(false);
      try {
        pipeline.prepareExecution();
        ITransform transform = pipeline.getTransform(copy.getName(), 0);
        prepare(transform, pipelineMeta, transformMeta, variables);
      } finally {
        pipeline.disposeInitializedTransforms();
        // Prepared but never run, so never finished: let go of the VFS namespace it took
        pipeline.cleanup();
      }
      prepared.add(transformMeta.getName());
    }
    return prepared;
  }

  /**
   * {@link #prepareOnDriver(PipelineMeta, Collection, IVariables, IHopMetadataProvider,
   * ILoggingObject)} for the transforms that run: the ones an enabled hop connects (a lone
   * transform in a pipeline without hops runs too).
   */
  public static List<String> prepareOnDriver(
      PipelineMeta pipelineMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      ILoggingObject parent)
      throws HopException {
    Set<TransformMeta> active = new java.util.LinkedHashSet<>();
    for (PipelineHopMeta hop : pipelineMeta.getPipelineHops()) {
      if (hop != null && hop.isEnabled()) {
        if (hop.getFromTransform() != null) {
          active.add(hop.getFromTransform());
        }
        if (hop.getToTransform() != null) {
          active.add(hop.getToTransform());
        }
      }
    }
    if (active.isEmpty() && pipelineMeta.nrTransforms() == 1) {
      active.add(pipelineMeta.getTransform(0));
    }
    return prepareOnDriver(pipelineMeta, active, variables, metadataProvider, parent);
  }

  /** The value of {@link Const#INTERNAL_VARIABLE_PIPELINE_PREPARED_ONCE_TRANSFORMS}. */
  public static String preparedOnDriverValue(Collection<String> transformNames) {
    return String.join("\n", transformNames);
  }

  static Set<String> preparedOnDriver(String value) {
    Set<String> names = new HashSet<>();
    if (StringUtils.isNotEmpty(value)) {
      for (String name : value.split("\n")) {
        if (!name.isEmpty()) {
          names.add(name);
        }
      }
    }
    return names;
  }

  private static void prepare(
      ITransform transform,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IVariables variables)
      throws HopException {
    BaseTransform<?, ?> preset = null;
    if (transform instanceof BaseTransform<?, ?> baseTransform
        && baseTransform.getInputRowMeta() == null) {
      // The rows haven't arrived yet: the layout the previous transforms declare, as the SQL a
      // transform dialog shows is built from
      baseTransform.setInputRowMeta(pipelineMeta.getPrevTransformFields(variables, transformMeta));
      preset = baseTransform;
    }
    try {
      transform.prepareOnce();
    } catch (HopException e) {
      throw new HopException(
          "Unable to prepare transform '" + transformMeta.getName() + "' for this run", e);
    } finally {
      if (preset != null) {
        // Only for the preparation: a transform takes the layout of the rows that really arrive
        // from its first row, and only while it has none. The declared layout can differ (Get rows
        // from result declares its fields for design time only).
        preset.setInputRowMeta(null);
      }
    }
  }
}
