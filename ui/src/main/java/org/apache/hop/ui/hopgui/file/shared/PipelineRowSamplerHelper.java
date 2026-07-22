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

package org.apache.hop.ui.hopgui.file.shared;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.IMeasuringLocalPipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration.SampleType;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.RowToAdapter;

/**
 * Helper to attach row listeners to a pipeline so output rows are sampled into a target map (e.g.
 * for execution preview or drill-down). Uses the pipeline's run configuration (sample type and size
 * in GUI).
 *
 * <p>Supports both transform-level samples ({@code IRowListener} / {@code putRow}) and hop-level
 * samples ({@code IRowToListener} / {@code putRowTo}) for target hops such as Filter and
 * Switch/Case.
 */
public final class PipelineRowSamplerHelper {

  private static final String HOP_KEY_SEPARATOR = "\t";

  private PipelineRowSamplerHelper() {}

  /**
   * Build a stable map key for a hop from origin transform name to destination transform name.
   *
   * @param originTransformName source transform name
   * @param destinationTransformName target transform name
   * @return hop key used in hop sample maps
   */
  public static String hopKey(String originTransformName, String destinationTransformName) {
    return Const.NVL(originTransformName, "")
        + HOP_KEY_SEPARATOR
        + Const.NVL(destinationTransformName, "");
  }

  /**
   * Build a hop key from a rowset's origin/destination names.
   *
   * @param rowSet the destination rowset
   * @return hop key, or null if rowSet is null
   */
  public static String hopKey(IRowSet rowSet) {
    if (rowSet == null) {
      return null;
    }
    return hopKey(rowSet.getOriginTransformName(), rowSet.getDestinationTransformName());
  }

  /**
   * Attach row listeners to a pipeline so output rows are sampled into {@code targetMap} (transform
   * name → RowBuffer). Uses the pipeline's run configuration (sample type and size in GUI). Does
   * nothing if not a local pipeline, run config is missing, or sample type is None/empty.
   *
   * @param pipeline the pipeline engine (must be {@link LocalPipelineEngine})
   * @param targetMap map to fill; transform name → RowBuffer (last N rows per transform)
   */
  public static void addRowSamplersToPipeline(
      IPipelineEngine<PipelineMeta> pipeline, Map<String, RowBuffer> targetMap) {
    addRowSamplersToPipeline(pipeline, targetMap, null);
  }

  /**
   * Attach transform-level and optional hop-level row samplers.
   *
   * @param pipeline the pipeline engine
   * @param targetMap transform name → RowBuffer (for {@code putRow} / {@code IRowListener})
   * @param hopTargetMap hop key → RowBuffer (for {@code putRowTo} / {@code IRowToListener}); may be
   *     null to skip hop sampling
   */
  public static void addRowSamplersToPipeline(
      IPipelineEngine<PipelineMeta> pipeline,
      Map<String, RowBuffer> targetMap,
      Map<String, RowBuffer> hopTargetMap) {
    if (pipeline == null || targetMap == null) {
      return;
    }

    if (pipeline.getPipelineRunConfiguration() == null
        || !(pipeline.getPipelineRunConfiguration().getEngineRunConfiguration()
            instanceof IMeasuringLocalPipelineRunConfiguration lprConfig)) {
      return;
    }
    if (StringUtils.isEmpty(lprConfig.getSampleTypeInGui())) {
      return;
    }
    SampleType sampleType;
    try {
      sampleType = SampleType.valueOf(lprConfig.getSampleTypeInGui());
    } catch (Exception e) {
      return;
    }
    if (sampleType == SampleType.None) {
      return;
    }
    final int sampleSize = Const.toInt(pipeline.resolve(lprConfig.getSampleSize()), 100);
    if (sampleSize <= 0) {
      return;
    }
    PipelineMeta meta = pipeline.getPipelineMeta();
    if (meta == null) {
      return;
    }
    final Random random = new Random();
    // Per-key counters for Random sampling (transform name or hop key)
    final Map<String, AtomicInteger> rowCounters = new ConcurrentHashMap<>();
    for (final String transformName : meta.getTransformNames()) {
      IEngineComponent component = pipeline.findComponent(transformName, 0);
      if (component == null) {
        continue;
      }

      // Transform-level samples (main putRow path)
      component.addRowListener(
          new RowAdapter() {
            @Override
            public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                throws HopTransformException {
              applySample(
                  targetMap,
                  transformName,
                  rowMeta,
                  row,
                  sampleType,
                  sampleSize,
                  random,
                  rowCounters);
            }
          });

      // Hop-level samples (putRowTo / target hops)
      if (hopTargetMap != null) {
        component.addRowToListener(
            new RowToAdapter() {
              @Override
              public void rowWrittenTo(IRowMeta rowMeta, Object[] row, IRowSet rowSet)
                  throws HopTransformException {
                String key = hopKey(rowSet);
                if (StringUtils.isEmpty(key) || key.equals(HOP_KEY_SEPARATOR)) {
                  return;
                }
                applySample(
                    hopTargetMap, key, rowMeta, row, sampleType, sampleSize, random, rowCounters);
              }
            });
      }
    }
  }

  /** Apply First/Last/Random sampling into the map. */
  private static void applySample(
      Map<String, RowBuffer> targetMap,
      String key,
      IRowMeta rowMeta,
      Object[] row,
      SampleType sampleType,
      int sampleSize,
      Random random,
      Map<String, AtomicInteger> rowCounters)
      throws HopTransformException {
    RowBuffer rowBuffer = targetMap.get(key);
    if (rowBuffer == null) {
      rowBuffer = new RowBuffer(rowMeta);
      if (sampleType == SampleType.Last) {
        rowBuffer.setBuffer(Collections.synchronizedList(new LinkedList<>()));
      } else {
        rowBuffer.setBuffer(Collections.synchronizedList(new ArrayList<>()));
      }
      targetMap.put(key, rowBuffer);
    }
    try {
      row = rowMeta.cloneRow(row);
    } catch (HopValueException e) {
      throw new HopTransformException("Error copying row for preview", e);
    }
    switch (sampleType) {
      case First:
        if (rowBuffer.size() < sampleSize) {
          rowBuffer.addRow(row);
        }
        break;
      case Last:
        rowBuffer.addRow(0, row);
        if (rowBuffer.size() > sampleSize) {
          rowBuffer.removeRow(rowBuffer.size() - 1);
        }
        break;
      case Random:
        int nrRows = rowCounters.computeIfAbsent(key, k -> new AtomicInteger(0)).incrementAndGet();
        if (rowBuffer.size() < sampleSize) {
          rowBuffer.addRow(row);
        } else {
          int randomIndex = random.nextInt(nrRows);
          if (randomIndex < sampleSize) {
            rowBuffer.setRow(randomIndex, row);
          }
        }
        break;
      default:
        break;
    }
  }
}
