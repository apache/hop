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

import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
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
 *
 * <p>The listeners run for every row the sampled transform writes, so they do as little as possible
 * per row: each listener resolves its buffer once, and a row is only cloned when it is kept. First
 * stops cloning once the buffer is full, Random clones only the rows that land in the reservoir,
 * and Last keeps a fixed ring of rows instead of a linked list.
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
    for (final String transformName : meta.getTransformNames()) {
      IEngineComponent component = pipeline.findComponent(transformName, 0);
      if (component == null) {
        continue;
      }

      // Transform-level samples (main putRow path)
      component.addRowListener(
          new RowAdapter() {
            private RowSampler sampler;

            @Override
            public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                throws HopTransformException {
              RowSampler s = sampler;
              if (s == null) {
                s = createSampler(sampleType, sampleSize, rowMeta);
                targetMap.put(transformName, s.getRowBuffer());
                sampler = s;
              }
              s.sample(rowMeta, row);
            }
          });

      // Hop-level samples (putRowTo / target hops)
      if (hopTargetMap != null) {
        component.addRowToListener(
            new RowToAdapter() {
              // One sampler per target row set, so the hop key is built once per hop, not per row
              private final Map<IRowSet, RowSampler> samplers = new ConcurrentHashMap<>();

              @Override
              public void rowWrittenTo(IRowMeta rowMeta, Object[] row, IRowSet rowSet)
                  throws HopTransformException {
                if (rowSet == null) {
                  return;
                }
                RowSampler s = samplers.get(rowSet);
                if (s == null) {
                  String key = hopKey(rowSet);
                  if (StringUtils.isEmpty(key) || key.equals(HOP_KEY_SEPARATOR)) {
                    return;
                  }
                  s = createSampler(sampleType, sampleSize, rowMeta);
                  hopTargetMap.put(key, s.getRowBuffer());
                  samplers.put(rowSet, s);
                }
                s.sample(rowMeta, row);
              }
            });
      }
    }
  }

  /**
   * Create the sampler for one transform or hop. Package-private for tests.
   *
   * @param sampleType First, Last or Random
   * @param sampleSize the number of rows to keep
   * @param rowMeta the layout of the sampled rows
   * @return a new sampler with an empty buffer
   */
  static RowSampler createSampler(SampleType sampleType, int sampleSize, IRowMeta rowMeta) {
    return switch (sampleType) {
      case First -> new FirstRowsSampler(rowMeta, sampleSize);
      case Last -> new LastRowsSampler(rowMeta, sampleSize);
      case Random -> new RandomRowsSampler(rowMeta, sampleSize);
      default -> throw new IllegalArgumentException("Unsupported sample type " + sampleType);
    };
  }

  private static Object[] copyRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {
    try {
      return rowMeta.cloneRow(row);
    } catch (HopValueException e) {
      throw new HopTransformException("Error copying row for preview", e);
    }
  }

  /**
   * Keeps a sample of the rows of one transform or hop in a {@link RowBuffer}. Readers lock the
   * buffer's list ({@code synchronized (rowBuffer.getBuffer())}) to get a consistent view, so every
   * change to the list happens under that same lock.
   */
  abstract static class RowSampler {
    protected final RowBuffer rowBuffer;
    protected final int sampleSize;

    RowSampler(RowBuffer rowBuffer, int sampleSize) {
      this.rowBuffer = rowBuffer;
      this.sampleSize = sampleSize;
    }

    RowBuffer getRowBuffer() {
      return rowBuffer;
    }

    abstract void sample(IRowMeta rowMeta, Object[] row) throws HopTransformException;
  }

  /** Keeps the first rows, then ignores the rest without cloning them. */
  static final class FirstRowsSampler extends RowSampler {
    private volatile boolean full;

    FirstRowsSampler(IRowMeta rowMeta, int sampleSize) {
      super(new RowBuffer(rowMeta), sampleSize);
    }

    @Override
    void sample(IRowMeta rowMeta, Object[] row) throws HopTransformException {
      if (full) {
        return;
      }
      List<Object[]> rows = rowBuffer.getBuffer();
      synchronized (rows) {
        if (rows.size() < sampleSize) {
          rows.add(copyRow(rowMeta, row));
        }
        full = rows.size() >= sampleSize;
      }
    }
  }

  /** Keeps the last rows in a fixed ring, newest first. */
  static final class LastRowsSampler extends RowSampler {
    private final LastRowsList rows;

    LastRowsSampler(IRowMeta rowMeta, int sampleSize) {
      super(new RowBuffer(rowMeta), sampleSize);
      rows = new LastRowsList(sampleSize);
      rowBuffer.setBuffer(rows);
    }

    @Override
    void sample(IRowMeta rowMeta, Object[] row) throws HopTransformException {
      rows.addNewest(copyRow(rowMeta, row));
    }
  }

  /**
   * Keeps a uniform random sample (reservoir sampling). Only the rows that enter the reservoir are
   * cloned.
   */
  static final class RandomRowsSampler extends RowSampler {
    private long rowsSeen;

    RandomRowsSampler(IRowMeta rowMeta, int sampleSize) {
      super(new RowBuffer(rowMeta), sampleSize);
    }

    @Override
    void sample(IRowMeta rowMeta, Object[] row) throws HopTransformException {
      List<Object[]> rows = rowBuffer.getBuffer();
      synchronized (rows) {
        long seen = ++rowsSeen;
        if (seen <= sampleSize) {
          rows.add(copyRow(rowMeta, row));
          return;
        }
        long index = ThreadLocalRandom.current().nextLong(seen);
        if (index < sampleSize) {
          rows.set((int) index, copyRow(rowMeta, row));
        }
      }
    }
  }

  /**
   * A fixed-size ring of rows exposed as a list, newest row at index 0. Adding a row once the ring
   * is full replaces the oldest one, without allocating a node or shifting elements. All access
   * locks the list itself, the same lock {@code Collections.synchronizedList} uses, so readers can
   * keep doing {@code synchronized (rowBuffer.getBuffer())}.
   */
  static final class LastRowsList extends AbstractList<Object[]> {
    private final Object[][] ring;
    private int next;
    private int size;

    LastRowsList(int capacity) {
      ring = new Object[capacity][];
    }

    synchronized void addNewest(Object[] row) {
      ring[next] = row;
      next = (next + 1) % ring.length;
      if (size < ring.length) {
        size++;
      }
    }

    @Override
    public synchronized Object[] get(int index) {
      if (index < 0 || index >= size) {
        throw new IndexOutOfBoundsException("Index " + index + ", size " + size);
      }
      return ring[Math.floorMod(next - 1 - index, ring.length)];
    }

    @Override
    public synchronized int size() {
      return size;
    }

    @Override
    public synchronized void clear() {
      Arrays.fill(ring, null);
      next = 0;
      size = 0;
      modCount++;
    }
  }
}
