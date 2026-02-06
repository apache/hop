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
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration.SampleType;
import org.apache.hop.pipeline.transform.RowAdapter;

/**
 * Helper to attach row listeners to a pipeline so output rows are sampled into a target map (e.g.
 * for execution preview or drill-down). Uses the pipeline's run configuration (sample type and size
 * in GUI).
 */
public final class PipelineRowSamplerHelper {

  private PipelineRowSamplerHelper() {}

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
    if (pipeline == null || targetMap == null || !(pipeline instanceof LocalPipelineEngine)) {
      return;
    }
    if (pipeline.getPipelineRunConfiguration() == null
        || !(pipeline.getPipelineRunConfiguration().getEngineRunConfiguration()
            instanceof LocalPipelineRunConfiguration)) {
      return;
    }
    LocalPipelineRunConfiguration lprConfig =
        (LocalPipelineRunConfiguration)
            pipeline.getPipelineRunConfiguration().getEngineRunConfiguration();
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
    for (final String transformName : meta.getTransformNames()) {
      IEngineComponent component = pipeline.findComponent(transformName, 0);
      if (component != null) {
        component.addRowListener(
            new RowAdapter() {
              int nrRows = 0;

              @Override
              public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                  throws HopTransformException {
                RowBuffer rowBuffer = targetMap.get(transformName);
                if (rowBuffer == null) {
                  rowBuffer = new RowBuffer(rowMeta);
                  if (sampleType == SampleType.Last) {
                    rowBuffer.setBuffer(Collections.synchronizedList(new LinkedList<>()));
                  } else {
                    rowBuffer.setBuffer(Collections.synchronizedList(new ArrayList<>()));
                  }
                  targetMap.put(transformName, rowBuffer);
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
                    nrRows++;
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
            });
      }
    }
  }
}
