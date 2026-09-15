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

package org.apache.hop.pipeline.transforms.transformsmetrics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Wait for selected sibling transforms to finish, then output one metrics row per watched copy.
 * This transform does not consume incoming rows.
 */
public class TransformsMetrics extends BaseTransform<TransformsMetricsMeta, TransformsMetricsData> {

  private static final Class<?> PKG = TransformsMetricsMeta.class;
  private static final long POLL_SLEEP_MS = 20L;

  public TransformsMetrics(
      TransformMeta transformMeta,
      TransformsMetricsMeta meta,
      TransformsMetricsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    if (first) {
      first = false;
      initializeWatchedTransforms();
    }

    List<IEngineComponent> remaining = data.getRemaining();
    while (!remaining.isEmpty() && !isStopped()) {
      boolean waiting = false;
      Iterator<IEngineComponent> iterator = remaining.iterator();
      while (iterator.hasNext()) {
        IEngineComponent component = iterator.next();
        if (isStillRunning(component)) {
          waiting = true;
        } else {
          iterator.remove();
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "TransformsMetrics.Log.Finished",
                    component.getName(),
                    String.valueOf(component.getCopyNr())));
          }
          emitMetricsRow(component);
        }
      }
      if (waiting && !remaining.isEmpty() && !isStopped()) {
        try {
          TimeUnit.MILLISECONDS.sleep(POLL_SLEEP_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          setStopped(true);
        }
      }
    }

    setOutputDone();
    return false;
  }

  private void initializeWatchedTransforms() throws HopException {
    if (meta.getMetricTransforms() == null || meta.getMetricTransforms().isEmpty()) {
      throw new HopException(BaseMessages.getString(PKG, "TransformsMetrics.Error.NotTransforms"));
    }

    data.setTransformNameField(resolve(meta.getTransformNameField()));
    data.setTransformIdField(resolve(meta.getTransformIdField()));
    data.setLinesInputField(resolve(meta.getLinesInputField()));
    data.setLinesOutputField(resolve(meta.getLinesOutputField()));
    data.setLinesReadField(resolve(meta.getLinesReadField()));
    data.setLinesUpdatedField(resolve(meta.getLinesUpdatedField()));
    data.setLinesWrittenField(resolve(meta.getLinesWrittenField()));
    data.setLinesRejectedField(resolve(meta.getLinesRejectedField()));
    data.setDurationField(resolve(meta.getDurationField()));

    String[] nextTransforms = getPipelineMeta().getNextTransformNames(getTransformMeta());
    List<IEngineComponent> remaining = new ArrayList<>();

    for (MetricTransform metricTransform : meta.getMetricTransforms()) {
      String name = metricTransform.getName();
      if (StringUtils.isEmpty(name)) {
        continue;
      }
      if (name.equals(getTransformName())) {
        throw new HopException(
            BaseMessages.getString(PKG, "TransformsMetrics.Error.CannotWatchSelf", name));
      }
      if (nextTransforms != null) {
        for (String nextTransform : nextTransforms) {
          if (name.equals(nextTransform)) {
            throw new HopException(
                BaseMessages.getString(
                    PKG, "TransformsMetrics.Error.CannotWatchTarget", nextTransform));
          }
        }
      }

      int copyNr = Const.toInt(resolve(metricTransform.getCopyNr()), 0);
      IEngineComponent component = getDispatcher().findComponent(name, copyNr);
      if (component == null) {
        if (metricTransform.isRequired()) {
          throw new HopException(
              BaseMessages.getString(
                  PKG, "TransformsMetrics.Error.TransformNotFound", name, String.valueOf(copyNr)));
        }
        continue;
      }
      remaining.add(component);
    }

    data.setRemaining(remaining);

    data.setOutputRowMeta(new RowMeta());
    meta.getFields(
        data.getOutputRowMeta(), getTransformName(), null, null, this, getMetadataProvider());
  }

  private boolean isStillRunning(IEngineComponent component) {
    ComponentExecutionStatus status = component.getStatus();
    return status == ComponentExecutionStatus.STATUS_RUNNING
        || status == ComponentExecutionStatus.STATUS_IDLE
        || status == ComponentExecutionStatus.STATUS_INIT
        || status == ComponentExecutionStatus.STATUS_PAUSED;
  }

  private void emitMetricsRow(IEngineComponent component) throws HopException {
    Object[] rowData = RowDataUtil.allocateRowData(data.getOutputRowMeta().size());
    incrementLinesRead();

    int index = 0;
    if (StringUtils.isNotBlank(data.getTransformNameField())) {
      rowData[index++] = component.getName();
    }
    if (StringUtils.isNotBlank(data.getTransformIdField())) {
      rowData[index++] = pluginIdOf(component);
    }
    if (StringUtils.isNotBlank(data.getLinesInputField())) {
      rowData[index++] = component.getLinesInput();
    }
    if (StringUtils.isNotBlank(data.getLinesOutputField())) {
      rowData[index++] = component.getLinesOutput();
    }
    if (StringUtils.isNotBlank(data.getLinesReadField())) {
      rowData[index++] = component.getLinesRead();
    }
    if (StringUtils.isNotBlank(data.getLinesUpdatedField())) {
      rowData[index++] = component.getLinesUpdated();
    }
    if (StringUtils.isNotBlank(data.getLinesWrittenField())) {
      rowData[index++] = component.getLinesWritten();
    }
    if (StringUtils.isNotBlank(data.getLinesRejectedField())) {
      rowData[index++] = component.getLinesRejected();
    }
    if (StringUtils.isNotBlank(data.getDurationField())) {
      rowData[index] = component.getExecutionDuration();
    }

    putRow(data.getOutputRowMeta(), rowData);
  }

  private String pluginIdOf(IEngineComponent component) {
    TransformMeta watched = getPipelineMeta().findTransform(component.getName());
    return watched == null ? null : watched.getTransformPluginId();
  }
}
