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

package org.apache.hop.pipeline.transforms.delay;

import java.util.Locale;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Delay input row. */
public class Delay extends BaseTransform<DelayMeta, DelayData> {

  private static final Class<?> PKG = DelayMeta.class;

  public Delay(
      TransformMeta transformMeta,
      DelayMeta meta,
      DelayData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row, set busy!

    if (r == null) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    IRowMeta rowMeta = getInputRowMeta();

    if (first) {
      first = false;
      initializeMeta(meta, rowMeta);
    }

    data.timeout = resolveTimeout(meta, r);
    data.multiple = resolveScaleMultiple(meta, r);

    if (data.timeout <= 0 || data.multiple <= 0) {
      putRow(rowMeta, r);
      if (checkFeedback(getLinesRead()) && isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "Delay.Log.LineNumber", "" + getLinesRead()));
      }
      return true;
    }

    long delayMillis;
    try {
      delayMillis = Math.multiplyExact(data.timeout, data.multiple);
    } catch (ArithmeticException e) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "Delay.Log.DelayOverflow",
              Long.toString(data.timeout),
              Long.toString(data.multiple)),
          e);
    }

    if (isDebug()) {
      logDebug(
          BaseMessages.getString(
              PKG,
              "Delay.Log.TimeOutWithScale",
              String.valueOf(data.timeout),
              getScaleLabel(data.multiple),
              String.valueOf(delayMillis)));
    }

    applyDelay(delayMillis);

    if (isDebug()) {
      logDebug(BaseMessages.getString(PKG, "Delay.WaitTimeIsElapsed.Label"));
    }

    putRow(rowMeta, r);

    if (checkFeedback(getLinesRead()) && isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "Delay.Log.LineNumber", "" + getLinesRead()));
    }

    return true;
  }

  private void initializeMeta(DelayMeta meta, IRowMeta rowMeta) throws HopException {
    data.staticTimeout = Const.toLong(resolve(meta.getTimeout()), 0L);
    data.staticScaleTimeCode = meta.getScaleTimeCode();
    data.multiple = determineMultiple(data.staticScaleTimeCode);
    if (!Utils.isEmpty(meta.getTimeoutField())) {
      data.timeoutFieldIndex = rowMeta.indexOfValue(meta.getTimeoutField());
      if (data.timeoutFieldIndex < 0) {
        throw new HopException(
            BaseMessages.getString(PKG, "Delay.Log.TimeoutFieldNotFound", meta.getTimeoutField()));
      }
      data.timeoutValueMeta = rowMeta.getValueMeta(data.timeoutFieldIndex);
      if (!data.timeoutValueMeta.isNumeric()
          && data.timeoutValueMeta.getType() != IValueMeta.TYPE_STRING) {
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "Delay.Log.TimeoutFieldType",
                meta.getTimeoutField(),
                data.timeoutValueMeta.getTypeDesc()));
      }
    } else {
      data.timeoutFieldIndex = -1;
      data.timeoutValueMeta = null;
    }

    if (meta.isScaleTimeFromField()) {
      if (Utils.isEmpty(meta.getScaleTimeField())) {
        throw new HopException(BaseMessages.getString(PKG, "Delay.Log.ScaleTimeFieldMissing"));
      }
      data.scaleTimeFieldIndex = rowMeta.indexOfValue(meta.getScaleTimeField());
      if (data.scaleTimeFieldIndex < 0) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "Delay.Log.ScaleTimeFieldNotFound", meta.getScaleTimeField()));
      }
      data.scaleTimeValueMeta = rowMeta.getValueMeta(data.scaleTimeFieldIndex);
      if (data.scaleTimeValueMeta.getType() != IValueMeta.TYPE_STRING) {
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "Delay.Log.ScaleTimeFieldType",
                meta.getScaleTimeField(),
                data.scaleTimeValueMeta.getTypeDesc()));
      }
    } else {
      data.scaleTimeFieldIndex = -1;
      data.scaleTimeValueMeta = null;
    }
  }

  private long resolveTimeout(DelayMeta meta, Object[] row) throws HopException {
    if (data.timeoutFieldIndex < 0) {
      return Math.max(0L, data.staticTimeout);
    }

    Object timeoutValue = row[data.timeoutFieldIndex];
    if (data.timeoutValueMeta.isNull(timeoutValue)) {
      return 0L;
    }

    try {
      Double number = data.timeoutValueMeta.getNumber(timeoutValue);
      if (number == null) {
        String str = data.timeoutValueMeta.getString(timeoutValue);
        if (Utils.isEmpty(str)) {
          return 0L;
        }
        number = Double.valueOf(str);
      }
      long timeout = Math.round(number);
      if (timeout < 0) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "Delay.Log.TimeoutFieldNegative", meta.getTimeoutField(), timeout));
      }
      return timeout;
    } catch (NumberFormatException | HopValueException e) {
      throw new HopException(
          BaseMessages.getString(PKG, "Delay.Log.TimeoutFieldNotNumeric", meta.getTimeoutField()),
          e);
    }
  }

  private long resolveScaleMultiple(DelayMeta meta, Object[] row) throws HopException {
    int scaleCode = data.staticScaleTimeCode;

    if (data.scaleTimeFieldIndex >= 0) {
      scaleCode = 1; // default to seconds when using a field
      Object scaleValue = row[data.scaleTimeFieldIndex];
      if (!data.scaleTimeValueMeta.isNull(scaleValue)) {
        String value = data.scaleTimeValueMeta.getString(scaleValue);
        if (!Utils.isEmpty(value)) {
          Integer parsedCode = parseScaleTimeCode(value);
          if (parsedCode == null) {
            throw new HopException(
                BaseMessages.getString(
                    PKG, "Delay.Log.ScaleTimeFieldInvalid", meta.getScaleTimeField(), value));
          }
          scaleCode = parsedCode;
        }
      }
    }

    return determineMultiple(scaleCode);
  }

  private long determineMultiple(int scaleCode) {
    switch (scaleCode) {
      case 0:
        return 1L;
      case 1:
        return 1000L;
      case 2:
        return 60000L;
      case 3:
        return 3600000L;
      default:
        return 1000L;
    }
  }

  private String getScaleLabel(long multiple) {
    if (multiple == 1L) {
      return BaseMessages.getString(PKG, "DelayDialog.MSScaleTime.Label");
    }
    if (multiple == 1000L) {
      return BaseMessages.getString(PKG, "DelayDialog.SScaleTime.Label");
    }
    if (multiple == 60000L) {
      return BaseMessages.getString(PKG, "DelayDialog.MnScaleTime.Label");
    }
    if (multiple == 3600000L) {
      return BaseMessages.getString(PKG, "DelayDialog.HrScaleTime.Label");
    }
    return BaseMessages.getString(PKG, "Delay.Log.UnknownScale");
  }

  private Integer parseScaleTimeCode(String value) {
    String normalized = value.trim().toLowerCase(Locale.ROOT);
    switch (normalized) {
      case "ms", "msec", "millisecond", "milliseconds":
        return 0;
      case "s", "sec", "secs", "second", "seconds":
        return 1;
      case "m", "min", "mins", "minute", "minutes":
        return 2;
      case "h", "hr", "hour", "hours":
        return 3;
      default:
        return null;
    }
  }

  private void applyDelay(long delayMillis) {
    if (delayMillis <= 0) {
      return;
    }

    if (delayMillis < 1000L) {
      try {
        Thread.sleep(delayMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return;
    }

    long endTime = System.currentTimeMillis() + delayMillis;
    while (!isStopped()) {
      long remaining = endTime - System.currentTimeMillis();
      if (remaining <= 0) {
        break;
      }
      long sleepChunk = Math.min(remaining, 1000L);
      try {
        Thread.sleep(sleepChunk);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }
}
