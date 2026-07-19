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

package org.apache.hop.spark.core;

import java.io.Serializable;
import java.util.Objects;

/**
 * Snapshot of Hop transform counters for one Spark partition (engine component copy).
 *
 * <p>Counters are absolute values from the mini-pipeline / native tracker on that partition.
 * Merging multiple snapshots for the same key takes the maximum of each counter (progress updates),
 * not a sum. Timing uses earliest start and latest end (epoch millis).
 */
public class SparkTransformMetricSlice implements Serializable {
  private static final long serialVersionUID = 2L;

  private final String transformName;
  private final int copyNr;
  private final String host;
  private final long linesRead;
  private final long linesWritten;
  private final long linesInput;
  private final long linesOutput;
  private final long errors;
  private final boolean running;
  private final boolean finished;

  /** Wall-clock start of this partition's work (epoch ms), or 0 if unknown. */
  private final long startTimeMs;

  /**
   * Wall-clock end of this partition's work (epoch ms). 0 while still running so the GUI can treat
   * duration as "live" until finish.
   */
  private final long endTimeMs;

  public SparkTransformMetricSlice(
      String transformName,
      int copyNr,
      String host,
      long linesRead,
      long linesWritten,
      long linesInput,
      long linesOutput,
      long errors,
      boolean running,
      boolean finished,
      long startTimeMs,
      long endTimeMs) {
    this.transformName = transformName;
    this.copyNr = copyNr;
    this.host = host;
    this.linesRead = linesRead;
    this.linesWritten = linesWritten;
    this.linesInput = linesInput;
    this.linesOutput = linesOutput;
    this.errors = errors;
    this.running = running;
    this.finished = finished;
    this.startTimeMs = startTimeMs;
    this.endTimeMs = endTimeMs;
  }

  /** Map key for merge-by-partition. */
  public String key() {
    return transformName + '\0' + copyNr;
  }

  /**
   * Merge two absolute snapshots for the same partition: max counters; min start; max end when
   * finished; finished wins over running.
   */
  public SparkTransformMetricSlice mergeWith(SparkTransformMetricSlice other) {
    if (other == null) {
      return this;
    }
    boolean mergedFinished = this.finished || other.finished;
    boolean mergedRunning = !mergedFinished && (this.running || other.running);
    String mergedHost = this.host != null ? this.host : other.host;
    long mergedStart = minPositive(this.startTimeMs, other.startTimeMs);
    long mergedEnd;
    if (mergedFinished) {
      mergedEnd = Math.max(this.endTimeMs, other.endTimeMs);
      // If finish was reported without a clock stamp, fall back to the later of known ends/starts
      if (mergedEnd <= 0) {
        mergedEnd = Math.max(this.endTimeMs, other.endTimeMs);
      }
    } else {
      // Still running: keep end unset so UI duration keeps ticking
      mergedEnd = 0L;
    }
    return new SparkTransformMetricSlice(
        this.transformName,
        this.copyNr,
        mergedHost,
        Math.max(this.linesRead, other.linesRead),
        Math.max(this.linesWritten, other.linesWritten),
        Math.max(this.linesInput, other.linesInput),
        Math.max(this.linesOutput, other.linesOutput),
        Math.max(this.errors, other.errors),
        mergedRunning,
        mergedFinished,
        mergedStart,
        mergedEnd);
  }

  private static long minPositive(long a, long b) {
    if (a <= 0) {
      return b;
    }
    if (b <= 0) {
      return a;
    }
    return Math.min(a, b);
  }

  /**
   * Duration in milliseconds for this slice. While running (no end), uses wall clock from start to
   * now. Returns 0 when start is unknown.
   */
  public long durationMs() {
    if (startTimeMs <= 0) {
      return 0L;
    }
    long end = endTimeMs > 0 ? endTimeMs : System.currentTimeMillis();
    return Math.max(0L, end - startTimeMs);
  }

  public String getTransformName() {
    return transformName;
  }

  public int getCopyNr() {
    return copyNr;
  }

  public String getHost() {
    return host;
  }

  public long getLinesRead() {
    return linesRead;
  }

  public long getLinesWritten() {
    return linesWritten;
  }

  public long getLinesInput() {
    return linesInput;
  }

  public long getLinesOutput() {
    return linesOutput;
  }

  public long getErrors() {
    return errors;
  }

  public boolean isRunning() {
    return running;
  }

  public boolean isFinished() {
    return finished;
  }

  public long getStartTimeMs() {
    return startTimeMs;
  }

  public long getEndTimeMs() {
    return endTimeMs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SparkTransformMetricSlice that = (SparkTransformMetricSlice) o;
    return copyNr == that.copyNr
        && linesRead == that.linesRead
        && linesWritten == that.linesWritten
        && linesInput == that.linesInput
        && linesOutput == that.linesOutput
        && errors == that.errors
        && running == that.running
        && finished == that.finished
        && startTimeMs == that.startTimeMs
        && endTimeMs == that.endTimeMs
        && Objects.equals(transformName, that.transformName)
        && Objects.equals(host, that.host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        transformName,
        copyNr,
        host,
        linesRead,
        linesWritten,
        linesInput,
        linesOutput,
        errors,
        running,
        finished,
        startTimeMs,
        endTimeMs);
  }
}
