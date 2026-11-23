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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Counts its preparations and records, per partition, whether it was flagged as prepared. */
public class PrepareCountingTransform
    extends BaseTransform<PrepareCountingTransformMeta, PrepareCountingTransformData> {

  public static final AtomicInteger PREPARED = new AtomicInteger();
  public static final List<Boolean> FLAGGED = new CopyOnWriteArrayList<>();

  public static void reset() {
    PREPARED.set(0);
    FLAGGED.clear();
  }

  public PrepareCountingTransform(
      TransformMeta transformMeta,
      PrepareCountingTransformMeta meta,
      PrepareCountingTransformData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public void prepareOnce() throws HopException {
    PREPARED.incrementAndGet();
  }

  @Override
  public boolean processRow() throws HopException {
    if (first) {
      first = false;
      FLAGGED.add(isPreparedOnce());
    }
    Object[] row = getRow();
    if (row == null) {
      setOutputDone();
      return false;
    }
    putRow(getInputRowMeta(), row);
    return true;
  }
}
