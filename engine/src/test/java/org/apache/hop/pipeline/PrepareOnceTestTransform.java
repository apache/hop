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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Counts how often it is prepared and records, for every copy, whether it was flagged as prepared
 * and which input fields the preparation saw.
 */
public class PrepareOnceTestTransform
    extends BaseTransform<PrepareOnceTestMeta, PrepareOnceTestData> {

  static final AtomicInteger PREPARED = new AtomicInteger();
  static final List<Boolean> FLAGGED = new CopyOnWriteArrayList<>();
  static volatile IRowMeta preparedWith;

  static void reset() {
    PREPARED.set(0);
    FLAGGED.clear();
    preparedWith = null;
  }

  public PrepareOnceTestTransform(
      TransformMeta transformMeta,
      PrepareOnceTestMeta meta,
      PrepareOnceTestData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public void prepareOnce() throws HopException {
    PREPARED.incrementAndGet();
    preparedWith = getInputRowMeta();
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
