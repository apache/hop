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

package org.apache.hop.pipeline.debug;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Generates integers, copies rows, or drops even values. One row per processRow. */
public class DebugFixture extends BaseTransform<DebugFixtureMeta, DebugFixtureData> {

  public DebugFixture(
      TransformMeta transformMeta,
      DebugFixtureMeta meta,
      DebugFixtureData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    if (meta.getMode() == DebugFixtureMeta.Mode.GENERATE) {
      return generate();
    }
    Object[] row = getRow();
    if (row == null) {
      setOutputDone();
      return false;
    }
    if (meta.getMode() == DebugFixtureMeta.Mode.DROP_EVEN) {
      long value = ((Number) row[0]).longValue();
      if (value % 2L == 0L) {
        return true;
      }
    }
    putRow(getInputRowMeta(), row);
    return true;
  }

  private boolean generate() throws HopException {
    if (data.generated >= meta.getRowsToGenerate()) {
      setOutputDone();
      return false;
    }
    if (first) {
      first = false;
      data.outputRowMeta = new RowMeta();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, null);
    }
    putRow(data.outputRowMeta, new Object[] {Long.valueOf(data.generated + 1L)});
    data.generated++;
    return true;
  }
}
