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

package org.apache.hop.beam.transform;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Adds n * 2; a multiple of five is an error, sent to the error hop with its input fields. */
public class ErrorRoutingTransform
    extends BaseTransform<ErrorRoutingTransformMeta, ErrorRoutingTransformData> {

  public ErrorRoutingTransform(
      TransformMeta transformMeta,
      ErrorRoutingTransformMeta meta,
      ErrorRoutingTransformData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      setOutputDone();
      return false;
    }
    if (first) {
      first = false;
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    }
    Long n = getInputRowMeta().getInteger(row, 0);
    if (n != null && n % 5 == 0) {
      putError(getInputRowMeta(), row, 1, "multiple of five: " + n, "n", "FIVE");
      return true;
    }
    Object[] out = RowDataUtil.resizeArray(row, data.outputRowMeta.size());
    out[getInputRowMeta().size()] = n == null ? null : n * 2;
    putRow(data.outputRowMeta, out);
    return true;
  }
}
