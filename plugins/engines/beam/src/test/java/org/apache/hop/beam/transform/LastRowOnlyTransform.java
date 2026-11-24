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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Holds back every row and writes the last one when the input ends. Notes, over all its instances
 * in the JVM, how many rows each instance had read when it saw the end of its input.
 */
public class LastRowOnlyTransform
    extends BaseTransform<LastRowOnlyTransformMeta, LastRowOnlyTransformData> {

  /** For every end of input seen: the number of rows read before it. */
  static final List<Integer> ROWS_AT_END = new CopyOnWriteArrayList<>();

  public LastRowOnlyTransform(
      TransformMeta transformMeta,
      LastRowOnlyTransformMeta meta,
      LastRowOnlyTransformData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      ROWS_AT_END.add(data.rows);
      if (data.lastRow != null) {
        putRow(data.rowMeta, data.lastRow);
      }
      setOutputDone();
      return false;
    }
    data.rowMeta = getInputRowMeta();
    data.lastRow = row;
    data.rows++;
    return true;
  }
}
