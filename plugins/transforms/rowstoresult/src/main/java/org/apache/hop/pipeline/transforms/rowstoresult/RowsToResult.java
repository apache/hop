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

package org.apache.hop.pipeline.transforms.rowstoresult;

import java.util.List;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Writes results to a next pipeline in a Job */
public class RowsToResult extends BaseTransform<RowsToResultMeta, RowsToResultData> {

  private static final Class<?> PKG = RowsToResult.class;

  public RowsToResult(
      TransformMeta transformMeta,
      RowsToResultMeta meta,
      RowsToResultData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row, set busy!
    if (r == null) { // no more input to be expected...
      handOverRows();
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
      data.resultRowMeta = data.outputRowMeta.clone();
    }

    // Keep a copy of the row: the transforms after this one get the same array and may change it
    // in place.
    data.rows.add(new RowMetaAndData(data.resultRowMeta, data.resultRowMeta.cloneRow(r)));
    putRow(data.outputRowMeta, r);

    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic(BaseMessages.getString(PKG, "RowsToResult.Log.LineNumber") + getLinesRead());
    }

    return true;
  }

  /**
   * The single threaded executor calls processRow() only for the rows waiting on the input, so it
   * never reaches the end of the input there. Hand over the rows of every batch instead.
   */
  @Override
  public void batchComplete() throws HopException {
    handOverRows();
  }

  /**
   * Adds the buffered rows to the pipeline's result rows and empties the buffer. Several copies, or
   * several Copy rows to result transforms, add to the same list, so the list is locked.
   */
  private void handOverRows() {
    if (data.rows.isEmpty()) {
      return;
    }
    List<RowMetaAndData> resultRows = ((Pipeline) getPipeline()).getResultRows();
    synchronized (resultRows) {
      resultRows.addAll(data.rows);
    }
    data.rows.clear();
  }

  @Override
  public boolean init() {

    if (super.init()) {
      // Add init code here.
      return true;
    }
    return false;
  }
}
