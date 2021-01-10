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

package org.apache.hop.pipeline.transforms.rowsfromresult;

import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;

/**
 * Reads results from a previous pipeline in a Job
 *
 * @author Matt
 * @since 2-jun-2003
 */
public class RowsFromResult extends BaseTransform<RowsFromResultMeta, RowsFromResultData> implements ITransform<RowsFromResultMeta, RowsFromResultData> {
  private static final Class<?> PKG = RowsFromResult.class; // For Translator

  public RowsFromResultData data;

  public RowsFromResult( TransformMeta transformMeta, RowsFromResultMeta meta, RowsFromResultData data, int copyNr, PipelineMeta pipelineMeta,
                         Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {
    Result previousResult = getPipeline().getPreviousResult();
    if ( previousResult == null || getLinesRead() >= previousResult.getRows().size() ) {
      setOutputDone();
      return false;
    }
    RowMetaAndData row = previousResult.getRows().get( (int) getLinesRead() );
    incrementLinesRead();

    // We don't get the meta-data from the previous transforms (there aren't any) but from the previous pipeline or workflow
    //
    data.outputRowMeta = row.getRowMeta();

    // copy row to possible alternate rowset(s).
    //
    putRow( data.outputRowMeta, row.getData() );

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "RowsFromResult.Log.LineNumber" ) + getLinesRead() );
      }
    }

    return true;
  }
}
