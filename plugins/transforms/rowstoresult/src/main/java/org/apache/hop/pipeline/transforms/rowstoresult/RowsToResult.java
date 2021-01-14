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

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Writes results to a next pipeline in a Job
 *
 * @author Matt
 * @since 2-jun-2003
 */
public class RowsToResult extends BaseTransform<RowsToResultMeta, RowsToResultData> implements ITransform<RowsToResultMeta, RowsToResultData> {
  private static final Class<?> PKG = RowsToResult.class; // For Translator

  public RowsToResult( TransformMeta transformMeta, RowsToResultMeta meta, RowsToResultData data, int copyNr, PipelineMeta pipelineMeta,
                       Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) { // no more input to be expected...


      ((Pipeline)getPipeline()).getResultRows().addAll( data.rows );

      setOutputDone();
      return false;
    }

    // Add all rows to rows buffer...
    data.rows.add( new RowMetaAndData( getInputRowMeta(), r ) );
    data.outputRowMeta = getInputRowMeta().clone();
    meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
    putRow( data.outputRowMeta, r ); // copy row to possible alternate
    // rowset(s).

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "RowsToResult.Log.LineNumber" ) + getLinesRead() );
    }

    return true;
  }

  public boolean init() {

    if ( super.init() ) {
      // Add init code here.
      return true;
    }
    return false;
  }

}
