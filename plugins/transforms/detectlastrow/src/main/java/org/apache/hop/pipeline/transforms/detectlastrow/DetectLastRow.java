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

package org.apache.hop.pipeline.transforms.detectlastrow;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Detect last row in a stream
 *
 * @author Samatar
 * @since 03June2008
 */
public class DetectLastRow extends BaseTransform<DetectLastRowMeta, DetectLastRowData> implements ITransform<DetectLastRowMeta, DetectLastRowData> {

  private static final Class<?> PKG = DetectLastRowMeta.class; // For Translator

  private Object[] previousRow;

  public DetectLastRow( TransformMeta transformMeta, DetectLastRowMeta meta, DetectLastRowData data, int copyNr, PipelineMeta pipelineMeta,
                        Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // Get row from input rowset & set row busy!

    if ( first ) {
      if ( getInputRowMeta() == null ) {
        setOutputDone();
        return false;
      }

      // get the RowMeta
      data.previousRowMeta = getInputRowMeta().clone();
      data.NrPrevFields = data.previousRowMeta.size();
      data.outputRowMeta = data.previousRowMeta;
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
    }
    Object[] outputRow = null;

    if ( r == null ) { // no more input to be expected...

      if ( previousRow != null ) {
        //
        // Output the last row with last row indicator set to true.
        //
        if ( !Utils.isEmpty( meta.getResultFieldName() ) ) {
          outputRow = RowDataUtil.addRowData( previousRow, getInputRowMeta().size(), data.getTrueArray() );
        } else {
          outputRow = previousRow;
        }

        putRow( data.outputRowMeta, outputRow ); // copy row to output rowset(s);

        if ( log.isRowLevel() ) {
          logRowlevel( BaseMessages.getString( PKG, "DetectLastRow.Log.WroteRowToNextTransform" )
            + data.outputRowMeta.getString( outputRow ) );
        }

        if ( checkFeedback( getLinesRead() ) ) {
          logBasic( BaseMessages.getString( PKG, "DetectLastRow.Log.LineNumber" ) + getLinesRead() );
        }
      }

      setOutputDone();
      return false;
    }

    if ( !first ) {
      outputRow = RowDataUtil.addRowData( previousRow, getInputRowMeta().size(), data.getFalseArray() );
      putRow( data.outputRowMeta, outputRow ); // copy row to output rowset(s);

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "DetectLastRow.Log.WroteRowToNextTransform" )
          + data.outputRowMeta.getString( outputRow ) );
      }

      if ( checkFeedback( getLinesRead() ) ) {
        logBasic( BaseMessages.getString( PKG, "DetectLastRow.Log.LineNumber" ) + getLinesRead() );
      }
    }
    // keep track of the current row
    previousRow = r;
    if ( first ) {
      first = false;
    }

    return true;
  }

  public boolean init() {

    if ( super.init() ) {
      if ( Utils.isEmpty( meta.getResultFieldName() ) ) {
        logError( BaseMessages.getString( PKG, "DetectLastRow.Error.ResultFieldMissing" ) );
        return false;
      }

      return true;
    }
    return false;
  }

}
