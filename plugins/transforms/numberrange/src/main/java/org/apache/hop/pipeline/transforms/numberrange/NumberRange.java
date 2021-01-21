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

package org.apache.hop.pipeline.transforms.numberrange;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Business logic for the NumberRange
 *
 */
public class NumberRange extends BaseTransform<NumberRangeMeta, NumberRangeData> implements ITransform<NumberRangeMeta, NumberRangeData> {
  private static final Class<?> PKG = NumberRangeMeta.class; // For Translator

  private NumberRangeSet numberRange;

  public NumberRange( TransformMeta transformMeta, NumberRangeMeta meta, NumberRangeData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  /**
   * Column number where the input value is stored
   */

  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if ( row == null ) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      numberRange = new NumberRangeSet( meta.getRules(), meta.getFallBackValue() );
      data.outputRowMeta = getInputRowMeta().clone();
      // Prepare output fields
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Find column numbers
      data.inputColumnNr = data.outputRowMeta.indexOfValue( meta.getInputField() );

      // Check if a field was not available
      if ( data.inputColumnNr < 0 ) {
        logError( "Field for input could not be found: " + meta.getInputField() );
        return false;
      }
    }
    try {
      // get field value
      Double value = getInputRowMeta().getNumber( row, data.inputColumnNr );

      // return range
      String ranges = numberRange.evaluate( value );
      // add value to output
      row = RowDataUtil.addRowData( row, getInputRowMeta().size(), new Object[] { ranges } );
      putRow( data.outputRowMeta, row );
      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "NumberRange.Log.LineNumber" ) + getLinesRead() );
        }
      }
    } catch ( HopException e ) {
      boolean sendToErrorRow = false;
      String errorMessage = null;

      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "NumberRange.Log.ErrorInTransformRunning" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), row, 1, errorMessage, null, "NumberRange001" );
      }
    }

    return true;
  }

  public boolean init() {
    return super.init();
  }

  public void dispose() {
    super.dispose();
  }

}
