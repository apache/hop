/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.numberrange;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;

/**
 * Business logic for the NumberRange
 *
 * @author ronny.roeller@fredhopper.com
 */
public class NumberRange extends BaseTransform implements TransformInterface {
  private static Class<?> PKG = NumberRangeMeta.class; // for i18n purposes, needed by Translator!!

  private NumberRangeData data;
  private NumberRangeMeta meta;

  private NumberRangeSet numberRange;

  /**
   * Column number where the input value is stored
   */

  public boolean processRow( TransformMetaInterface smi, TransformDataInterface sdi ) throws HopException {
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
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metaStore );

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

  public NumberRange( TransformMeta s, TransformDataInterface transformDataInterface, int c, PipelineMeta t, Pipeline dis ) {
    super( s, transformDataInterface, c, t, dis );
  }

  public boolean init( TransformMetaInterface smi, TransformDataInterface sdi ) {
    meta = (NumberRangeMeta) smi;
    data = (NumberRangeData) sdi;

    return super.init( smi, sdi );
  }

  public void dispose( TransformMetaInterface smi, TransformDataInterface sdi ) {
    meta = (NumberRangeMeta) smi;
    data = (NumberRangeData) sdi;

    super.dispose( smi, sdi );
  }

}
