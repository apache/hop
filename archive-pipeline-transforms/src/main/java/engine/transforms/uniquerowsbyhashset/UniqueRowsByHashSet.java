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

package org.apache.hop.pipeline.transforms.uniquerowsbyhashset;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;

public class UniqueRowsByHashSet extends BaseTransform implements ITransform {
  private static Class<?> PKG = UniqueRowsByHashSetMeta.class; // for i18n purposes, needed by Translator!!

  private UniqueRowsByHashSetMeta meta;
  private UniqueRowsByHashSetData data;

  public UniqueRowsByHashSet( TransformMeta transformMeta, ITransformData iTransformData, int copyNr,
                              PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, iTransformData, copyNr, pipelineMeta, pipeline );

    meta = (UniqueRowsByHashSetMeta) getTransformMeta().getTransformMetaInterface();
    data = (UniqueRowsByHashSetData) iTransformData; // create new data object.
  }

  private boolean isUniqueRow( Object[] row ) {
    return data.seen.add( new RowKey( row, data ) );
  }

  public boolean processRow( TransformMetaInterface smi, ITransformData sdi ) throws HopException {
    meta = (UniqueRowsByHashSetMeta) smi;
    data = (UniqueRowsByHashSetData) sdi;

    Object[] r = getRow(); // get row!
    if ( r == null ) { // no more input to be expected...

      data.clearHashSet();
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.inputRowMeta = getInputRowMeta().clone();
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metaStore );

      data.storeValues = meta.getStoreValues();

      // ICache lookup of fields
      data.fieldnrs = new int[ meta.getCompareFields().length ];

      for ( int i = 0; i < meta.getCompareFields().length; i++ ) {
        data.fieldnrs[ i ] = getInputRowMeta().indexOfValue( meta.getCompareFields()[ i ] );
        if ( data.fieldnrs[ i ] < 0 ) {
          logError( BaseMessages.getString( PKG, "UniqueRowsByHashSet.Log.CouldNotFindFieldInRow", meta
            .getCompareFields()[ i ] ) );
          setErrors( 1 );
          stopAll();
          return false;
        }
        if ( data.sendDuplicateRows ) {
          data.compareFields =
            data.compareFields == null ? meta.getCompareFields()[ i ] : data.compareFields
              + "," + meta.getCompareFields()[ i ];
        }
      }
      if ( data.sendDuplicateRows && !Utils.isEmpty( meta.getErrorDescription() ) ) {
        data.realErrorDescription = environmentSubstitute( meta.getErrorDescription() );
      }
    }

    if ( isUniqueRow( r ) ) {
      putRow( data.outputRowMeta, r );
    } else {
      incrementLinesRejected();
      if ( data.sendDuplicateRows ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, data.realErrorDescription, Utils.isEmpty( data.compareFields )
          ? null : data.compareFields, "UNRH001" );
      }
    }

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "UniqueRowsByHashSet.Log.LineNumber" ) + getLinesRead() );
      }
    }

    return true;
  }

  public boolean init( TransformMetaInterface smi, ITransformData sdi ) {
    meta = (UniqueRowsByHashSetMeta) smi;
    data = (UniqueRowsByHashSetData) sdi;

    if ( super.init( smi, sdi ) ) {
      // Add init code here.
      data.sendDuplicateRows = getTransformMeta().getTransformErrorMeta() != null && meta.supportsErrorHandling();
      return true;
    }
    return false;
  }

}
