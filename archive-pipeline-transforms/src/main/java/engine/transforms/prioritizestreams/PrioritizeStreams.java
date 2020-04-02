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

package org.apache.hop.pipeline.transforms.prioritizestreams;

import org.apache.hop.core.RowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRowException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;

/**
 * Prioritize INPUT Streams.
 *
 * @author Samatar
 * @since 30-06-2008
 */

public class PrioritizeStreams extends BaseTransform implements ITransform {
  private static Class<?> PKG = PrioritizeStreamsMeta.class; // for i18n purposes, needed by Translator!!

  private PrioritizeStreamsMeta meta;
  private PrioritizeStreamsData data;

  public PrioritizeStreams( TransformMeta transformMeta, ITransformData iTransformData, int copyNr,
                            PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, iTransformData, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow( TransformMetaInterface smi, ITransformData sdi ) throws HopException {
    meta = (PrioritizeStreamsMeta) smi;
    data = (PrioritizeStreamsData) sdi;

    if ( first ) {
      if ( meta.getTransformName() != null || meta.getTransformName().length > 0 ) {
        data.transformnrs = meta.getTransformName().length;
        data.rowSets = new RowSet[ data.transformnrs ];

        for ( int i = 0; i < data.transformnrs; i++ ) {
          data.rowSets[ i ] = findInputRowSet( meta.getTransformName()[ i ] );
          if ( i > 0 ) {
            // Compare layout of first stream with the current stream
            checkInputLayoutValid( data.rowSets[ 0 ].getRowMeta(), data.rowSets[ i ].getRowMeta() );
          }
        }
      } else {
        // error
        throw new HopException( BaseMessages.getString( PKG, "PrioritizeStreams.Error.NotInputTransforms" ) );
      }
      data.currentRowSet = data.rowSets[ 0 ];
    } // end if first, part 1

    Object[] input = getOneRow();

    while ( input == null && data.transformnr < data.transformnrs - 1 && !isStopped() ) {
      input = getOneRow();
    }

    if ( input == null ) {
      // no more input to be expected...
      setOutputDone();
      return false;
    }

    if ( first ) {
      // Take the row Meta from the first rowset read
      data.outputRowMeta = data.currentRowSet.getRowMeta();
      first = false;
    }

    putRow( data.outputRowMeta, input );

    return true;
  }

  private Object[] getOneRow() throws HopException {
    Object[] input = getRowFrom( data.currentRowSet );
    if ( input == null ) {
      if ( data.transformnr < data.transformnrs - 1 ) {
        // read rows from the next transform
        data.transformnr++;
        data.currentRowSet = data.rowSets[ data.transformnr ];
        input = getRowFrom( data.currentRowSet );
      }
    }
    return input;
  }

  public boolean init( TransformMetaInterface smi, ITransformData sdi ) {
    meta = (PrioritizeStreamsMeta) smi;
    data = (PrioritizeStreamsData) sdi;

    if ( super.init( smi, sdi ) ) {
      // Add init code here.
      data.transformnr = 0;
      return true;
    }
    return false;
  }

  public void dispose( TransformMetaInterface smi, ITransformData sdi ) {
    data.currentRowSet = null;
    data.rowSets = null;
    super.dispose( smi, sdi );
  }

  /**
   * Checks whether 2 template rows are compatible for the merge transform.
   *
   * @param referenceRow Reference row
   * @param compareRow   Row to compare to
   * @return true when templates are compatible.
   * @throws HopRowException in case there is a compatibility error.
   */
  protected void checkInputLayoutValid( IRowMeta referenceRowMeta, IRowMeta compareRowMeta ) throws HopRowException {
    if ( referenceRowMeta != null && compareRowMeta != null ) {
      BaseTransform.safeModeChecking( referenceRowMeta, compareRowMeta );
    }
  }
}
