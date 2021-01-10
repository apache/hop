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

package org.apache.hop.pipeline.transforms.flattener;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Pivots data based on key-value pairs
 *
 * @author Matt
 * @since 17-jan-2006
 */
public class Flattener extends BaseTransform<FlattenerMeta, FlattenerData> implements ITransform<FlattenerMeta, FlattenerData> {
  private static final Class<?> PKG = FlattenerMeta.class; // For Translator

  public Flattener( TransformMeta transformMeta, FlattenerMeta meta, FlattenerData data, int copyNr, PipelineMeta pipelineMeta,
                    Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {
    Object[] r = getRow(); // get row!
    if ( r == null ) { // no more input to be expected...

      // Don't forget the last set of rows...
      if ( data.processed > 0 ) {
        Object[] outputRowData = createOutputRow( data.previousRow );

        // send out inputrow + the flattened part
        //
        putRow( data.outputRowMeta, outputRowData );
      }

      setOutputDone();
      return false;
    }

    if ( first ) {
      data.inputRowMeta = getInputRowMeta();
      data.outputRowMeta = data.inputRowMeta.clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      data.fieldNr = data.inputRowMeta.indexOfValue( meta.getFieldName() );
      if ( data.fieldNr < 0 ) {
        logError( BaseMessages.getString( PKG, "Flattener.Log.FieldCouldNotFound", meta.getFieldName() ) );
        setErrors( 1 );
        stopAll();
        return false;
      }

      // Allocate the result row...
      //
      data.targetResult = new Object[ meta.getTargetField().length ];

      first = false;
    }

    // set it to value # data.processed
    //
    data.targetResult[ data.processed++ ] = r[ data.fieldNr ];

    if ( data.processed >= meta.getTargetField().length ) {
      Object[] outputRowData = createOutputRow( r );

      // send out input row + the flattened part
      putRow( data.outputRowMeta, outputRowData );

      // clear the result row
      data.targetResult = new Object[ meta.getTargetField().length ];

      data.processed = 0;
    }

    // Keep track in case we want to send out the last couple of flattened values.
    data.previousRow = r;

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "Flattener.Log.LineNumber" ) + getLinesRead() );
    }

    return true;
  }

  private Object[] createOutputRow( Object[] rowData ) {

    Object[] outputRowData = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
    int outputIndex = 0;

    // copy the values from previous, but don't take along index 'data.fieldNr'...
    //
    for ( int i = 0; i < data.inputRowMeta.size(); i++ ) {
      if ( i != data.fieldNr ) {
        outputRowData[ outputIndex++ ] = rowData[ i ];
      }
    }

    // Now add the fields we flattened...
    //
    for ( int i = 0; i < data.targetResult.length; i++ ) {
      outputRowData[ outputIndex++ ] = data.targetResult[ i ];
    }

    return outputRowData;
  }
}
