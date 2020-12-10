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

package org.apache.hop.pipeline.transforms.getvariable;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;

/**
 * Get information from the System or the supervising pipeline.
 *
 * @author Matt
 * @since 4-aug-2003
 */
public class GetVariable extends BaseTransform<GetVariableMeta, GetVariableData> implements ITransform<GetVariableMeta, GetVariableData> {

  public GetVariable( TransformMeta transformMeta, GetVariableMeta meta, GetVariableData data, int copyNr, PipelineMeta pipelineMeta,
                      Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {
    Object[] rowData;

    if ( data.readsRows ) {
      rowData = getRow();
      if ( rowData == null ) {
        setOutputDone();
        return false;
      }
    } else {
      rowData = RowDataUtil.allocateRowData( 0 );
      incrementLinesRead();
    }

    // initialize
    if ( first && rowData != null ) {
      first = false;

      // Make output meta data
      //
      if ( data.readsRows ) {
        data.inputRowMeta = getInputRowMeta();
      } else {
        data.inputRowMeta = new RowMeta();
      }
      data.outputRowMeta = data.inputRowMeta.clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Create a copy of the output row metadata to do the data conversion...
      //
      data.conversionMeta = data.outputRowMeta.cloneToType( IValueMeta.TYPE_STRING );

      // Add the variables to the row...
      //
      // Keep the Object[] for speed. Although this transform will always be used in "small" amounts, there's always going to
      // be those cases where performance is required.
      //
      int fieldsLength = meta.getFieldDefinitions().length;
      data.extraData = new Object[ fieldsLength ];
      for ( int i = 0; i < fieldsLength; i++ ) {
        String newValue = resolve( meta.getFieldDefinitions()[ i ].getVariableString() );
        if ( log.isDetailed() ) {
          logDetailed( "field [" + meta.getFieldDefinitions()[ i ].getFieldName() + "] has value [" + newValue + "]" );
        }

        // Convert the data to the desired data type...
        //
        IValueMeta targetMeta = data.outputRowMeta.getValueMeta( data.inputRowMeta.size() + i );
        IValueMeta sourceMeta = data.conversionMeta.getValueMeta( data.inputRowMeta.size() + i ); // String type
        // +
        // conversion
        // masks,
        // symbols,
        // trim type,
        // etc
        data.extraData[ i ] = targetMeta.convertData( sourceMeta, newValue );
      }
    }

    rowData = RowDataUtil.addRowData( rowData, data.inputRowMeta.size(), data.extraData );

    putRow( data.outputRowMeta, rowData );

    if ( !data.readsRows ) { // Just one row and then stop!

      setOutputDone();
      return false;
    }

    return true;
  }

  public boolean init() {

    if ( super.init() ) {
      // Add init code here.
      //      data.readsRows = getTransformMeta().getRemoteInputTransforms().size() > 0;
      List<TransformMeta> previous = getPipelineMeta().findPreviousTransforms( getTransformMeta() );
      if ( previous != null && previous.size() > 0 ) {
        data.readsRows = true;
      }

      return true;
    }
    return false;
  }

}
