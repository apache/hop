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

package org.apache.hop.pipeline.transforms.samplerows;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Sample rows. Filter rows based on line number
 *
 * @author Samatar
 * @since 2-jun-2003
 */

public class SampleRows extends BaseTransform<SampleRowsMeta, SampleRowsData> implements ITransform<SampleRowsMeta, SampleRowsData> {
  private static final Class<?> PKG = SampleRowsMeta.class; // For Translator

  public SampleRows( TransformMeta transformMeta, SampleRowsMeta meta, SampleRowsData data, int copyNr, PipelineMeta pipelineMeta,
                     Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }
    if ( first ) {
      first = false;

      String realRange = resolve( meta.getLinesRange() );
      data.addlineField = ( !Utils.isEmpty( resolve( meta.getLineNumberField() ) ) );

      // get the RowMeta
      data.previousRowMeta = getInputRowMeta().clone();
      data.NrPrevFields = data.previousRowMeta.size();
      data.outputRowMeta = data.previousRowMeta;
      if ( data.addlineField ) {
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
      }

      String[] rangePart = realRange.split( "," );
      ImmutableRangeSet.Builder<Integer> setBuilder = ImmutableRangeSet.builder();

      for ( String part : rangePart ) {
        if ( part.matches( "\\d+" ) ) {
          if ( log.isDebug() ) {
            logDebug( BaseMessages.getString( PKG, "SampleRows.Log.RangeValue", part ) );
          }
          int vpart = Integer.valueOf( part );
          setBuilder.add( Range.singleton( vpart ) );

        } else if ( part.matches( "\\d+\\.\\.\\d+" ) ) {
          String[] rangeMultiPart = part.split( "\\.\\." );
          Integer start = Integer.valueOf( rangeMultiPart[ 0 ] );
          Integer end = Integer.valueOf( rangeMultiPart[ 1 ] );
          Range<Integer> range = Range.closed( start, end );
          if ( log.isDebug() ) {
            logDebug( BaseMessages.getString( PKG, "SampleRows.Log.RangeValue", range ) );
          }
          setBuilder.add( range );
        }
      }
      data.rangeSet = setBuilder.build();
    } // end if first

    if ( data.addlineField ) {
      data.outputRow = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
      for ( int i = 0; i < data.NrPrevFields; i++ ) {
        data.outputRow[ i ] = r[ i ];
      }
    } else {
      data.outputRow = r;
    }

    int linesRead = (int) getLinesRead();
    if ( data.rangeSet.contains( linesRead ) ) {
      if ( data.addlineField ) {
        data.outputRow[ data.NrPrevFields ] = getLinesRead();
      }

      // copy row to possible alternate rowset(s).
      //
      putRow( data.outputRowMeta, data.outputRow );

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "SampleRows.Log.LineNumber", linesRead
          + " : " + getInputRowMeta().getString( r ) ) );
      }
    }

    // Check if maximum value has been exceeded
    if ( data.rangeSet.isEmpty() || linesRead >= data.rangeSet.span().upperEndpoint() ) {
      setOutputDone();
    }

    // Allowed to continue to read in data
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
