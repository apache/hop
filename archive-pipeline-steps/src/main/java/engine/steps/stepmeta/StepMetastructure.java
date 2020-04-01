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

package org.apache.hop.pipeline.steps.stepmeta;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.BaseStep;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;

/**
 * Return the structure of the stream
 *
 * @author Ingo Klose
 * @since 22-april-2008
 */

public class StepMetastructure extends BaseStep implements StepInterface {
  private StepMetastructureMeta meta;
  private StepMetastructureData data;

  public StepMetastructure( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                            PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( stepMeta, stepDataInterface, copyNr, pipelineMeta, pipeline );
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (StepMetastructureMeta) smi;
    data = (StepMetastructureData) sdi;

    if ( super.init( smi, sdi ) ) {
      // Add init code here.
      data.rowCount = 0;
      return true;
    }
    return false;
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws HopException {

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    Object[] metastructureRow = null;

    // initialize
    if ( first ) {
      first = false;

      // handle empty input
      if ( r == null ) {
        setOutputDone();
        return false;
      }

      // Create the row metadata for the output rows
      //
      data.outputRowMeta = new RowMeta();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, metaStore );
    }

    if ( r == null ) {
      metastructureRow = RowDataUtil.allocateRowData( data.outputRowMeta.size() );

      RowMetaInterface row = getInputRowMeta().clone();

      for ( int i = 0; i < row.size(); i++ ) {

        ValueMetaInterface v = row.getValueMeta( i );

        ValueMetaInterface v_position = data.outputRowMeta.getValueMeta( 0 );
        metastructureRow =
          RowDataUtil.addValueData( metastructureRow, 0, v_position.convertDataCompatible( v_position, new Long(
            i + 1 ) ) );

        metastructureRow = RowDataUtil.addValueData( metastructureRow, 1, v.getName() );
        metastructureRow = RowDataUtil.addValueData( metastructureRow, 2, v.getComments() );
        metastructureRow = RowDataUtil.addValueData( metastructureRow, 3, v.getTypeDesc() );

        ValueMetaInterface v_length = data.outputRowMeta.getValueMeta( 4 );
        metastructureRow =
          RowDataUtil.addValueData( metastructureRow, 4, v_length.convertDataCompatible( v_length, new Long( v
            .getLength() ) ) );

        ValueMetaInterface v_precision = data.outputRowMeta.getValueMeta( 5 );
        metastructureRow =
          RowDataUtil.addValueData( metastructureRow, 5, v_precision.convertDataCompatible(
            v_precision, new Long( v.getPrecision() ) ) );

        metastructureRow = RowDataUtil.addValueData( metastructureRow, 6, v.getOrigin() );

        if ( meta.isOutputRowcount() ) {
          ValueMetaInterface v_rowCount = data.outputRowMeta.getValueMeta( 7 );
          metastructureRow =
            RowDataUtil.addValueData( metastructureRow, 7, v_rowCount.convertDataCompatible(
              v_rowCount, new Long( data.rowCount ) ) );
        }
        putRow( data.outputRowMeta, metastructureRow.clone() );
      }

      // We're done, call it a day
      //
      setOutputDone();
      return false;
    }

    data.rowCount++;

    return true;

  }

}
