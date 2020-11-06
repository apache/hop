/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.transformmeta;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;

/**
 * Return the structure of the stream
 *
 * @author Ingo Klose
 * @since 22-april-2008
 */

public class TransformMetastructure extends BaseTransform implements ITransform {
  private TransformMetastructureMeta meta;
  private TransformMetastructureData data;

  public TransformMetastructure( TransformMeta transformMeta, ITransformData data, int copyNr,
                                 PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean init() {
    meta = (TransformMetastructureMeta) smi;
    data = (TransformMetastructureData) sdi;

    if ( super.init() ) {
      // Add init code here.
      data.rowCount = 0;
      return true;
    }
    return false;
  }

  public boolean processRow() throws HopException {

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
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
    }

    if ( r == null ) {
      metastructureRow = RowDataUtil.allocateRowData( data.outputRowMeta.size() );

      IRowMeta row = getInputRowMeta().clone();

      for ( int i = 0; i < row.size(); i++ ) {

        IValueMeta v = row.getValueMeta( i );

        IValueMeta vPosition = data.outputRowMeta.getValueMeta( 0 );
        metastructureRow =
          RowDataUtil.addValueData( metastructureRow, 0, vPosition.convertDataCompatible( vPosition, new Long(
            i + 1 ) ) );

        metastructureRow = RowDataUtil.addValueData( metastructureRow, 1, v.getName() );
        metastructureRow = RowDataUtil.addValueData( metastructureRow, 2, v.getComments() );
        metastructureRow = RowDataUtil.addValueData( metastructureRow, 3, v.getTypeDesc() );

        IValueMeta vLength = data.outputRowMeta.getValueMeta( 4 );
        metastructureRow =
          RowDataUtil.addValueData( metastructureRow, 4, vLength.convertDataCompatible( vLength, new Long( v
            .getLength() ) ) );

        IValueMeta vPrecision = data.outputRowMeta.getValueMeta( 5 );
        metastructureRow =
          RowDataUtil.addValueData( metastructureRow, 5, vPrecision.convertDataCompatible(
            vPrecision, new Long( v.getPrecision() ) ) );

        metastructureRow = RowDataUtil.addValueData( metastructureRow, 6, v.getOrigin() );

        if ( meta.isOutputRowcount() ) {
          IValueMeta vRowCount = data.outputRowMeta.getValueMeta( 7 );
          metastructureRow =
            RowDataUtil.addValueData( metastructureRow, 7, vRowCount.convertDataCompatible(
              vRowCount, new Long( data.rowCount ) ) );
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
