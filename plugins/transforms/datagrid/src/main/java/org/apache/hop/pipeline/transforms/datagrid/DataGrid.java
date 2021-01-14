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

package org.apache.hop.pipeline.transforms.datagrid;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;

import java.util.List;

/**
 * Generates a number of (empty or the same) rows
 *
 * @author Matt
 * @since 4-apr-2003
 */
public class DataGrid extends BaseTransform<DataGridMeta, DataGridData> implements ITransform<DataGridMeta, DataGridData> {

  private static final Class<?> PKG = DataGridMeta.class; // For Translator

  public DataGrid( TransformMeta transformMeta, DataGridMeta meta, DataGridData data, int copyNr, PipelineMeta pipelineMeta,
                   Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {
    if ( data.linesWritten >= meta.getDataLines().size() ) {
      // no more rows to be written
      setOutputDone();
      return false;
    }

    if ( first ) {
      // The output meta is the original input meta + the
      // additional constant fields.

      first = false;
      data.linesWritten = 0;

      data.outputRowMeta = new RowMeta();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Use these metadata values to convert data...
      //
      data.convertMeta = data.outputRowMeta.cloneToType( IValueMeta.TYPE_STRING );
    }

    Object[] outputRowData = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
    List<String> outputLine = meta.getDataLines().get( data.linesWritten );

    for ( int i = 0; i < data.outputRowMeta.size(); i++ ) {
      if ( meta.isSetEmptyString()[ i ] ) {
        // Set empty string
        outputRowData[ i ] = StringUtil.EMPTY_STRING;
      } else {

        IValueMeta valueMeta = data.outputRowMeta.getValueMeta( i );
        IValueMeta convertMeta = data.convertMeta.getValueMeta( i );
        String valueData = outputLine.get( i );

        if ( valueData != null && valueMeta.isNull( valueData ) ) {
          valueData = null;
        }
        outputRowData[ i ] = valueMeta.convertDataFromString( valueData, convertMeta, null, null, 0 );
      }
    }

    putRow( data.outputRowMeta, outputRowData );
    data.linesWritten++;

    if ( log.isRowLevel() ) {
      log.logRowlevel( toString(), BaseMessages.getString( PKG, "DataGrid.Log.Wrote.Row", Long
        .toString( getLinesWritten() ), data.outputRowMeta.getString( outputRowData ) ) );
    }

    if ( checkFeedback( getLinesWritten() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "DataGrid.Log.LineNr", Long.toString( getLinesWritten() ) ) );
      }
    }

    return true;
  }

}
