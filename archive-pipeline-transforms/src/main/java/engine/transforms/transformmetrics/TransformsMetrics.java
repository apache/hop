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

package org.apache.hop.pipeline.transforms.transformsmetrics;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.BaseTransformData.TransformExecutionStatus;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Ouptu transform metrics
 *
 * @author Samatar
 * @since 30-06-2008
 */

public class TransformsMetrics extends BaseTransform implements ITransform {
  private static Class<?> PKG = TransformsMetrics.class; // for i18n purposes, needed by Translator!!

  private TransformsMetricsMeta meta;
  private TransformsMetricsData data;

  public HashSet<ITransform> transformInterfaces;

  public TransformsMetrics( TransformMeta transformMeta, ITransformData data, int copyNr, PipelineMeta pipelineMeta,
                       Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {
    meta = (TransformsMetricsMeta) smi;
    data = (TransformsMetricsData) sdi;

    if ( first ) {
      first = false;
      String[] transformnames = null;
      int transformnrs = 0;
      if ( meta.getTransformName() != null && meta.getTransformName().length > 0 ) {
        transformnames = meta.getTransformName();
        transformnrs = transformnames.length;
      } else {
        // error
        throw new HopException( BaseMessages.getString( PKG, "TransformsMetrics.Error.NotTransforms" ) );
      }
      // check for output fields
      data.realTransformNameField = environmentSubstitute( meta.getTransformNameFieldName() );
      data.realTransformIdField = environmentSubstitute( meta.getTransformIdFieldName() );
      data.realTransformLinesInputField = environmentSubstitute( meta.getTransformLinesInputFieldName() );
      data.realTransformLinesOutputField = environmentSubstitute( meta.getTransformLinesOutputFieldName() );
      data.realTransformLinesreadField = environmentSubstitute( meta.getTransformLinesReadFieldName() );
      data.realTransformLinesWrittenField = environmentSubstitute( meta.getTransformLinesWrittenFieldName() );
      data.realTransformLinesUpdatedField = environmentSubstitute( meta.getTransformLinesUpdatedFieldName() );
      data.realTransformLinesErrorsField = environmentSubstitute( meta.getTransformLinesErrorsFieldName() );
      data.realTransformSecondsField = environmentSubstitute( meta.getTransformSecondsFieldName() );

      // Get target transformnames
      String[] targetTransforms = getPipelineMeta().getNextTransformNames( getTransformMeta() );

      data.transformInterfaces = new ConcurrentHashMap<Integer, ITransform>();
      for ( int i = 0; i < transformnrs; i++ ) {
        // We can not get metrics from current transform
        if ( transformnames[ i ].equals( getTransformName() ) ) {
          throw new HopException( "You can not get metrics for the current transform [" + transformnames[ i ] + "]!" );
        }
        if ( targetTransforms != null ) {
          // We can not metrics from the target transforms
          for ( int j = 0; j < targetTransforms.length; j++ ) {
            if ( transformnames[ i ].equals( targetTransforms[ j ] ) ) {
              throw new HopException( "You can not get metrics for the target transform [" + targetTransforms[ j ] + "]!" );
            }
          }
        }

        int CopyNr = Const.toInt( meta.getTransformCopyNr()[ i ], 0 );
        ITransform si = getPipeline().getTransformInterface( transformnames[ i ], CopyNr );
        if ( si != null ) {
          data.transformInterfaces.put( i, getDispatcher().findBaseTransforms( transformnames[ i ] ).get( CopyNr ) );
        } else {
          if ( meta.getTransformRequired()[ i ].equals( TransformsMetricsMeta.YES ) ) {
            throw new HopException( "We cannot get transform [" + transformnames[ i ] + "] CopyNr=" + CopyNr + "!" );
          }
        }
      }

      data.outputRowMeta = new RowMeta();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metaStore );
    } // end if first

    data.continueLoop = true;
    // Wait until all specified transforms have finished!
    while ( data.continueLoop && !isStopped() ) {
      data.continueLoop = false;
      Iterator<Entry<Integer, ITransform>> it = data.transformInterfaces.entrySet().iterator();
      while ( it.hasNext() ) {
        Entry<Integer, ITransform> e = it.next();
        ITransform transform = e.getValue();

        if ( transform.getStatus() != TransformExecutionStatus.STATUS_FINISHED ) {
          // This transform is still running...
          data.continueLoop = true;
        } else {
          // We have done with this transform.
          // remove it from the map
          data.transformInterfaces.remove( e.getKey() );
          if ( log.isDetailed() ) {
            logDetailed( "Finished running transform [" + transform.getTransformName() + "(" + transform.getCopy() + ")]." );
          }

          // Build an empty row based on the meta-data
          Object[] rowData = buildEmptyRow();
          incrementLinesRead();

          int index = 0;
          // transform finished
          // output transform metrics
          if ( !Utils.isEmpty( data.realTransformNameField ) ) {
            rowData[ index++ ] = transform.getTransformName();
          }
          if ( !Utils.isEmpty( data.realTransformIdField ) ) {
            rowData[ index++ ] = transform.getTransformPluginId();
          }
          if ( !Utils.isEmpty( data.realTransformLinesInputField ) ) {
            rowData[ index++ ] = transform.getLinesInput();
          }
          if ( !Utils.isEmpty( data.realTransformLinesOutputField ) ) {
            rowData[ index++ ] = transform.getLinesOutput();
          }
          if ( !Utils.isEmpty( data.realTransformLinesreadField ) ) {
            rowData[ index++ ] = transform.getLinesRead();
          }
          if ( !Utils.isEmpty( data.realTransformLinesUpdatedField ) ) {
            rowData[ index++ ] = transform.getLinesUpdated();
          }
          if ( !Utils.isEmpty( data.realTransformLinesWrittenField ) ) {
            rowData[ index++ ] = transform.getLinesWritten();
          }
          if ( !Utils.isEmpty( data.realTransformLinesErrorsField ) ) {
            rowData[ index++ ] = transform.getLinesRejected();
          }
          if ( !Utils.isEmpty( data.realTransformSecondsField ) ) {
            rowData[ index++ ] = transform.getRuntime();
          }

          // Send row to the buffer
          putRow( data.outputRowMeta, rowData );
        }
      }
      if ( data.continueLoop ) {
        try {
          Thread.sleep( 200 );
        } catch ( Exception d ) {
          // Ignore
        }
      }
    }

    setOutputDone();
    return false;
  }

  /**
   * Build an empty row based on the meta-data...
   *
   * @return
   */

  private Object[] buildEmptyRow() {
    Object[] rowData = RowDataUtil.allocateRowData( data.outputRowMeta.size() );

    return rowData;
  }

  public boolean init() {
    meta = (TransformsMetricsMeta) smi;
    data = (TransformsMetricsData) sdi;

    if ( super.init() ) {
      // Add init code here.
      return true;
    }
    return false;
  }

}
