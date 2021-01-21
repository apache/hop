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

package org.apache.hop.pipeline.transforms.filterrows;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;

import java.util.List;

/**
 * Filters input rows base on conditions.
 *
 * @author Matt
 * @since 16-apr-2003, 07-nov-2004 (rewrite)
 */
public class FilterRows extends BaseTransform<FilterRowsMeta, FilterRowsData> implements ITransform<FilterRowsMeta, FilterRowsData> {

  private static final Class<?> PKG = FilterRowsMeta.class; // For Translator

  public FilterRows( TransformMeta transformMeta, FilterRowsMeta meta, FilterRowsData data, int copyNr, PipelineMeta pipelineMeta,
                     Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private synchronized boolean keepRow( IRowMeta rowMeta, Object[] row ) throws HopException {
    try {
      return meta.getCondition().evaluate( rowMeta, row );
    } catch ( Exception e ) {
      String message =
        BaseMessages.getString( PKG, "FilterRows.Exception.UnexpectedErrorFoundInEvaluationFuction" );
      logError( message );
      logError( BaseMessages.getString( PKG, "FilterRows.Log.ErrorOccurredForRow" ) + rowMeta.getString( row ) );
      logError( Const.getStackTracker( e ) );
      throw new HopException( message, e );
    }
  }

  public boolean processRow() throws HopException {

    boolean keep;

    Object[] r = getRow(); // Get next usable row from input rowset(s)!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( getInputRowMeta(), getTransformName(), null, null, this, metadataProvider );

      // if filter refers to non-existing fields, throw exception
      checkNonExistingFields();

      // Cache the position of the IRowSet for the output.
      //
      if ( data.chosesTargetTransforms ) {
        List<IStream> targetStreams = meta.getTransformIOMeta().getTargetStreams();
        if ( !Utils.isEmpty( targetStreams.get( 0 ).getTransformName() ) ) {
          data.trueRowSet = findOutputRowSet( getTransformName(), getCopy(), targetStreams.get( 0 ).getTransformName(), 0 );
          if ( data.trueRowSet == null ) {
            throw new HopException( BaseMessages.getString(
              PKG, "FilterRows.Log.TargetTransformInvalid", targetStreams.get( 0 ).getTransformName() ) );
          }
        } else {
          data.trueRowSet = null;
        }

        if ( !Utils.isEmpty( targetStreams.get( 1 ).getTransformName() ) ) {
          data.falseRowSet = findOutputRowSet( getTransformName(), getCopy(), targetStreams.get( 1 ).getTransformName(), 0 );
          if ( data.falseRowSet == null ) {
            throw new HopException( BaseMessages.getString(
              PKG, "FilterRows.Log.TargetTransformInvalid", targetStreams.get( 1 ).getTransformName() ) );
          }
        } else {
          data.falseRowSet = null;
        }
      }
    }

    keep = keepRow( getInputRowMeta(), r ); // Keep this row?
    if ( !data.chosesTargetTransforms ) {
      if ( keep ) {
        putRow( data.outputRowMeta, r ); // copy row to output rowset(s);
      }
    } else {
      if ( keep ) {
        if ( data.trueRowSet != null ) {
          if ( log.isRowLevel() ) {
            logRowlevel( "Sending row to true  :" + data.trueTransformName + " : " + getInputRowMeta().getString( r ) );
          }
          putRowTo( data.outputRowMeta, r, data.trueRowSet );
        }
      } else {
        if ( data.falseRowSet != null ) {
          if ( log.isRowLevel() ) {
            logRowlevel( "Sending row to false :" + data.falseTransformName + " : " + getInputRowMeta().getString( r ) );
          }
          putRowTo( data.outputRowMeta, r, data.falseRowSet );
        }
      }
    }

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "FilterRows.Log.LineNumber" ) + getLinesRead() );
      }
    }

    return true;
  }

  @Override
  public boolean init() {

    if ( super.init() ) {
      // PDI-6785
      // could it be a better idea to have a clone on the condition in data and do this on the first row?
      meta.getCondition().clearFieldPositions();

      List<IStream> targetStreams = meta.getTransformIOMeta().getTargetStreams();
      data.trueTransformName = targetStreams.get( 0 ).getTransformName();
      data.falseTransformName = targetStreams.get( 1 ).getTransformName();

      data.chosesTargetTransforms =
        targetStreams.get( 0 ).getTransformMeta() != null || targetStreams.get( 1 ).getTransformMeta() != null;
      return true;
    }
    return false;
  }

  protected void checkNonExistingFields() throws HopException {
    List<String> orphanFields = meta.getOrphanFields(
      meta.getCondition(), getInputRowMeta() );
    if ( orphanFields != null && orphanFields.size() > 0 ) {
      String fields = "";
      boolean first = true;
      for ( String field : orphanFields ) {
        if ( !first ) {
          fields += ", ";
        }
        fields += "'" + field + "'";
        first = false;
      }
      String errorMsg = BaseMessages.getString( PKG, "FilterRows.CheckResult.FieldsNotFoundFromPreviousTransform", fields );
      throw new HopException( errorMsg );
    }
  }
}
