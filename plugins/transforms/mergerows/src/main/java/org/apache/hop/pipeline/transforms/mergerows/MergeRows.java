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

package org.apache.hop.pipeline.transforms.mergerows;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRowException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;

import java.util.Arrays;
import java.util.List;

/**
 * Merge rows from 2 sorted streams to detect changes. Use this as feed for a dimension in case you have no time stamps
 * in your source system.
 *
 * @author Matt
 * @since 19-dec-2005
 */
public class MergeRows extends BaseTransform<MergeRowsMeta, MergeRowsData> implements ITransform<MergeRowsMeta, MergeRowsData> {
  private static final Class<?> PKG = MergeRowsMeta.class; // For Translator

  private static final String VALUE_IDENTICAL = "identical";
  private static final String VALUE_CHANGED = "changed";
  private static final String VALUE_NEW = "new";
  private static final String VALUE_DELETED = "deleted";

  private boolean useRefWhenIdentical = false;

  public MergeRows( TransformMeta transformMeta, MergeRowsMeta meta, MergeRowsData data, int copyNr, PipelineMeta pipelineMeta,
                    Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {
    if ( first ) {
      first = false;

      // Find the appropriate RowSet
      //
      List<IStream> infoStreams = meta.getTransformIOMeta().getInfoStreams();

      //oneRowSet is the "Reference" stream
      data.oneRowSet = findInputRowSet( infoStreams.get( 0 ).getTransformName() );
      //twoRowSet is the "Comparison" stream
      data.twoRowSet = findInputRowSet( infoStreams.get( 1 ).getTransformName() );

      //rowSetWhenIdentical is use in case the comparison is IDENTICAL.
      //this should be the "Comparison" stream but can be the "Reference" stream for backward compatibility (PDI-736)
      String useRefWhenIdenticalVar = Const.NVL( getVariable( Const.HOP_COMPATIBILITY_MERGE_ROWS_USE_REFERENCE_STREAM_WHEN_IDENTICAL ), "N" );
      if ( "N".equalsIgnoreCase( useRefWhenIdenticalVar ) ) {
        //use the reference stream (as per documentation)
        useRefWhenIdentical = false;
      } else {
        //use the comparison stream (for backward compatibility)
        useRefWhenIdentical = true;
      }
      data.one = getRowFrom( data.oneRowSet );
      data.two = getRowFrom( data.twoRowSet );

      try {
        checkInputLayoutValid( data.oneRowSet.getRowMeta(), data.twoRowSet.getRowMeta() );
      } catch ( HopRowException e ) {
        throw new HopException( BaseMessages.getString( PKG, "MergeRows.Exception.InvalidLayoutDetected" ), e );
      }

      if ( data.one != null ) {
        // Find the key indexes:
        data.keyNrs = new int[ meta.getKeyFields().length ];
        for ( int i = 0; i < data.keyNrs.length; i++ ) {
          data.keyNrs[ i ] = data.oneRowSet.getRowMeta().indexOfValue( meta.getKeyFields()[ i ] );
          if ( data.keyNrs[ i ] < 0 ) {
            String message =
              BaseMessages.getString( PKG, "MergeRows.Exception.UnableToFindFieldInReferenceStream", meta
                .getKeyFields()[ i ] );
            logError( message );
            throw new HopTransformException( message );
          }
        }
      }

      if ( data.two != null ) {
        data.valueNrs = new int[ meta.getValueFields().length ];
        for ( int i = 0; i < data.valueNrs.length; i++ ) {
          data.valueNrs[ i ] = data.twoRowSet.getRowMeta().indexOfValue( meta.getValueFields()[ i ] );
          if ( data.valueNrs[ i ] < 0 ) {
            String message =
              BaseMessages.getString( PKG, "MergeRows.Exception.UnableToFindFieldInReferenceStream", meta
                .getValueFields()[ i ] );
            logError( message );
            throw new HopTransformException( message );
          }
        }
      }
    }

    if ( log.isRowLevel() ) {
      logRowlevel( BaseMessages.getString( PKG, "MergeRows.Log.DataInfo",
        Arrays.toString( data.one ) + "" ) + Arrays.toString( data.two ) );
    }

    if ( data.one == null && data.two == null ) {
      setOutputDone();
      return false;
    }

    if ( data.outputRowMeta == null ) {
      data.outputRowMeta = new RowMeta();
      if ( data.one != null ) {
        meta.getFields(
          data.outputRowMeta, getTransformName(), new IRowMeta[] { data.oneRowSet.getRowMeta() }, null, this,
          metadataProvider );
      } else {
        meta.getFields(
          data.outputRowMeta, getTransformName(), new IRowMeta[] { data.twoRowSet.getRowMeta() }, null, this,
          metadataProvider );
      }
    }

    Object[] outputRow;
    int outputIndex;
    String flagField = null;

    if ( data.one == null && data.two != null ) { // Record 2 is flagged as new!

      outputRow = data.two;
      outputIndex = data.twoRowSet.getRowMeta().size();
      flagField = VALUE_NEW;

      // Also get a next row from compare rowset...
      data.two = getRowFrom( data.twoRowSet );
    } else if ( data.one != null && data.two == null ) { // Record 1 is flagged as deleted!
      outputRow = data.one;
      outputIndex = data.oneRowSet.getRowMeta().size();
      flagField = VALUE_DELETED;

      // Also get a next row from reference rowset...
      data.one = getRowFrom( data.oneRowSet );
    } else { // OK, Here is the real start of the compare code!

      int compare = data.oneRowSet.getRowMeta().compare( data.one, data.two, data.keyNrs );
      if ( compare == 0 ) { // The Key matches, we CAN compare the two rows...

        int compareValues = data.oneRowSet.getRowMeta().compare( data.one, data.two, data.valueNrs );
        if ( compareValues == 0 ) {
          if ( useRefWhenIdentical ) {  //backwards compatible behavior: use the reference stream (PDI-736)
            outputRow = data.one;
            outputIndex = data.oneRowSet.getRowMeta().size();
          } else {
            outputRow = data.two;       //documented behavior: use the comparison stream (PDI-736)
            outputIndex = data.twoRowSet.getRowMeta().size();
          }
          flagField = VALUE_IDENTICAL;
        } else {
          // Return the compare (most recent) row
          //
          outputRow = data.two;
          outputIndex = data.twoRowSet.getRowMeta().size();
          flagField = VALUE_CHANGED;
        }

        // Get a new row from both streams...
        data.one = getRowFrom( data.oneRowSet );
        data.two = getRowFrom( data.twoRowSet );
      } else {
        if ( compare < 0 ) { // one < two

          outputRow = data.one;
          outputIndex = data.oneRowSet.getRowMeta().size();
          flagField = VALUE_DELETED;

          data.one = getRowFrom( data.oneRowSet );
        } else {
          outputRow = data.two;
          outputIndex = data.twoRowSet.getRowMeta().size();
          flagField = VALUE_NEW;

          data.two = getRowFrom( data.twoRowSet );
        }
      }
    }

    // send the row to the next transforms...
    putRow( data.outputRowMeta, RowDataUtil.addValueData( outputRow, outputIndex, flagField ) );

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "MergeRows.LineNumber" ) + getLinesRead() );
      }
    }

    return true;
  }

  /**
   * @see ITransform#init(org.apache.hop.pipeline.transform.ITransform, org.apache.hop.pipeline.transform.ITransformData)
   */
  public boolean init() {

    if ( super.init() ) {
      List<IStream> infoStreams = meta.getTransformIOMeta().getInfoStreams();

      if ( infoStreams.get( 0 ).getTransformMeta() != null ^ infoStreams.get( 1 ).getTransformMeta() != null ) {
        logError( BaseMessages.getString( PKG, "MergeRows.Log.BothTrueAndFalseNeeded" ) );
      } else {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks whether 2 template rows are compatible for the merge transform.
   *
   * @param referenceRowMeta Reference row
   * @param compareRowMeta   Row to compare to
   * @return true when templates are compatible.
   * @throws HopRowException in case there is a compatibility error.
   */
  static void checkInputLayoutValid( IRowMeta referenceRowMeta, IRowMeta compareRowMeta ) throws HopRowException {
    if ( referenceRowMeta != null && compareRowMeta != null ) {
      BaseTransform.safeModeChecking( referenceRowMeta, compareRowMeta );
    }
  }

}
