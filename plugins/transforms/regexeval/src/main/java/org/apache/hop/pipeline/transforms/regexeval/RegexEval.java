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

package org.apache.hop.pipeline.transforms.regexeval;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class RegexEval extends BaseTransform<RegexEvalMeta,RegexEvalData> implements ITransform<RegexEvalMeta,RegexEvalData> {
  private static final Class<?> PKG = RegexEvalMeta.class; // For Translator

  public RegexEval(TransformMeta transformMeta, RegexEvalMeta meta, RegexEvalData data, int copyNr, PipelineMeta pipelineMeta,
                   Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {

    Object[] row = getRow();

    if ( row == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if ( first ) { // we just got started

      first = false;

      // get the RowMeta
      data.outputRowMeta = getInputRowMeta().clone();
      int captureIndex = getInputRowMeta().size();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Let's check that Result Field is given
      if ( Utils.isEmpty( resolve( meta.getResultFieldName() ) ) ) {
        if ( !meta.isAllowCaptureGroupsFlagSet() ) {
          // Result field is missing !
          logError( BaseMessages.getString( PKG, "RegexEval.Log.ErrorResultFieldMissing" ) );
          throw new HopTransformException( BaseMessages.getString(
            PKG, "RegexEval.Exception.ErrorResultFieldMissing" ) );
        }
        data.indexOfResultField = -1;
      } else {
        if ( meta.isReplacefields() ) {
          data.indexOfResultField = getInputRowMeta().indexOfValue( meta.getResultFieldName() );
        }
        if ( data.indexOfResultField < 0 ) {
          data.indexOfResultField = getInputRowMeta().size();
          captureIndex++;
        }
      }

      // Check if a Field (matcher) is given
      if ( meta.getMatcher() == null ) {
        // Matcher is missing !
        logError( BaseMessages.getString( PKG, "RegexEval.Log.ErrorMatcherMissing" ) );
        throw new HopTransformException( BaseMessages.getString( PKG, "RegexEval.Exception.ErrorMatcherMissing" ) );
      }

      // ICache the position of the Field
      data.indexOfFieldToEvaluate = getInputRowMeta().indexOfValue( meta.getMatcher() );
      if ( data.indexOfFieldToEvaluate < 0 ) {
        // The field is unreachable !
        logError( BaseMessages.getString( PKG, "RegexEval.Log.ErrorFindingField" ) + "[" + meta.getMatcher() + "]" );
        throw new HopTransformException( BaseMessages.getString( PKG, "RegexEval.Exception.CouldnotFindField", meta
          .getMatcher() ) );
      }

      // ICache the position of the CaptureGroups
      if ( meta.isAllowCaptureGroupsFlagSet() ) {
        data.positions = new int[ meta.getFieldName().length ];
        String[] fieldName = meta.getFieldName();
        for ( int i = 0; i < fieldName.length; i++ ) {
          if ( fieldName[ i ] == null || fieldName[ i ].length() == 0 ) {
            continue;
          }
          if ( meta.isReplacefields() ) {
            data.positions[ i ] = data.outputRowMeta.indexOfValue( fieldName[ i ] );
          } else {
            data.positions[ i ] = captureIndex;
            captureIndex++;
          }
        }
      } else {
        data.positions = new int[ 0 ];
      }

      // Now create objects to do string to data type conversion...
      //
      data.conversionRowMeta = data.outputRowMeta.cloneToType( IValueMeta.TYPE_STRING );
    }

    // reserve room
    Object[] outputRow = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
    System.arraycopy( row, 0, outputRow, 0, getInputRowMeta().size() );

    try {
      // Get the Field value
      String fieldValue;
      boolean isMatch;

      if ( getInputRowMeta().isNull( row, data.indexOfFieldToEvaluate ) ) {
        fieldValue = "";
        isMatch = false;
      } else {
        fieldValue = getInputRowMeta().getString( row, data.indexOfFieldToEvaluate );

        // Start search engine
        Matcher m = data.pattern.matcher( fieldValue );
        isMatch = m.matches();

        if ( meta.isAllowCaptureGroupsFlagSet() && data.positions.length != m.groupCount() ) {
          // Runtime exception case. The number of capture groups in the
          // regex doesn't match the number of fields.
          logError( BaseMessages.getString( PKG, "RegexEval.Log.ErrorCaptureGroupFieldsMismatch", String
            .valueOf( m.groupCount() ), String.valueOf( data.positions.length ) ) );
          throw new HopTransformException( BaseMessages.getString(
            PKG, "RegexEval.Exception.ErrorCaptureGroupFieldsMismatch", String.valueOf( m.groupCount() ), String
              .valueOf( data.positions.length ) ) );
        }

        for ( int i = 0; i < data.positions.length; i++ ) {
          int index = data.positions[ i ];
          String value;
          if ( isMatch ) {
            value = m.group( i + 1 );
          } else {
            value = null;
          }

          // this part (or possibly the whole) of the regex didn't match
          // preserve the incoming data, but allow for "trim type", etc.
          if ( value == null ) {
            try {
              value = data.outputRowMeta.getString( outputRow, index );
            } catch ( ArrayIndexOutOfBoundsException err ) {
              // Ignore errors
            }
          }

          IValueMeta valueMeta = data.outputRowMeta.getValueMeta( index );
          IValueMeta conversionValueMeta = data.conversionRowMeta.getValueMeta( index );
          Object convertedValue =
            valueMeta.convertDataFromString( value, conversionValueMeta, meta.getFieldNullIf()[ i ], meta
              .getFieldIfNull()[ i ], meta.getFieldTrimType()[ i ] );

          outputRow[ index ] = convertedValue;
        }
      }

      if ( data.indexOfResultField >= 0 ) {
        outputRow[ data.indexOfResultField ] = isMatch;
      }

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "RegexEval.Log.ReadRow" )
          + " " + getInputRowMeta().getString( row ) );
      }

      // copy row to output rowset(s);
      //
      putRow( data.outputRowMeta, outputRow );
    } catch ( HopException e ) {
      boolean sendToErrorRow = false;
      String errorMessage = null;

      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        throw new HopTransformException( BaseMessages.getString( PKG, "RegexEval.Log.ErrorInTransform" ), e );
      }

      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), outputRow, 1, errorMessage, null, "REGEX001" );
      }
    }
    return true;
  }

  public boolean init() {

    if ( super.init() ) {
      // Embedded options
      String options = meta.getRegexOptions();

      // Regular expression
      String regularexpression = meta.getScript();
      if ( meta.isUseVariableInterpolationFlagSet() ) {
        regularexpression = resolve( meta.getScript() );
      }
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "RegexEval.Log.Regexp" ) + " " + options + regularexpression );
      }

      if ( meta.isCanonicalEqualityFlagSet() ) {
        data.pattern = Pattern.compile( options + regularexpression, Pattern.CANON_EQ );
      } else {
        data.pattern = Pattern.compile( options + regularexpression );
      }
      return true;
    }
    return false;
  }

}
