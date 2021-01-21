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

package org.apache.hop.pipeline.transforms.calculator;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileNotFoundException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.ValueDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * Calculate new field values using pre-defined functions.
 *
 * @author Matt
 * @since 8-sep-2005
 */
public class Calculator extends BaseTransform<CalculatorMeta, CalculatorData> implements ITransform<CalculatorMeta, CalculatorData> {

  private static final Class<?> PKG = CalculatorMeta.class; // For Translator

  public class FieldIndexes {
    public int indexName;
    public int indexA;
    public int indexB;
    public int indexC;
  }


  public Calculator( TransformMeta transformMeta, CalculatorMeta meta, CalculatorData data, int copyNr, PipelineMeta pipelineMeta,
                     Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) { // no more input to be expected...
      setOutputDone();
      data.clearValuesMetaMapping();
      return false;
    }

    if ( first ) {
      first = false;
      data.setOutputRowMeta( getInputRowMeta().clone() );
      meta.getFields( data.getOutputRowMeta(), getTransformName(), null, null, this, metadataProvider );

      // get all metadata, including source rows and temporary fields.
      data.setCalcRowMeta( meta.getAllFields( getInputRowMeta() ) );

      data.setFieldIndexes( new FieldIndexes[ meta.getCalculation().length ] );
      List<Integer> tempIndexes = new ArrayList<>();

      // Calculate the indexes of the values and arguments in the target data or temporary data
      // We do this in advance to save time later on.
      //
      //CHECKSTYLE:Indentation:OFF
      for ( int i = 0; i < meta.getCalculation().length; i++ ) {
        CalculatorMetaFunction function = meta.getCalculation()[ i ];
        data.getFieldIndexes()[ i ] = new FieldIndexes();

        if ( !Utils.isEmpty( function.getFieldName() ) ) {
          data.getFieldIndexes()[ i ].indexName = data.getCalcRowMeta().indexOfValue( function.getFieldName() );
          if ( data.getFieldIndexes()[ i ].indexName < 0 ) {
            // Nope: throw an exception
            throw new HopTransformException( BaseMessages.getString(
              PKG, "Calculator.Error.UnableFindField", function.getFieldName(), "" + ( i + 1 ) ) );
          }
        } else {
          throw new HopTransformException( BaseMessages.getString( PKG, "Calculator.Error.NoNameField", ""
            + ( i + 1 ) ) );
        }

        if ( !Utils.isEmpty( function.getFieldA() ) ) {
          if ( function.getCalcType() != CalculatorMetaFunction.CALC_CONSTANT ) {
            data.getFieldIndexes()[ i ].indexA = data.getCalcRowMeta().indexOfValue( function.getFieldA() );
            if ( data.getFieldIndexes()[ i ].indexA < 0 ) {
              // Nope: throw an exception
              throw new HopTransformException( "Unable to find the first argument field '"
                + function.getFieldName() + " for calculation #" + ( i + 1 ) );
            }
          } else {
            data.getFieldIndexes()[ i ].indexA = -1;
          }
        } else {
          throw new HopTransformException( "There is no first argument specified for calculated field #" + ( i + 1 ) );
        }

        if ( !Utils.isEmpty( function.getFieldB() ) ) {
          data.getFieldIndexes()[ i ].indexB = data.getCalcRowMeta().indexOfValue( function.getFieldB() );
          if ( data.getFieldIndexes()[ i ].indexB < 0 ) {
            // Nope: throw an exception
            throw new HopTransformException( "Unable to find the second argument field '"
              + function.getFieldName() + " for calculation #" + ( i + 1 ) );
          }
        }
        data.getFieldIndexes()[ i ].indexC = -1;
        if ( !Utils.isEmpty( function.getFieldC() ) ) {
          data.getFieldIndexes()[ i ].indexC = data.getCalcRowMeta().indexOfValue( function.getFieldC() );
          if ( data.getFieldIndexes()[ i ].indexC < 0 ) {
            // Nope: throw an exception
            throw new HopTransformException( "Unable to find the third argument field '"
              + function.getFieldName() + " for calculation #" + ( i + 1 ) );
          }
        }

        if ( function.isRemovedFromResult() ) {
          tempIndexes.add( getInputRowMeta().size() + i );
        }
      }

      // Convert temp indexes to int[]
      data.setTempIndexes( new int[ tempIndexes.size() ] );
      for ( int i = 0; i < data.getTempIndexes().length; i++ ) {
        data.getTempIndexes()[ i ] = tempIndexes.get( i );
      }
    }

    if ( log.isRowLevel() ) {
      logRowlevel( BaseMessages.getString( PKG, "Calculator.Log.ReadRow" )
        + getLinesRead() + " : " + getInputRowMeta().getString( r ) );
    }

    try {
      Object[] row = calcFields( getInputRowMeta(), r );
      putRow( data.getOutputRowMeta(), row ); // copy row to possible alternate rowset(s).

      if ( log.isRowLevel() ) {
        logRowlevel( "Wrote row #" + getLinesWritten() + " : " + getInputRowMeta().getString( r ) );
      }
      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "Calculator.Log.Linenr", "" + getLinesRead() ) );
        }
      }
    } catch ( HopFileNotFoundException e ) {
      if ( meta.isFailIfNoFile() ) {
        logError( BaseMessages.getString( PKG, "Calculator.Log.NoFile" ) + " : " + e.getFilepath() );
        setErrors( getErrors() + 1 );
        return false;
      }
    } catch ( HopException e ) {
      logError( BaseMessages.getString( PKG, "Calculator.ErrorInTransformRunning" + " : " + e.getMessage() ) );
      throw new HopTransformException( BaseMessages.getString( PKG, "Calculator.ErrorInTransformRunning" ), e );
    }
    return true;
  }

  /**
   * @param inputRowMeta the input row metadata
   * @param r            the input row (data)
   * @return A row including the calculations, excluding the temporary values
   * @throws HopValueException in case there is a calculation error.
   */
  private Object[] calcFields( IRowMeta inputRowMeta, Object[] r ) throws HopValueException,
    HopFileNotFoundException {
    // First copy the input data to the new result...
    Object[] calcData = RowDataUtil.resizeArray( r, data.getCalcRowMeta().size() );

    for ( int i = 0, index = inputRowMeta.size() + i; i < meta.getCalculation().length; i++, index++ ) {
      CalculatorMetaFunction fn = meta.getCalculation()[ i ];
      if ( !Utils.isEmpty( fn.getFieldName() ) ) {
        IValueMeta targetMeta = data.getCalcRowMeta().getValueMeta( index );

        // Get the metadata & the data...
        // IValueMeta metaTarget = data.calcRowMeta.getValueMeta(i);

        IValueMeta metaA = null;
        Object dataA = null;

        if ( data.getFieldIndexes()[ i ].indexA >= 0 ) {
          metaA = data.getCalcRowMeta().getValueMeta( data.getFieldIndexes()[ i ].indexA );
          dataA = calcData[ data.getFieldIndexes()[ i ].indexA ];
        }

        IValueMeta metaB = null;
        Object dataB = null;

        if ( data.getFieldIndexes()[ i ].indexB >= 0 ) {
          metaB = data.getCalcRowMeta().getValueMeta( data.getFieldIndexes()[ i ].indexB );
          dataB = calcData[ data.getFieldIndexes()[ i ].indexB ];
        }

        IValueMeta metaC = null;
        Object dataC = null;

        if ( data.getFieldIndexes()[ i ].indexC >= 0 ) {
          metaC = data.getCalcRowMeta().getValueMeta( data.getFieldIndexes()[ i ].indexC );
          dataC = calcData[ data.getFieldIndexes()[ i ].indexC ];
        }

        int calcType = fn.getCalcType();
        // The data types are those of the first argument field, convert to the target field.
        // Exceptions:
        // - multiply can be string
        // - constant is string
        // - all date functions except add days/months
        // - hex encode / decodes

        int resultType;
        if ( metaA != null ) {
          resultType = metaA.getType();
        } else {
          resultType = IValueMeta.TYPE_NONE;
        }

        switch ( calcType ) {
          case CalculatorMetaFunction.CALC_NONE:
            break;
          case CalculatorMetaFunction.CALC_COPY_OF_FIELD: // Create a copy of field A

            calcData[ index ] = dataA;

            break;
          case CalculatorMetaFunction.CALC_ADD: // A + B
            calcData[ index ] = ValueDataUtil.plus( metaA, dataA, metaB, dataB );
            if ( metaA.isString() || metaB.isString() ) {
              resultType = IValueMeta.TYPE_STRING;
            }
            break;
          case CalculatorMetaFunction.CALC_SUBTRACT: // A - B
            calcData[ index ] = ValueDataUtil.minus( metaA, dataA, metaB, dataB );
            if ( metaA.isDate() ) {
              resultType = IValueMeta.TYPE_INTEGER;
            }
            break;
          case CalculatorMetaFunction.CALC_MULTIPLY: // A * B
            calcData[ index ] = ValueDataUtil.multiply( metaA, dataA, metaB, dataB );
            if ( metaA.isString() || metaB.isString() ) {
              resultType = IValueMeta.TYPE_STRING;
            }
            break;
          case CalculatorMetaFunction.CALC_DIVIDE: // A / B
            calcData[ index ] = ValueDataUtil.divide( metaA, dataA, metaB, dataB );
            break;
          case CalculatorMetaFunction.CALC_SQUARE: // A * A
            calcData[ index ] = ValueDataUtil.multiply( metaA, dataA, metaA, dataA );
            break;
          case CalculatorMetaFunction.CALC_SQUARE_ROOT: // SQRT( A )
            calcData[ index ] = ValueDataUtil.sqrt( metaA, dataA );
            break;
          case CalculatorMetaFunction.CALC_PERCENT_1: // 100 * A / B
            calcData[ index ] = ValueDataUtil.percent1( metaA, dataA, metaB, dataB );
            break;
          case CalculatorMetaFunction.CALC_PERCENT_2: // A - ( A * B / 100 )
            calcData[ index ] = ValueDataUtil.percent2( metaA, dataA, metaB, dataB );
            break;
          case CalculatorMetaFunction.CALC_PERCENT_3: // A + ( A * B / 100 )
            calcData[ index ] = ValueDataUtil.percent3( metaA, dataA, metaB, dataB );
            break;
          case CalculatorMetaFunction.CALC_COMBINATION_1: // A + B * C
            calcData[ index ] = ValueDataUtil.combination1( metaA, dataA, metaB, dataB, metaC, dataC );
            break;
          case CalculatorMetaFunction.CALC_COMBINATION_2: // SQRT( A*A + B*B )
            calcData[ index ] = ValueDataUtil.combination2( metaA, dataA, metaB, dataB );
            break;
          case CalculatorMetaFunction.CALC_ROUND_1: // ROUND( A )
            calcData[ index ] = ValueDataUtil.round( metaA, dataA );
            break;
          case CalculatorMetaFunction.CALC_ROUND_2: // ROUND( A , B )
            calcData[ index ] = ValueDataUtil.round( metaA, dataA, metaB, dataB );
            break;
          case CalculatorMetaFunction.CALC_ROUND_CUSTOM_1: // ROUND( A , B )
            calcData[ index ] = ValueDataUtil.round( metaA, dataA, metaB.getNumber( dataB ).intValue() );
            break;
          case CalculatorMetaFunction.CALC_ROUND_CUSTOM_2: // ROUND( A , B, C )
            calcData[ index ] = ValueDataUtil.round( metaA, dataA, metaB, dataB, metaC.getNumber( dataC ).intValue() );
            break;
          case CalculatorMetaFunction.CALC_ROUND_STD_1: // ROUND( A )
            calcData[ index ] = ValueDataUtil.round( metaA, dataA, java.math.BigDecimal.ROUND_HALF_UP );
            break;
          case CalculatorMetaFunction.CALC_ROUND_STD_2: // ROUND( A , B )
            calcData[ index ] = ValueDataUtil.round( metaA, dataA, metaB, dataB, java.math.BigDecimal.ROUND_HALF_UP );
            break;
          case CalculatorMetaFunction.CALC_CEIL: // CEIL( A )
            calcData[ index ] = ValueDataUtil.ceil( metaA, dataA );
            break;
          case CalculatorMetaFunction.CALC_FLOOR: // FLOOR( A )
            calcData[ index ] = ValueDataUtil.floor( metaA, dataA );
            break;
          case CalculatorMetaFunction.CALC_CONSTANT: // Set field to constant value...
            calcData[ index ] = fn.getFieldA(); // A string
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_NVL: // Replace null values with another value
            calcData[ index ] = ValueDataUtil.nvl( metaA, dataA, metaB, dataB );
            break;
          case CalculatorMetaFunction.CALC_ADD_DAYS: // Add B days to date field A
            calcData[ index ] = ValueDataUtil.addDays( metaA, dataA, metaB, dataB );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_ADD_HOURS: // Add B hours to date field A
            calcData[ index ] = ValueDataUtil.addHours( metaA, dataA, metaB, dataB );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_ADD_MINUTES: // Add B minutes to date field A
            calcData[ index ] = ValueDataUtil.addMinutes( metaA, dataA, metaB, dataB );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_YEAR_OF_DATE: // What is the year (Integer) of a date?
            calcData[ index ] = ValueDataUtil.yearOfDate( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_MONTH_OF_DATE: // What is the month (Integer) of a date?
            calcData[ index ] = ValueDataUtil.monthOfDate( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_DAY_OF_YEAR: // What is the day of year (Integer) of a date?
            calcData[ index ] = ValueDataUtil.dayOfYear( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_DAY_OF_MONTH: // What is the day of month (Integer) of a date?
            calcData[ index ] = ValueDataUtil.dayOfMonth( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_DAY_OF_WEEK: // What is the day of week (Integer) of a date?
            calcData[ index ] = ValueDataUtil.dayOfWeek( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_WEEK_OF_YEAR: // What is the week of year (Integer) of a date?
            calcData[ index ] = ValueDataUtil.weekOfYear( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_WEEK_OF_YEAR_ISO8601: // What is the week of year (Integer) of a date ISO8601
            // style?
            calcData[ index ] = ValueDataUtil.weekOfYearISO8601( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_YEAR_OF_DATE_ISO8601: // What is the year (Integer) of a date ISO8601 style?
            calcData[ index ] = ValueDataUtil.yearOfDateISO8601( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_BYTE_TO_HEX_ENCODE: // Byte to Hex encode string field A
            calcData[ index ] = ValueDataUtil.byteToHexEncode( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_HEX_TO_BYTE_DECODE: // Hex to Byte decode string field A
            calcData[ index ] = ValueDataUtil.hexToByteDecode( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;

          case CalculatorMetaFunction.CALC_CHAR_TO_HEX_ENCODE: // Char to Hex encode string field A
            calcData[ index ] = ValueDataUtil.charToHexEncode( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_HEX_TO_CHAR_DECODE: // Hex to Char decode string field A
            calcData[ index ] = ValueDataUtil.hexToCharDecode( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_CRC32: // CRC32
            calcData[ index ] = ValueDataUtil.checksumCRC32( metaA, dataA, meta.isFailIfNoFile() );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_ADLER32: // ADLER32
            calcData[ index ] = ValueDataUtil.checksumAdler32( metaA, dataA, meta.isFailIfNoFile() );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_MD5: // MD5
            calcData[ index ] = ValueDataUtil.createChecksum( metaA, dataA, "MD5", meta.isFailIfNoFile() );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_SHA1: // SHA-1
            calcData[ index ] = ValueDataUtil.createChecksum( metaA, dataA, "SHA-1", meta.isFailIfNoFile() );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_LEVENSHTEIN_DISTANCE: // LEVENSHTEIN DISTANCE
            calcData[ index ] = ValueDataUtil.getLevenshtein_Distance( metaA, dataA, metaB, dataB );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_METAPHONE: // METAPHONE
            calcData[ index ] = ValueDataUtil.get_Metaphone( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_DOUBLE_METAPHONE: // Double METAPHONE
            calcData[ index ] = ValueDataUtil.get_Double_Metaphone( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_ABS: // ABS( A )
            calcData[ index ] = ValueDataUtil.abs( metaA, dataA );
            break;
          case CalculatorMetaFunction.CALC_REMOVE_TIME_FROM_DATE: // Remove Time from field A
            calcData[ index ] = ValueDataUtil.removeTimeFromDate( metaA, dataA );
            break;
          case CalculatorMetaFunction.CALC_DATE_DIFF: // DateA - DateB
            calcData[ index ] = ValueDataUtil.DateDiff( metaA, dataA, metaB, dataB, "d" );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_ADD3: // A + B + C
            calcData[ index ] = ValueDataUtil.plus3( metaA, dataA, metaB, dataB, metaC, dataC );
            if ( metaA.isString() || metaB.isString() || metaC.isString() ) {
              resultType = IValueMeta.TYPE_STRING;
            }
            break;
          case CalculatorMetaFunction.CALC_INITCAP: // InitCap( A )
            calcData[ index ] = ValueDataUtil.initCap( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_UPPER_CASE: // UpperCase( A )
            calcData[ index ] = ValueDataUtil.upperCase( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_LOWER_CASE: // UpperCase( A )
            calcData[ index ] = ValueDataUtil.lowerCase( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_MASK_XML: // escapeXML( A )
            calcData[ index ] = ValueDataUtil.escapeXml( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_USE_CDATA: // CDATA( A )
            calcData[ index ] = ValueDataUtil.useCDATA( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_REMOVE_CR: // REMOVE CR FROM A
            calcData[ index ] = ValueDataUtil.removeCR( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_REMOVE_LF: // REMOVE LF FROM A
            calcData[ index ] = ValueDataUtil.removeLF( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_REMOVE_CRLF: // REMOVE CRLF FROM A
            calcData[ index ] = ValueDataUtil.removeCRLF( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_REMOVE_TAB: // REMOVE TAB FROM A
            calcData[ index ] = ValueDataUtil.removeTAB( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_GET_ONLY_DIGITS: // GET ONLY DIGITS FROM A
            calcData[ index ] = ValueDataUtil.getDigits( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_REMOVE_DIGITS: // REMOVE DIGITS FROM A
            calcData[ index ] = ValueDataUtil.removeDigits( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_STRING_LEN: // RETURN THE LENGTH OF A
            calcData[ index ] = ValueDataUtil.stringLen( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_LOAD_FILE_CONTENT_BINARY: // LOAD CONTENT OF A FILE A IN A BLOB
            calcData[ index ] = ValueDataUtil.loadFileContentInBinary( metaA, dataA, meta.isFailIfNoFile() );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_ADD_TIME_TO_DATE: // Add time B to a date A
            calcData[ index ] = ValueDataUtil.addTimeToDate( metaA, dataA, metaB, dataB, metaC, dataC );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_QUARTER_OF_DATE: // What is the quarter (Integer) of a date?
            calcData[ index ] = ValueDataUtil.quarterOfDate( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_SUBSTITUTE_VARIABLE: // variable substitution in string
            calcData[ index ] = resolve( dataA.toString() );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_UNESCAPE_XML: // UnescapeXML( A )
            calcData[ index ] = ValueDataUtil.unEscapeXml( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_ESCAPE_HTML: // EscapeHTML( A )
            calcData[ index ] = ValueDataUtil.escapeHtml( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_UNESCAPE_HTML: // UnescapeHTML( A )
            calcData[ index ] = ValueDataUtil.unEscapeHtml( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_ESCAPE_SQL: // EscapeSQL( A )
            calcData[ index ] = ValueDataUtil.escapeSql( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_DATE_WORKING_DIFF: // DateWorkingDiff( A , B)
            calcData[ index ] = ValueDataUtil.DateWorkingDiff( metaA, dataA, metaB, dataB );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_ADD_MONTHS: // Add B months to date field A
            calcData[ index ] = ValueDataUtil.addMonths( metaA, dataA, metaB, dataB );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_CHECK_XML_FILE_WELL_FORMED: // Check if file A is well formed
            calcData[ index ] = ValueDataUtil.isXmlFileWellFormed( metaA, dataA, meta.isFailIfNoFile() );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_CHECK_XML_WELL_FORMED: // Check if xml A is well formed
            calcData[ index ] = ValueDataUtil.isXmlWellFormed( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_GET_FILE_ENCODING: // Get file encoding from a file A
            calcData[ index ] = ValueDataUtil.getFileEncoding( metaA, dataA, meta.isFailIfNoFile() );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_DAMERAU_LEVENSHTEIN: // DAMERAULEVENSHTEIN DISTANCE
            calcData[ index ] = ValueDataUtil.getDamerauLevenshtein_Distance( metaA, dataA, metaB, dataB );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_NEEDLEMAN_WUNSH: // NEEDLEMANWUNSH DISTANCE
            calcData[ index ] = CalculatorValueDataUtil.getNeedlemanWunschDistance( dataA, dataB );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_JARO: // Jaro DISTANCE
            calcData[ index ] = ValueDataUtil.getJaro_Similitude( metaA, dataA, metaB, dataB );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_JARO_WINKLER: // Jaro DISTANCE
            calcData[ index ] = ValueDataUtil.getJaroWinkler_Similitude( metaA, dataA, metaB, dataB );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_SOUNDEX: // SOUNDEX
            calcData[ index ] = ValueDataUtil.get_SoundEx( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_REFINED_SOUNDEX: // REFINEDSOUNDEX
            calcData[ index ] = ValueDataUtil.get_RefinedSoundEx( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_DATE_DIFF_MSEC: // DateA - DateB (ms)
            calcData[ index ] = ValueDataUtil.DateDiff( metaA, dataA, metaB, dataB, "ms" );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_DATE_DIFF_SEC: // DateA - DateB (s)
            calcData[ index ] = ValueDataUtil.DateDiff( metaA, dataA, metaB, dataB, "s" );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_DATE_DIFF_MN: // DateA - DateB (mn)
            calcData[ index ] = ValueDataUtil.DateDiff( metaA, dataA, metaB, dataB, "mn" );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_DATE_DIFF_HR: // DateA - DateB (h)
            calcData[ index ] = ValueDataUtil.DateDiff( metaA, dataA, metaB, dataB, "h" );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_HOUR_OF_DAY:
            calcData[ index ] = ValueDataUtil.hourOfDay( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_MINUTE_OF_HOUR:
            calcData[ index ] = ValueDataUtil.minuteOfHour( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_SECOND_OF_MINUTE:
            calcData[ index ] = ValueDataUtil.secondOfMinute( metaA, dataA );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_ADD_SECONDS: // Add B seconds to date field A
            calcData[ index ] = ValueDataUtil.addSeconds( metaA, dataA, metaB, dataB );
            resultType = CalculatorMetaFunction.calcDefaultResultType[ calcType ];
            break;
          case CalculatorMetaFunction.CALC_REMAINDER:
            if ( targetMeta.getType() != metaA.getType() || targetMeta.getType() != metaB.getType() ) {
              dataA = targetMeta.convertData( metaA, dataA );
              metaA = targetMeta.clone();
              dataB = targetMeta.convertData( metaB, dataB );
              metaB = targetMeta.clone();
            }
            calcData[ index ] = ValueDataUtil.remainder( metaA, dataA, metaB, dataB );
            resultType = targetMeta.getType();
            break;
          default:
            throw new HopValueException( BaseMessages.getString( PKG, "Calculator.Log.UnknownCalculationType" )
              + fn.getCalcType() );
        }

        // If we don't have a target data type, throw an error.
        // Otherwise the result is non-deterministic.
        //
        if ( targetMeta.getType() == IValueMeta.TYPE_NONE ) {
          throw new HopValueException( BaseMessages.getString( PKG, "Calculator.Log.NoType" )
            + ( i + 1 ) + " : " + fn.getFieldName() + " = " + fn.getCalcTypeDesc() + " / "
            + fn.getCalcTypeLongDesc() );
        }

        // Convert the data to the correct target data type.
        //
        if ( calcData[ index ] != null ) {
          if ( targetMeta.getType() != resultType ) {
            IValueMeta resultMeta;
            try {
              // clone() is not necessary as one data instance belongs to one transform instance and no race condition occurs
              resultMeta = data.getValueMetaFor( resultType, "result" );
            } catch ( Exception exception ) {
              throw new HopValueException( "Error creating value" );
            }
            resultMeta.setConversionMask( fn.getConversionMask() );
            resultMeta.setGroupingSymbol( fn.getGroupingSymbol() );
            resultMeta.setDecimalSymbol( fn.getDecimalSymbol() );
            resultMeta.setCurrencySymbol( fn.getCurrencySymbol() );
            try {
              calcData[ index ] = targetMeta.convertData( resultMeta, calcData[ index ] );
            } catch ( Exception ex ) {
              throw new HopValueException( "resultType: "
                + resultType + "; targetMeta: " + targetMeta.getType(), ex );
            }
          }
        }
      }
    }

    // OK, now we should refrain from adding the temporary fields to the result.
    // So we remove them.
    //
    return RowDataUtil.removeItems( calcData, data.getTempIndexes() );
  }
}
