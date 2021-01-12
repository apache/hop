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

package org.apache.hop.pipeline.transforms.stringoperations;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.ValueDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Apply certain operations too string.
 *
 * @author Samatar Hassan
 * @since 02 April 2009
 */
public class StringOperations extends BaseTransform<StringOperationsMeta, StringOperationsData> implements ITransform<StringOperationsMeta, StringOperationsData> {
  private static final Class<?> PKG = StringOperationsMeta.class; // For Translator


  public StringOperations( TransformMeta transformMeta, StringOperationsMeta meta, StringOperationsData data, int copyNr, PipelineMeta pipelineMeta,
                           Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private String processString( String string, int trimType, int lowerUpper, int padType, String padChar, int padLen,
                                int iniCap, int maskHTML, int digits, int removeSpecialCharacters ) {
    String rcode = string;

    // Trim ?
    if ( !Utils.isEmpty( rcode ) ) {
      switch ( trimType ) {
        case StringOperationsMeta.TRIM_RIGHT:
          rcode = Const.rtrim( rcode );
          break;
        case StringOperationsMeta.TRIM_LEFT:
          rcode = Const.ltrim( rcode );
          break;
        case StringOperationsMeta.TRIM_BOTH:
          rcode = Const.trim( rcode );
          break;
        default:
          break;
      }
    }
    // Lower/Upper ?
    if ( !Utils.isEmpty( rcode ) ) {
      switch ( lowerUpper ) {
        case StringOperationsMeta.LOWER_UPPER_LOWER:
          rcode = rcode.toLowerCase();
          break;
        case StringOperationsMeta.LOWER_UPPER_UPPER:
          rcode = rcode.toUpperCase();
          break;
        default:
          break;
      }
    }

    // pad String?
    if ( !Utils.isEmpty( rcode ) ) {
      switch ( padType ) {
        case StringOperationsMeta.PADDING_LEFT:
          rcode = Const.Lpad( rcode, padChar, padLen );
          break;
        case StringOperationsMeta.PADDING_RIGHT:
          rcode = Const.Rpad( rcode, padChar, padLen );
          break;
        default:
          break;
      }
    }

    // InitCap ?
    if ( !Utils.isEmpty( rcode ) ) {
      switch ( iniCap ) {
        case StringOperationsMeta.INIT_CAP_NO:
          break;
        case StringOperationsMeta.INIT_CAP_YES:
          rcode = ValueDataUtil.initCap( null, rcode );
          break;
        default:
          break;
      }
    }

    // escape ?
    if ( !Utils.isEmpty( rcode ) ) {
      switch ( maskHTML ) {
        case StringOperationsMeta.MASK_ESCAPE_XML:
          rcode = Const.escapeXml( rcode );
          break;
        case StringOperationsMeta.MASK_CDATA:
          rcode = Const.protectXmlCdata( rcode );
          break;
        case StringOperationsMeta.MASK_UNESCAPE_XML:
          rcode = Const.unEscapeXml( rcode );
          break;
        case StringOperationsMeta.MASK_ESCAPE_HTML:
          rcode = Const.escapeHtml( rcode );
          break;
        case StringOperationsMeta.MASK_UNESCAPE_HTML:
          rcode = Const.unEscapeHtml( rcode );
          break;
        case StringOperationsMeta.MASK_ESCAPE_SQL:
          rcode = Const.escapeSql( rcode );
          break;
        default:
          break;
      }
    }
    // digits only or remove digits ?
    if ( !Utils.isEmpty( rcode ) ) {
      switch ( digits ) {
        case StringOperationsMeta.DIGITS_NONE:
          break;
        case StringOperationsMeta.DIGITS_ONLY:
          rcode = Const.getDigitsOnly( rcode );
          break;
        case StringOperationsMeta.DIGITS_REMOVE:
          rcode = Const.removeDigits( rcode );
          break;
        default:
          break;
      }
    }

    // remove special characters ?
    if ( !Utils.isEmpty( rcode ) ) {
      switch ( removeSpecialCharacters ) {
        case StringOperationsMeta.REMOVE_SPECIAL_CHARACTERS_NONE:
          break;
        case StringOperationsMeta.REMOVE_SPECIAL_CHARACTERS_CR:
          rcode = Const.removeCR( rcode );
          break;
        case StringOperationsMeta.REMOVE_SPECIAL_CHARACTERS_LF:
          rcode = Const.removeLF( rcode );
          break;
        case StringOperationsMeta.REMOVE_SPECIAL_CHARACTERS_CRLF:
          rcode = Const.removeCRLF( rcode );
          break;
        case StringOperationsMeta.REMOVE_SPECIAL_CHARACTERS_TAB:
          rcode = Const.removeTAB( rcode );
          break;
        case StringOperationsMeta.REMOVE_SPECIAL_CHARACTERS_ESPACE:
          rcode = rcode.replace( " ", "" );
          break;
        default:
          break;
      }
    }

    return rcode;
  }

  private Object[] processRow(IRowMeta rowMeta, Object[] row) throws HopException {

    Object[] RowData = new Object[ data.outputRowMeta.size() ];
    // Copy the input fields.
    System.arraycopy( row, 0, RowData, 0, rowMeta.size() );
    int j = 0; // Index into "new fields" area, past the first {data.inputFieldsNr} records
    for ( int i = 0; i < data.nrFieldsInStream; i++ ) {
      if ( data.inStreamNrs[ i ] >= 0 ) {
        // Get source value
        String value = getInputRowMeta().getString( row, data.inStreamNrs[ i ] );
        // Apply String operations and return result value
        value =
          processString( value, data.trimOperators[ i ], data.lowerUpperOperators[ i ], data.padType[ i ], data.padChar[ i ],
            data.padLen[ i ], data.initCap[ i ], data.maskHTML[ i ], data.digits[ i ], data.removeSpecialCharacters[ i ] );
        if ( Utils.isEmpty( data.outStreamNrs[ i ] ) ) {
          // Update field
          RowData[ data.inStreamNrs[ i ] ] = value;
          data.outputRowMeta.getValueMeta( data.inStreamNrs[ i ] )
            .setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
        } else {
          // create a new Field
          RowData[ data.inputFieldsNr + j ] = value;
          j++;
        }
      }
    }
    return RowData;
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if ( r == null ) {
      // no more input to be expected...
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      // What's the format of the output row?
      data.outputRowMeta = getInputRowMeta().clone();
      data.inputFieldsNr = data.outputRowMeta.size();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
      data.nrFieldsInStream = meta.getFieldInStream().length;
      data.inStreamNrs = new int[ data.nrFieldsInStream ];
      for ( int i = 0; i < meta.getFieldInStream().length; i++ ) {
        data.inStreamNrs[ i ] = getInputRowMeta().indexOfValue( meta.getFieldInStream()[ i ] );
        if ( data.inStreamNrs[ i ] < 0 ) { // couldn't find field!

          throw new HopTransformException( BaseMessages.getString( PKG, "StringOperations.Exception.FieldRequired", meta
            .getFieldInStream()[ i ] ) );
        }
        // check field type
        if ( !getInputRowMeta().getValueMeta( data.inStreamNrs[ i ] ).isString() ) {
          throw new HopTransformException( BaseMessages.getString( PKG, "StringOperations.Exception.FieldTypeNotString",
            meta.getFieldInStream()[ i ] ) );
        }
      }

      data.outStreamNrs = new String[ data.nrFieldsInStream ];
      for ( int i = 0; i < meta.getFieldInStream().length; i++ ) {
        data.outStreamNrs[ i ] = meta.getFieldOutStream()[ i ];
      }

      // Keep track of the trim operators locally for a very small
      // optimization.
      data.trimOperators = new int[ data.nrFieldsInStream ];
      for ( int i = 0; i < meta.getFieldInStream().length; i++ ) {
        data.trimOperators[ i ] = meta.getTrimType()[ i ];
      }
      // lower Upper
      data.lowerUpperOperators = new int[ data.nrFieldsInStream ];
      for ( int i = 0; i < meta.getFieldInStream().length; i++ ) {
        data.lowerUpperOperators[ i ] = meta.getLowerUpper()[ i ];
      }

      // padding type?
      data.padType = new int[ data.nrFieldsInStream ];
      for ( int i = 0; i < meta.getFieldInStream().length; i++ ) {
        data.padType[ i ] = meta.getPaddingType()[ i ];
      }

      // padding char
      data.padChar = new String[ data.nrFieldsInStream ];
      for ( int i = 0; i < meta.getFieldInStream().length; i++ ) {
        data.padChar[ i ] = resolve( meta.getPadChar()[ i ] );
      }

      // padding len
      data.padLen = new int[ data.nrFieldsInStream ];
      for ( int i = 0; i < meta.getFieldInStream().length; i++ ) {
        data.padLen[ i ] = Const.toInt( resolve( meta.getPadLen()[ i ] ), 0 );
      }
      // InitCap?
      data.initCap = new int[ data.nrFieldsInStream ];
      for ( int i = 0; i < meta.getFieldInStream().length; i++ ) {
        data.initCap[ i ] = meta.getInitCap()[ i ];
      }
      // MaskXML?
      data.maskHTML = new int[ data.nrFieldsInStream ];
      for ( int i = 0; i < meta.getFieldInStream().length; i++ ) {
        data.maskHTML[ i ] = meta.getMaskXML()[ i ];
      }
      // digits?
      data.digits = new int[ data.nrFieldsInStream ];
      for ( int i = 0; i < meta.getFieldInStream().length; i++ ) {
        data.digits[ i ] = meta.getDigits()[ i ];
      }
      // remove special characters?
      data.removeSpecialCharacters = new int[ data.nrFieldsInStream ];
      for ( int i = 0; i < meta.getFieldInStream().length; i++ ) {
        data.removeSpecialCharacters[ i ] = meta.getRemoveSpecialCharacters()[ i ];
      }

    } // end if first

    try {
      Object[] output = processRow(getInputRowMeta(), r);

      putRow( data.outputRowMeta, output );

      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "StringOperations.Log.LineNumber" ) + getLinesRead() );
        }
      }
    } catch ( HopException e ) {

      boolean sendToErrorRow = false;
      String errorMessage = null;

      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "StringOperations.Log.ErrorInTransform", e.getMessage() ) );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, null, "StringOperations001" );
      }
    }
    return true;
  }

  public boolean init() {
    boolean rCode = true;

    if ( super.init() ) {

      return rCode;
    }
    return false;
  }

  public void dispose() {

    super.dispose();
  }
}
