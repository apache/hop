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

package org.apache.hop.pipeline.transforms.ifnull;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Sets a field value to a constant if it is null
 *
 * @author Samatar
 * @since 30-06-2008
 */

public class IfNull extends BaseTransform<IfNullMeta,IfNullData> implements ITransform<IfNullMeta,IfNullData> {

  private static final Class<?> PKG = IfNullMeta.class; // For Translator

  public IfNull( TransformMeta transformMeta, IfNullMeta meta, IfNullData data , int copyNr, PipelineMeta pipelineMeta,
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

      // What's the format of the output row?
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
      // Create convert meta-data objects that will contain Date & Number formatters
      data.convertRowMeta = data.outputRowMeta.clone();

      // For String to <type> conversions, we allocate a conversion meta data row as well...
      //
      data.convertRowMeta = data.outputRowMeta.cloneToType( IValueMeta.TYPE_STRING );

      if ( meta.isSelectFields() ) {
        // Consider only selected fields
        if ( meta.getFields() != null && meta.getFields().length > 0 ) {
          int fieldsLength = meta.getFields().length;
          data.fieldnrs = new int[ fieldsLength ];
          data.defaultValues = new String[ fieldsLength ];
          data.defaultMasks = new String[ fieldsLength ];
          data.setEmptyString = new boolean[ fieldsLength ];

          for ( int i = 0; i < meta.getFields().length; i++ ) {
            data.fieldnrs[ i ] = data.outputRowMeta.indexOfValue( meta.getFields()[ i ].getFieldName() );
            if ( data.fieldnrs[ i ] < 0 ) {
              logError( BaseMessages.getString( PKG, "IfNull.Log.CanNotFindField", meta.getFields()[ i ]
                .getFieldName() ) );
              throw new HopException( BaseMessages.getString( PKG, "IfNull.Log.CanNotFindField", meta.getFields()[ i ]
                .getFieldName() ) );
            }
            data.defaultValues[ i ] = resolve( meta.getFields()[ i ].getReplaceValue() );
            data.defaultMasks[ i ] = resolve( meta.getFields()[ i ].getReplaceMask() );
            data.setEmptyString[ i ] = meta.getFields()[ i ].isSetEmptyString();
          }
        } else {
          throw new HopException( BaseMessages.getString( PKG, "IfNull.Log.SelectFieldsEmpty" ) );
        }
      } else if ( meta.isSelectValuesType() ) {
        // Consider only select value types
        if ( meta.getValueTypes() != null && meta.getValueTypes().length > 0 ) {
          // return the real default values
          int typeLength = meta.getValueTypes().length;
          data.defaultValues = new String[ typeLength ];
          data.defaultMasks = new String[ typeLength ];
          data.setEmptyString = new boolean[ typeLength ];

          // return all type codes
          HashSet<String> AlllistTypes = new HashSet<>();
          for ( int i = 0; i < IValueMeta.typeCodes.length; i++ ) {
            AlllistTypes.add( IValueMeta.typeCodes[ i ] );
          }

          for ( int i = 0; i < meta.getValueTypes().length; i++ ) {
            if ( !AlllistTypes.contains( meta.getValueTypes()[ i ].getTypeName() ) ) {
              throw new HopException( BaseMessages.getString( PKG, "IfNull.Log.CanNotFindValueType", meta
                .getValueTypes()[ i ].getTypeName() ) );
            }

            data.ListTypes.put( meta.getValueTypes()[ i ].getTypeName(), i );
            data.defaultValues[ i ] = resolve( meta.getValueTypes()[ i ].getTypereplaceValue() );
            data.defaultMasks[ i ] = resolve( meta.getValueTypes()[ i ].getTypereplaceMask() );
            data.setEmptyString[ i ] = meta.getValueTypes()[ i ].isSetTypeEmptyString();
          }

          HashSet<Integer> fieldsSelectedIndex = new HashSet<>();
          for ( int i = 0; i < data.outputRowMeta.size(); i++ ) {
            IValueMeta fieldMeta = data.outputRowMeta.getValueMeta( i );
            if ( data.ListTypes.containsKey( fieldMeta.getTypeDesc() ) ) {
              fieldsSelectedIndex.add( i );
            }
          }
          data.fieldnrs = new int[ fieldsSelectedIndex.size() ];
          List<Integer> entries = new ArrayList<>( fieldsSelectedIndex );
          Integer[] fieldnr = entries.toArray( new Integer[ entries.size() ] );
          for ( int i = 0; i < fieldnr.length; i++ ) {
            data.fieldnrs[ i ] = fieldnr[ i ];
          }
        } else {
          throw new HopException( BaseMessages.getString( PKG, "IfNull.Log.SelectValueTypesEmpty" ) );
        }

      } else {
        data.realReplaceByValue = resolve( meta.getReplaceAllByValue() );
        data.realconversionMask = resolve( meta.getReplaceAllMask() );
        data.realSetEmptyString = meta.isSetEmptyStringAll();

        // Consider all fields in input stream
        data.fieldnrs = new int[ data.outputRowMeta.size() ];
        for ( int i = 0; i < data.outputRowMeta.size(); i++ ) {
          data.fieldnrs[ i ] = i;
        }
      }
      data.fieldnr = data.fieldnrs.length;
    } // end if first

    try {
      updateFields( r );

      putRow( data.outputRowMeta, r ); // copy row to output rowset(s);

    } catch ( Exception e ) {
      boolean sendToErrorRow = false;
      String errorMessage = null;

      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "IfNull.Log.ErrorInTransform", e.getMessage() ) );
        e.printStackTrace();
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( data.outputRowMeta, r, 1, errorMessage, null, "IFNULL001" );
      }
    }
    return true;
  }

  private void updateFields( Object[] r ) throws Exception {
    // Loop through fields
    for ( int i = 0; i < data.fieldnr; i++ ) {
      IValueMeta sourceValueMeta = data.convertRowMeta.getValueMeta( data.fieldnrs[ i ] );
      if ( data.outputRowMeta.getValueMeta( data.fieldnrs[ i ] ).isNull( r[ data.fieldnrs[ i ] ] ) ) {
        if ( meta.isSelectValuesType() ) {
          IValueMeta fieldMeta = data.outputRowMeta.getValueMeta( data.fieldnrs[ i ] );
          int pos = data.ListTypes.get( fieldMeta.getTypeDesc() );

          replaceNull(
            r, sourceValueMeta, data.fieldnrs[ i ], data.defaultValues[ pos ], data.defaultMasks[ pos ],
            data.setEmptyString[ pos ] );
        } else if ( meta.isSelectFields() ) {
          replaceNull(
            r, sourceValueMeta, data.fieldnrs[ i ], data.defaultValues[ i ], data.defaultMasks[ i ],
            data.setEmptyString[ i ] );
        } else { // all
          if ( data.outputRowMeta.getValueMeta( data.fieldnrs[ i ] ).isDate() ) {
            replaceNull(
              r, sourceValueMeta, data.fieldnrs[ i ], data.realReplaceByValue, data.realconversionMask, false );
          } else { // don't use any special date format when not a date
            replaceNull(
              r, sourceValueMeta, data.fieldnrs[ i ], data.realReplaceByValue, null, data.realSetEmptyString );
          }
        }

      }
    }
  }

  public void replaceNull( Object[] row, IValueMeta sourceValueMeta, int i, String realReplaceByValue,
                           String realconversionMask, boolean setEmptystring ) throws Exception {
    if ( setEmptystring ) {
      row[ i ] = StringUtil.EMPTY_STRING;
    } else {
      // DO CONVERSION OF THE DEFAULT VALUE ...
      // Entered by user
      IValueMeta targetValueMeta = data.outputRowMeta.getValueMeta( i );
      if ( !Utils.isEmpty( realconversionMask ) ) {
        sourceValueMeta.setConversionMask( realconversionMask );
      }
      row[ i ] = targetValueMeta.convertData( sourceValueMeta, realReplaceByValue );
    }
  }

  public boolean init() {

    if ( super.init() ) {
      // Add init code here.
      return true;
    }
    return false;
  }

}
