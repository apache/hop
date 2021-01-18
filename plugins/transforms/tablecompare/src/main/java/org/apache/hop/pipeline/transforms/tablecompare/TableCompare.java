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

package org.apache.hop.pipeline.transforms.tablecompare;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.sql.ResultSet;

/**
 * @author Matt
 * @since 19-11-2009
 */

public class TableCompare extends BaseTransform<TableCompareMeta, TableCompareData> implements ITransform<TableCompareMeta, TableCompareData> {
  private static final Class<?> PKG = TableCompare.class; // For Translator

  public TableCompare( TransformMeta transformMeta, TableCompareMeta meta, TableCompareData data, int copyNr, PipelineMeta pipelineMeta,
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
      //
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Reference schema
      //
      if ( Utils.isEmpty( meta.getReferenceSchemaField() ) ) {
        throw new HopException( BaseMessages.getString(
          PKG, "TableCompare.Exception.ReferenceSchemaNotSpecified" ) );
      }
      data.refSchemaIndex = getInputRowMeta().indexOfValue( meta.getReferenceSchemaField() );
      if ( data.refSchemaIndex < 0 ) {
        throw new HopException( BaseMessages.getString( PKG, "TableCompare.Exception.CanNotFindField", meta
          .getReferenceSchemaField() ) );
      }

      // Reference table
      //
      if ( Utils.isEmpty( meta.getReferenceTableField() ) ) {
        throw new HopException( BaseMessages.getString(
          PKG, "TableCompare.Exception.ReferenceTableNotSpecified" ) );
      }
      data.refTableIndex = getInputRowMeta().indexOfValue( meta.getReferenceTableField() );
      if ( data.refTableIndex < 0 ) {
        throw new HopException( BaseMessages.getString( PKG, "TableCompare.Exception.CanNotFindField", meta
          .getReferenceTableField() ) );
      }

      // Compare schema
      //
      if ( Utils.isEmpty( meta.getCompareSchemaField() ) ) {
        throw new HopException( BaseMessages
          .getString( PKG, "TableCompare.Exception.CompareSchemaNotSpecified" ) );
      }
      data.cmpSchemaIndex = getInputRowMeta().indexOfValue( meta.getCompareSchemaField() );
      if ( data.cmpSchemaIndex < 0 ) {
        throw new HopException( BaseMessages.getString( PKG, "TableCompare.Exception.CanNotFindField", meta
          .getCompareSchemaField() ) );
      }

      // Compare table
      //
      if ( Utils.isEmpty( meta.getCompareTableField() ) ) {
        throw new HopException( BaseMessages.getString( PKG, "TableCompare.Exception.CompareTableNotSpecified" ) );
      }
      data.cmpTableIndex = getInputRowMeta().indexOfValue( meta.getCompareTableField() );
      if ( data.cmpTableIndex < 0 ) {
        throw new HopException( BaseMessages.getString( PKG, "TableCompare.Exception.CanNotFindField", meta
          .getCompareTableField() ) );
      }

      // Key fields
      //
      if ( Utils.isEmpty( meta.getKeyFieldsField() ) ) {
        throw new HopException( BaseMessages.getString( PKG, "TableCompare.Exception.KeyFieldsNotSpecified" ) );
      }
      data.keyFieldsIndex = getInputRowMeta().indexOfValue( meta.getKeyFieldsField() );
      if ( data.keyFieldsIndex < 0 ) {
        throw new HopException( BaseMessages.getString( PKG, "TableCompare.Exception.CanNotFindField", meta
          .getKeyFieldsField() ) );
      }

      // Exclude fields
      //
      if ( Utils.isEmpty( meta.getExcludeFieldsField() ) ) {
        throw new HopException( BaseMessages
          .getString( PKG, "TableCompare.Exception.ExcludeFieldsNotSpecified" ) );
      }
      data.excludeFieldsIndex = getInputRowMeta().indexOfValue( meta.getExcludeFieldsField() );
      if ( data.excludeFieldsIndex < 0 ) {
        throw new HopException( BaseMessages.getString( PKG, "TableCompare.Exception.CanNotFindField", meta
          .getExcludeFieldsField() ) );
      }

      // error handling: Key description
      //
      if ( Utils.isEmpty( meta.getKeyDescriptionField() ) ) {
        throw new HopException( BaseMessages.getString(
          PKG, "TableCompare.Exception.KeyDescriptionFieldNotSpecified" ) );
      }
      data.keyDescIndex = getInputRowMeta().indexOfValue( meta.getKeyDescriptionField() );
      if ( data.keyDescIndex < 0 ) {
        throw new HopException( BaseMessages.getString( PKG, "TableCompare.Exception.CanNotFindField", meta
          .getKeyDescriptionField() ) );
      }

      // error handling: reference value
      //
      if ( Utils.isEmpty( meta.getValueReferenceField() ) ) {
        throw new HopException( BaseMessages.getString(
          PKG, "TableCompare.Exception.ValueReferenceFieldNotSpecified" ) );
      }
      data.valueReferenceIndex = getInputRowMeta().indexOfValue( meta.getValueReferenceField() );
      if ( data.valueReferenceIndex < 0 ) {
        throw new HopException( BaseMessages.getString( PKG, "TableCompare.Exception.CanNotFindField", meta
          .getValueReferenceField() ) );
      }

      // error handling: compare value
      //
      if ( Utils.isEmpty( meta.getValueCompareField() ) ) {
        throw new HopException( BaseMessages.getString(
          PKG, "TableCompare.Exception.ValueCompareFieldNotSpecified" ) );
      }
      data.valueCompareIndex = getInputRowMeta().indexOfValue( meta.getValueCompareField() );
      if ( data.valueCompareIndex < 0 ) {
        throw new HopException( BaseMessages.getString( PKG, "TableCompare.Exception.CanNotFindField", meta
          .getValueCompareField() ) );
      }

    } // end if first

    Object[] fields = compareTables( getInputRowMeta(), r );
    Object[] outputRowData = RowDataUtil.addRowData( r, getInputRowMeta().size(), fields );
    putRow( data.outputRowMeta, outputRowData ); // copy row to output rowset(s);
    return true;
  }

  private Object[] compareTables( IRowMeta rowMeta, Object[] r ) throws HopException {
    try {
      String referenceSchema = getInputRowMeta().getString( r, data.refSchemaIndex );
      String referenceTable = getInputRowMeta().getString( r, data.refTableIndex );
      String compareSchema = getInputRowMeta().getString( r, data.cmpSchemaIndex );
      String compareTable = getInputRowMeta().getString( r, data.cmpTableIndex );
      String keyFields = getInputRowMeta().getString( r, data.keyFieldsIndex );
      String excludeFields = getInputRowMeta().getString( r, data.excludeFieldsIndex );

      return compareTables(
        rowMeta, r, referenceSchema, referenceTable, compareSchema, compareTable, keyFields, excludeFields );
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString(
        PKG, "TableCompare.Exception.UnexpectedErrorComparingTables" ), e );
    }
  }

  private Object[] compareTables( IRowMeta rowMeta, Object[] r, String referenceSchema,
                                  String referenceTable, String compareSchema, String compareTable, String keyFields, String excludeFields ) throws HopException {
    long nrErrors = 0L;
    long nrLeftErrors = 0L;
    long nrRightErrors = 0L;
    long nrInnerErrors = 0L;
    long nrRecordsReference = 0L;
    long nrRecordsCompare = 0L;

    Object[] result = new Object[ 6 ];

    if ( Utils.isEmpty( referenceTable ) ) {
      Object[] errorRowData = constructErrorRow( rowMeta, r, null, null, null );
      putError( data.errorRowMeta, errorRowData, 1, BaseMessages.getString(
        PKG, "TableCompare.Exception.NoReferenceTableDefined" ), null, "TAC008" );
      nrErrors++;
    }

    if ( Utils.isEmpty( compareTable ) ) {
      Object[] errorRowData = constructErrorRow( rowMeta, r, null, null, null );
      putError( data.errorRowMeta, errorRowData, 1, BaseMessages.getString(
        PKG, "TableCompare.Exception.NoCompareTableDefined" ), null, "TAC008" );
      nrErrors++;
    }

    String refSchemaTable =
      meta.getReferenceConnection().getQuotedSchemaTableCombination( this, referenceSchema, referenceTable );
    String cmpSchemaTable =
      meta.getCompareConnection().getQuotedSchemaTableCombination( this, compareSchema, compareTable );

    if ( Utils.isEmpty( keyFields ) ) {
      Object[] errorRowData = constructErrorRow( rowMeta, r, null, null, null );
      putError( data.errorRowMeta, errorRowData, 1, BaseMessages.getString(
        PKG, "TableCompare.Exception.NoKeyFieldsDefined", refSchemaTable, cmpSchemaTable ), null, "TAC007" );
      nrErrors++;
    }

    // If something is wrong here, we can't continue...
    //
    if ( nrErrors > 0 ) {
      result[ 0 ] = Long.valueOf( nrErrors );
      return result;
    }

    String[] keys = keyFields.split( "," );
    for ( int i = 0; i < keys.length; i++ ) {
      keys[ i ] = Kjube.trim( keys[ i ] );
    }
    String[] excluded = Utils.isEmpty( excludeFields ) ? new String[ 0 ] : excludeFields.split( "," );
    for ( int i = 0; i < excluded.length; i++ ) {
      excluded[ i ] = Kjube.trim( excluded[ i ] );
    }

    try {
      IRowMeta refFields = data.referenceDb.getTableFieldsMeta( referenceSchema, referenceTable );
      IRowMeta cmpFields = data.compareDb.getTableFieldsMeta( referenceSchema, referenceTable );

      // Remove the excluded fields from these fields...
      //
      for ( String field : excluded ) {
        if ( refFields.indexOfValue( field ) >= 0 ) {
          refFields.removeValueMeta( field );
        }
        if ( cmpFields.indexOfValue( field ) >= 0 ) {
          cmpFields.removeValueMeta( field );
        }
      }

      // See if the 2 tables have the same nr of fields in it...
      //
      if ( refFields.size() != cmpFields.size() ) {
        Object[] errorRowData = constructErrorRow( rowMeta, r, null, null, null );
        putError( data.errorRowMeta, errorRowData, 1, BaseMessages.getString(
          PKG, "TableCompare.Error.NumberOfFieldsIsDifferent", refSchemaTable, Integer.toString( refFields
            .size() ), cmpSchemaTable, Integer.toString( cmpFields.size() ) ), null, "TAC001" );
        nrErrors++;
      } else {
        // See if all the key fields exist in the reference & compare tables...
        //
        for ( String key : keys ) {
          if ( refFields.indexOfValue( key ) < 0 ) {
            if ( getTransformMeta().isDoingErrorHandling() ) {
              Object[] errorRowData = constructErrorRow( rowMeta, r, null, null, null );
              putError(
                data.errorRowMeta, errorRowData, 1, BaseMessages.getString(
                  PKG, "TableCompare.Error.KeyFieldWasNotFoundInReferenceTable", key, refSchemaTable ), null,
                "TAC002" );
            }
            nrErrors++;
          }
        }
        for ( String key : keys ) {
          if ( cmpFields.indexOfValue( key ) < 0 ) {
            if ( getTransformMeta().isDoingErrorHandling() ) {
              Object[] errorRowData = constructErrorRow( rowMeta, r, null, null, null );
              putError(
                data.errorRowMeta, errorRowData, 1, BaseMessages.getString(
                  PKG, "TableCompare.Error.KeyFieldWasNotFoundInCompareTable", key, refSchemaTable ), null,
                "TAC003" );
            }
            nrErrors++;
          }
        }

        // If we can't find all key fields, stop here...
        //
        if ( nrErrors > 0 ) {
          result[ 0 ] = Long.valueOf( nrErrors );
          return result;
        }

        // Now we read the data from both tables and compare keys and values...
        // First we construct the SQL
        //
        IRowMeta keyRowMeta = new RowMeta();
        IRowMeta valueRowMeta = new RowMeta();

        int[] keyNrs = new int[ keys.length ];

        String refSql = "SELECT ";
        String cmpSql = "SELECT ";
        for ( int i = 0; i < keys.length; i++ ) {
          if ( i > 0 ) {
            refSql += ", ";
            cmpSql += ", ";
          }
          keyNrs[ i ] = i;
          refSql += meta.getReferenceConnection().quoteField( keys[ i ] );
          cmpSql += meta.getReferenceConnection().quoteField( keys[ i ] );
        }
        int[] valueNrs = new int[ refFields.size() - keys.length ];
        int valueNr = keys.length;
        int valueIndex = 0;
        for ( int i = 0; i < refFields.getFieldNames().length; i++ ) {
          String field = refFields.getFieldNames()[ i ];
          if ( Const.indexOfString( field, keys ) < 0 ) {
            refSql += ", " + meta.getReferenceConnection().quoteField( field );
            valueRowMeta.addValueMeta( refFields.searchValueMeta( field ) );
            valueNrs[ valueIndex++ ] = valueNr++;
          }
        }

        for ( String field : cmpFields.getFieldNames() ) {
          if ( Const.indexOfString( field, keys ) < 0 ) {
            cmpSql += ", " + meta.getCompareConnection().quoteField( field );
          }
        }
        refSql += " FROM " + refSchemaTable + " ORDER BY ";
        cmpSql += " FROM " + cmpSchemaTable + " ORDER BY ";
        for ( int i = 0; i < keys.length; i++ ) {
          if ( i > 0 ) {
            refSql += ", ";
            cmpSql += ", ";
          }
          refSql += meta.getReferenceConnection().quoteField( keys[ i ] );
          cmpSql += meta.getReferenceConnection().quoteField( keys[ i ] );
        }

        // Now we execute the SQL...
        //
        ResultSet refSet = data.referenceDb.openQuery( refSql );
        ResultSet cmpSet = data.compareDb.openQuery( cmpSql );

        // Now grab rows of data and start comparing the individual rows ...
        //
        IRowMeta oneMeta = null, twoMeta = null;

        Object[] one = data.referenceDb.getRow( refSet );
        if ( one != null ) {
          incrementLinesInput();
          if ( oneMeta == null ) {
            oneMeta = data.referenceDb.getReturnRowMeta();
            for ( int i = 0; i < keys.length; i++ ) {
              keyRowMeta.addValueMeta( oneMeta.searchValueMeta( keys[ i ] ) );
            }
          }
        }
        Object[] two = data.compareDb.getRow( cmpSet );
        if ( two != null ) {
          incrementLinesInput();
          if ( twoMeta == null ) {
            twoMeta = data.compareDb.getReturnRowMeta();
            if ( keyRowMeta.isEmpty() ) {
              for ( int i = 0; i < keys.length; i++ ) {
                keyRowMeta.addValueMeta( twoMeta.searchValueMeta( keys[ i ] ) );
              }
            }
          }
        }

        if ( one != null ) {
          nrRecordsReference++;
        }
        if ( two != null ) {
          nrRecordsCompare++;
        }

        do {

          if ( one == null && two != null ) {
            // A new record found in the compare table...
            //
            if ( getTransformMeta().isDoingErrorHandling() ) {
              String keyDesc = getKeyDesc( keyRowMeta, keyNrs, two );
              Object[] errorRowData = constructErrorRow( rowMeta, r, keyDesc, null, null );
              putError( data.errorRowMeta, errorRowData, 1, BaseMessages.getString(
                PKG, "TableCompare.Error.RecordNotInReferenceFoundInCompareTable", cmpSchemaTable, keyRowMeta
                  .getString( two ) ), null, "TAC004" );
            }
            nrErrors++;
            nrRightErrors++;

            two = data.compareDb.getRow( cmpSet );
            if ( two != null ) {
              nrRecordsCompare++;
            }

          } else if ( one != null && two == null ) {
            // A new record found in the reference table...
            //
            if ( getTransformMeta().isDoingErrorHandling() ) {
              String keyDesc = getKeyDesc( keyRowMeta, keyNrs, one );
              Object[] errorRowData = constructErrorRow( rowMeta, r, keyDesc, null, null );
              putError( data.errorRowMeta, errorRowData, 1, BaseMessages.getString(
                PKG, "TableCompare.Error.RecordInReferenceNotFoundInCompareTable", refSchemaTable, keyRowMeta
                  .getString( one ) ), null, "TAC005" );
            }
            nrErrors++;
            nrLeftErrors++;

            one = data.referenceDb.getRow( refSet );
            if ( one != null ) {
              nrRecordsReference++;
            }
          } else {
            if ( one != null && two != null ) {
              // both records are populated, compare the records...
              //
              int compare = oneMeta.compare( one, two, keyNrs );
              if ( compare == 0 ) { // The Key matches, we CAN compare the two rows...
                int compareValues = oneMeta.compare( one, two, valueNrs );
                if ( compareValues != 0 ) {
                  // Return the compare (most recent) row
                  //
                  if ( getTransformMeta().isDoingErrorHandling() ) {

                    // Give some details on what is wrong... (fields, values, etc)
                    //
                    for ( int idx : valueNrs ) {
                      IValueMeta valueMeta = oneMeta.getValueMeta( idx );
                      Object oneData = one[ idx ];
                      Object twoData = two[ idx ];
                      int cmp = valueMeta.compare( oneData, twoData );
                      if ( cmp != 0 ) {

                        String keyDesc = getKeyDesc( keyRowMeta, keyNrs, one );
                        String quote = valueMeta.isString() ? "'" : "";
                        String referenceData = quote + valueMeta.getString( oneData ) + quote;
                        String compareData = quote + valueMeta.getString( twoData ) + quote;

                        Object[] errorRowData =
                          constructErrorRow( rowMeta, r, keyDesc, referenceData, compareData );
                        putError(
                          data.errorRowMeta, errorRowData, 1, BaseMessages.getString(
                            PKG, "TableCompare.Error.CompareRowIsDifferentFromReference" ), valueMeta
                            .getName(), "TAC006" );
                      }
                    }
                  }
                  nrErrors++;
                  nrInnerErrors++;
                }

                // Get a new row from both streams...
                one = data.referenceDb.getRow( refSet );
                if ( one != null ) {
                  nrRecordsReference++;
                }
                two = data.compareDb.getRow( cmpSet );
                if ( two != null ) {
                  nrRecordsCompare++;
                }
              } else {
                if ( compare < 0 ) {
                  if ( getTransformMeta().isDoingErrorHandling() ) {
                    String keyDesc = getKeyDesc( keyRowMeta, keyNrs, one );
                    Object[] errorRowData = constructErrorRow( rowMeta, r, keyDesc, null, null );
                    putError( data.errorRowMeta, errorRowData, 1, BaseMessages.getString(
                      PKG, "TableCompare.Error.RecordNotInReferenceFoundInCompareTable", cmpSchemaTable,
                      keyRowMeta.getString( one ) ), null, "TAC004" );
                  }
                  nrErrors++;
                  nrRightErrors++;

                  one = data.referenceDb.getRow( refSet );
                  if ( one != null ) {
                    nrRecordsReference++;
                  }
                } else {
                  if ( getTransformMeta().isDoingErrorHandling() ) {
                    String keyDesc = getKeyDesc( keyRowMeta, keyNrs, two );
                    Object[] errorRowData = constructErrorRow( rowMeta, r, keyDesc, null, null );
                    putError( data.errorRowMeta, errorRowData, 1, BaseMessages.getString(
                      PKG, "TableCompare.Error.RecordInReferenceNotFoundInCompareTable", refSchemaTable,
                      keyRowMeta.getString( two ) ), null, "TAC005" );
                  }
                  nrErrors++;
                  nrLeftErrors++;

                  two = data.compareDb.getRow( cmpSet );
                  if ( two != null ) {
                    nrRecordsCompare++;
                  }
                }
              }
            }
          }

        } while ( ( one != null || two != null ) && !isStopped() );

        refSet.close();
        cmpSet.close();
      }

    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString(
        PKG, "TableCompare.Exception.UnexpectedErrorComparingTables", refSchemaTable, cmpSchemaTable ), e );
    }

    int index = 0;
    result[ index++ ] = Long.valueOf( nrErrors );
    result[ index++ ] = Long.valueOf( nrRecordsReference );
    result[ index++ ] = Long.valueOf( nrRecordsCompare );
    result[ index++ ] = Long.valueOf( nrLeftErrors );
    result[ index++ ] = Long.valueOf( nrInnerErrors );
    result[ index++ ] = Long.valueOf( nrRightErrors );

    r[ data.keyDescIndex ] = null;
    r[ data.valueReferenceIndex ] = null;
    r[ data.valueCompareIndex ] = null;

    return result;
  }

  private String getKeyDesc( IRowMeta keyRowMeta, int[] keyNrs, Object[] one ) throws HopException {
    StringBuilder keyDesc = new StringBuilder();
    for ( int x = 0; x < keyNrs.length; x++ ) {
      IValueMeta keyValueMeta = keyRowMeta.getValueMeta( x );
      Object keyValueData = one[ keyNrs[ x ] ];

      if ( keyDesc.length() > 0 ) {
        keyDesc.append( " and " );
      }
      keyDesc.append( keyValueMeta.getName() ).append( " = '" );
      keyDesc.append( keyValueMeta.getString( keyValueData ) );
      keyDesc.append( "'" );
    }
    return keyDesc.toString();
  }

  private Object[] constructErrorRow( IRowMeta rowMeta, Object[] r, String keyField,
                                      String referenceValue, String compareValue ) throws HopException {

    if ( data.errorRowMeta == null ) {
      data.errorRowMeta = rowMeta.clone();
    }

    r[ data.keyDescIndex ] = keyField;
    r[ data.valueReferenceIndex ] = referenceValue;
    r[ data.valueCompareIndex ] = compareValue;

    return r;
  }

  @Override
  public boolean init() {

    if ( super.init() ) {

      try {
        data.referenceDb = new Database( this, this, meta.getReferenceConnection() );
        data.referenceDb.connect();

      } catch ( Exception e ) {
        logError( BaseMessages.getString(
          PKG, "TableCompare.Exception.UnexpectedErrorConnectingToReferenceDatabase", meta
            .getReferenceConnection().getName() ), e );
        return false;
      }

      try {
        data.compareDb = new Database( this, this, meta.getCompareConnection() );
        data.compareDb.connect();

      } catch ( Exception e ) {
        logError( BaseMessages.getString(
          PKG, "TableCompare.Exception.UnexpectedErrorConnectingToCompareDatabase", meta
            .getCompareConnection().getName() ), e );
        return false;
      }
      return true;
    }
    return false;
  }

  @Override
  public void dispose() {

    if ( data.referenceDb != null ) {
      data.referenceDb.disconnect();
    }

    if ( data.compareDb != null ) {
      data.compareDb.disconnect();
    }

    super.dispose();
  }

}
