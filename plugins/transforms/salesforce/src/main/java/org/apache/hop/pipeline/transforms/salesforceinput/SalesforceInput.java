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

package org.apache.hop.pipeline.transforms.salesforceinput;

import com.sforce.ws.util.Base64;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceConnectionUtils;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceRecordValue;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceTransform;

import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;

/**
 * Read data from Salesforce module, convert them to rows and writes these to one or more output streams.
 *
 * @author Samatar
 * @since 10-06-2007
 */
public class SalesforceInput extends SalesforceTransform<SalesforceInputMeta, SalesforceInputData> {
  private static Class<?> PKG = SalesforceInputMeta.class; // For Translator

  public SalesforceInput( TransformMeta transformMeta, SalesforceInputMeta meta, SalesforceInputData data,
                          int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {
    if ( first ) {
      first = false;

      // Create the output row meta-data
      data.outputRowMeta = new RowMeta();

      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // For String to <type> conversions, we allocate a conversion meta data row as well...
      //
      data.convertRowMeta = data.outputRowMeta.cloneToType( IValueMeta.TYPE_STRING );

      // Let's query Salesforce
      data.connection.query( meta.isSpecifyQuery() );

      data.limitReached = true;
      data.recordcount = data.connection.getQueryResultSize();
      if ( data.recordcount > 0 ) {
        data.limitReached = false;
        data.nrRecords = data.connection.getRecordsCount();
      }
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "SalesforceInput.Log.RecordCount" ) + " : " + data.recordcount );
      }

    }

    Object[] outputRowData = null;

    try {
      // get one row ...
      outputRowData = getOneRow();

      if ( outputRowData == null ) {
        setOutputDone();
        return false;
      }

      putRow( data.outputRowMeta, outputRowData ); // copy row to output rowset(s);

      if ( checkFeedback( getLinesInput() ) ) {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "SalesforceInput.log.LineRow", "" + getLinesInput() ) );
        }
      }

      data.rownr++;
      data.recordIndex++;

      return true;
    } catch ( HopException e ) {
      boolean sendToErrorRow = false;
      String errorMessage = null;
      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "SalesforceInput.log.Exception", e.getMessage() ) );
        logError( Const.getStackTracker( e ) );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), outputRowData, 1, errorMessage, null, "SalesforceInput001" );
      }
    }
    return true;
  }

  private Object[] getOneRow() throws HopException {
    if ( data.limitReached || data.rownr >= data.recordcount ) {
      return null;
    }

    // Build an empty row based on the meta-data
    Object[] outputRowData = buildEmptyRow();

    try {

      // check for limit rows
      if ( data.limit > 0 && data.rownr >= data.limit ) {
        // User specified limit and we reached it
        // We end here
        data.limitReached = true;
        return null;
      } else {
        if ( data.rownr >= data.nrRecords || data.finishedRecord ) {
          if ( meta.getRecordsFilter() != SalesforceConnectionUtils.RECORDS_FILTER_UPDATED ) {
            // We retrieved all records available here
            // maybe we need to query more again ...
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "SalesforceInput.Log.NeedQueryMore", "" + data.rownr ) );
            }

            if ( data.connection.queryMore() ) {
              // We returned more result (query is not done yet)
              int nr = data.connection.getRecordsCount();
              data.nrRecords += nr;
              if ( log.isDetailed() ) {
                logDetailed( BaseMessages.getString( PKG, "SalesforceInput.Log.QueryMoreRetrieved", "" + nr ) );
              }

              // We need here to initialize recordIndex
              data.recordIndex = 0;

              data.finishedRecord = false;
            } else {
              // Query is done .. we finished !
              return null;
            }
          }
        }
      }

      // Return a record
      SalesforceRecordValue srvalue = data.connection.getRecord( data.recordIndex );
      data.finishedRecord = srvalue.isAllRecordsProcessed();

      if ( meta.getRecordsFilter() == SalesforceConnectionUtils.RECORDS_FILTER_DELETED ) {
        if ( srvalue.isRecordIndexChanges() ) {
          // We have moved forward...
          data.recordIndex = srvalue.getRecordIndex();
        }
        if ( data.finishedRecord && srvalue.getRecordValue() == null ) {
          // We processed all records
          return null;
        }
      }
      for ( int i = 0; i < data.nrFields; i++ ) {
        String value =
          data.connection.getRecordValue( srvalue.getRecordValue(), meta.getInputFields()[ i ].getField() );

        // DO Trimming!
        switch ( meta.getInputFields()[ i ].getTrimType() ) {
          case SalesforceInputField.TYPE_TRIM_LEFT:
            value = Const.ltrim( value );
            break;
          case SalesforceInputField.TYPE_TRIM_RIGHT:
            value = Const.rtrim( value );
            break;
          case SalesforceInputField.TYPE_TRIM_BOTH:
            value = Const.trim( value );
            break;
          default:
            break;
        }

        doConversions( outputRowData, i, value );

        // Do we need to repeat this field if it is null?
        if ( meta.getInputFields()[ i ].isRepeated() ) {
          if ( data.previousRow != null && Utils.isEmpty( value ) ) {
            outputRowData[ i ] = data.previousRow[ i ];
          }
        }

      } // End of loop over fields...

      int rowIndex = data.nrFields;

      // See if we need to add the url to the row...
      if ( meta.includeTargetURL() && !Utils.isEmpty( meta.getTargetURLField() ) ) {
        outputRowData[ rowIndex++ ] = data.connection.getURL();
      }

      // See if we need to add the module to the row...
      if ( meta.includeModule() && !Utils.isEmpty( meta.getModuleField() ) ) {
        outputRowData[ rowIndex++ ] = data.connection.getModule();
      }

      // See if we need to add the generated SQL to the row...
      if ( meta.includeSQL() && !Utils.isEmpty( meta.getSQLField() ) ) {
        outputRowData[ rowIndex++ ] = data.connection.getSQL();
      }

      // See if we need to add the server timestamp to the row...
      if ( meta.includeTimestamp() && !Utils.isEmpty( meta.getTimestampField() ) ) {
        outputRowData[ rowIndex++ ] = data.connection.getServerTimestamp();
      }

      // See if we need to add the row number to the row...
      if ( meta.includeRowNumber() && !Utils.isEmpty( meta.getRowNumberField() ) ) {
        outputRowData[ rowIndex++ ] = new Long( data.rownr );
      }

      if ( meta.includeDeletionDate() && !Utils.isEmpty( meta.getDeletionDateField() ) ) {
        outputRowData[ rowIndex++ ] = srvalue.getDeletionDate();
      }

      IRowMeta irow = getInputRowMeta();

      data.previousRow = irow == null ? outputRowData : irow.cloneRow( outputRowData ); // copy it to make
    } catch ( Exception e ) {
      throw new HopException( BaseMessages
        .getString( PKG, "SalesforceInput.Exception.CanNotReadFromSalesforce" ), e );
    }

    return outputRowData;
  }

  // DO CONVERSIONS...
  void doConversions( Object[] outputRowData, int i, String value ) throws HopValueException {
    IValueMeta targetValueMeta = data.outputRowMeta.getValueMeta( i );
    IValueMeta sourceValueMeta = data.convertRowMeta.getValueMeta( i );

    if ( IValueMeta.TYPE_BINARY != targetValueMeta.getType() ) {
      outputRowData[ i ] = targetValueMeta.convertData( sourceValueMeta, value );
    } else {
      // binary type of salesforce requires specific conversion
      if ( value != null ) {
        outputRowData[ i ] = Base64.decode( value.getBytes() );
      } else {
        outputRowData[ i ] = null;
      }
    }
  }

  /*
   * build the SQL statement to send to Salesforce
   */
  private String BuiltSOQl() {
    String sql = "";
    SalesforceInputField[] fields = meta.getInputFields();

    switch ( meta.getRecordsFilter() ) {
      case SalesforceConnectionUtils.RECORDS_FILTER_UPDATED:
        for ( int i = 0; i < data.nrFields; i++ ) {
          SalesforceInputField field = fields[ i ];
          sql += resolve( field.getField() );
          if ( i < data.nrFields - 1 ) {
            sql += ",";
          }
        }
        break;
      case SalesforceConnectionUtils.RECORDS_FILTER_DELETED:
        sql += "SELECT ";
        for ( int i = 0; i < data.nrFields; i++ ) {
          SalesforceInputField field = fields[ i ];
          sql += resolve( field.getField() );
          if ( i < data.nrFields - 1 ) {
            sql += ",";
          }
        }
        sql += " FROM " + resolve( meta.getModule() ) + " WHERE isDeleted = true";
        break;
      default:
        sql += "SELECT ";
        for ( int i = 0; i < data.nrFields; i++ ) {
          SalesforceInputField field = fields[ i ];
          sql += resolve( field.getField() );
          if ( i < data.nrFields - 1 ) {
            sql += ",";
          }
        }
        sql = sql + " FROM " + resolve( meta.getModule() );
        if ( !Utils.isEmpty( resolve( meta.getCondition() ) ) ) {
          sql += " WHERE " + resolve( meta.getCondition().replace( "\n\r", "" ).replace( "\n", "" ) );
        }
        break;
    }

    return sql;
  }

  /**
   * Build an empty row based on the meta-data.
   *
   * @return
   */
  private Object[] buildEmptyRow() {
    Object[] rowData = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
    return rowData;
  }

  @Override
  public boolean init() {

    if ( super.init() ) {
      // get total fields in the grid
      data.nrFields = meta.getInputFields().length;

      // Check if field list is filled
      if ( data.nrFields == 0 ) {
        log.logError( BaseMessages.getString( PKG, "SalesforceInputDialog.FieldsMissing.DialogMessage" ) );
        return false;
      }

      String soSQL = resolve( meta.getQuery() );
      try {

        if ( meta.isSpecifyQuery() ) {
          // Check if user specified a query
          if ( Utils.isEmpty( soSQL ) ) {
            log.logError( BaseMessages.getString( PKG, "SalesforceInputDialog.QueryMissing.DialogMessage" ) );
            return false;
          }
        } else {
          // check records filter
          if ( meta.getRecordsFilter() != SalesforceConnectionUtils.RECORDS_FILTER_ALL ) {
            String realFromDateString = resolve( meta.getReadFrom() );
            if ( Utils.isEmpty( realFromDateString ) ) {
              log.logError( BaseMessages.getString( PKG, "SalesforceInputDialog.FromDateMissing.DialogMessage" ) );
              return false;
            }
            String realToDateString = resolve( meta.getReadTo() );
            if ( Utils.isEmpty( realToDateString ) ) {
              log.logError( BaseMessages.getString( PKG, "SalesforceInputDialog.ToDateMissing.DialogMessage" ) );
              return false;
            }
            try {
              SimpleDateFormat dateFormat = new SimpleDateFormat( SalesforceInputMeta.DATE_TIME_FORMAT );
              data.startCal = new GregorianCalendar();
              data.startCal.setTime( dateFormat.parse( realFromDateString ) );
              data.endCal = new GregorianCalendar();
              data.endCal.setTime( dateFormat.parse( realToDateString ) );
              dateFormat = null;
            } catch ( Exception e ) {
              log.logError( BaseMessages.getString( PKG, "SalesforceInput.ErrorParsingDate" ), e );
              return false;
            }
          }
        }

        data.limit = Const.toLong( resolve( meta.getRowLimit() ), 0 );

        // Do we have to query for all records included deleted records
        data.connection.setQueryAll( meta.isQueryAll() );

        // Build query if needed
        if ( meta.isSpecifyQuery() ) {
          // Free hand SOQL Query
          data.connection.setSQL( soSQL.replace( "\n\r", " " ).replace( "\n", " " ) );
        } else {
          // Set calendars for update or deleted records
          if ( meta.getRecordsFilter() != SalesforceConnectionUtils.RECORDS_FILTER_ALL ) {
            data.connection.setCalendar( meta.getRecordsFilter(), data.startCal, data.endCal );
          }

          if ( meta.getRecordsFilter() == SalesforceConnectionUtils.RECORDS_FILTER_UPDATED ) {
            // Return fields list
            data.connection.setFieldsList( BuiltSOQl() );
          } else {
            // Build now SOQL
            data.connection.setSQL( BuiltSOQl() );
          }
        }

        // Now connect ...
        data.connection.connect();

        return true;
      } catch ( HopException ke ) {
        logError( BaseMessages.getString( PKG, "SalesforceInput.Log.ErrorOccurredDuringTransformInitialize" )
          + ke.getMessage() );
        return false;
      }
    }
    return false;
  }

  @Override
  public void dispose() {
    if ( data.outputRowMeta != null ) {
      data.outputRowMeta = null;
    }
    if ( data.convertRowMeta != null ) {
      data.convertRowMeta = null;
    }
    if ( data.previousRow != null ) {
      data.previousRow = null;
    }
    if ( data.startCal != null ) {
      data.startCal = null;
    }
    if ( data.endCal != null ) {
      data.endCal = null;
    }
    super.dispose();
  }
}
