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

package org.apache.hop.testing;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * The implementation of a CSV Data Set Group
 * We simply write/read the rows without a header into a file defined by the tableName in the data set
 */
public class DataSetCsvUtil {


  private static void setValueFormats( IRowMeta rowMeta ) {
    for ( IValueMeta valueMeta : rowMeta.getValueMetaList() ) {
      if ( StringUtils.isEmpty( valueMeta.getConversionMask() ) ) {
        switch ( valueMeta.getType() ) {
          case IValueMeta.TYPE_INTEGER:
            valueMeta.setConversionMask( "0" );
            break;
          case IValueMeta.TYPE_NUMBER:
            valueMeta.setConversionMask( "0.#" );
            break;
          case IValueMeta.TYPE_DATE:
            valueMeta.setConversionMask( "yyyyMMdd-HHmmss.SSS" );
            break;
          default:
            break;
        }
      }
    }
  }

  public static final List<Object[]> getAllRows( IVariables variables, DataSet dataSet ) throws HopException {
    IRowMeta setRowMeta = dataSet.getSetRowMeta();
    setValueFormats( setRowMeta );
    String dataSetFilename = dataSet.getActualDataSetFilename(variables);
    List<Object[]> rows = new ArrayList<>();
    final ValueMetaString constantValueMeta = new ValueMetaString( "constant" );

    try {
      FileObject file = HopVfs.getFileObject( dataSetFilename );
      if ( !file.exists() ) {
        // This is fine.  We haven't put rows in yet.
        //
        return rows;
      }

      try (
        Reader reader = new InputStreamReader( new BufferedInputStream( HopVfs.getInputStream( file ) ) );
        CSVParser csvParser = new CSVParser( reader, getCsvFormat( setRowMeta ) );
      ) {
        for ( CSVRecord csvRecord : csvParser ) {
          if ( csvRecord.getRecordNumber() > 1 ) {
            Object[] row = RowDataUtil.allocateRowData( setRowMeta.size() );
            for ( int i = 0; i < setRowMeta.size(); i++ ) {
              IValueMeta valueMeta = setRowMeta.getValueMeta( i ).clone();
              constantValueMeta.setConversionMetadata( valueMeta );
              String value = csvRecord.get( i );
              row[ i ] = valueMeta.convertData( constantValueMeta, value );
            }
            rows.add( row );
          }
        }
      }
      return rows;
    } catch ( Exception e ) {
      throw new HopException( "Unable to get all rows for CSV data set '" + dataSet.getName() + "'", e );
    }
  }


  /**
   * Get the rows for this data set in the format of the data set.
   *
   *
   * @param variables
   * @param log      the logging channel to which you can write.
   * @param location The fields to obtain in the order given
   * @return The rows for the given location
   * @throws HopException
   */
  public static final List<Object[]> getAllRows( IVariables variables, ILogChannel log, DataSet dataSet, PipelineUnitTestSetLocation location ) throws HopException {

    IRowMeta setRowMeta = dataSet.getSetRowMeta();

    // The row description of the output of this transform...
    //
    final IRowMeta outputRowMeta = dataSet.getMappedDataSetFieldsRowMeta( location );

    setValueFormats( setRowMeta );
    String dataSetFilename = dataSet.getActualDataSetFilename(variables);
    List<Object[]> rows = new ArrayList<>();
    final ValueMetaString constantValueMeta = new ValueMetaString( "constant" );

    try {

      FileObject file = HopVfs.getFileObject( dataSetFilename );
      if ( !file.exists() ) {
        // This is fine.  We haven't put rows in yet.
        //
        return rows;
      }

      List<String> sortFields = location.getFieldOrder();

      // See how we mapped the fields
      //
      List<PipelineUnitTestFieldMapping> fieldMappings = location.getFieldMappings();
      int[] dataSetFieldIndexes = new int[ fieldMappings.size() ];
      for ( int i = 0; i < fieldMappings.size(); i++ ) {
        PipelineUnitTestFieldMapping fieldMapping = fieldMappings.get( i );
        String dataSetFieldName = fieldMapping.getDataSetFieldName();
        dataSetFieldIndexes[ i ] = setRowMeta.indexOfValue( dataSetFieldName );
      }

      try (
        Reader reader = new InputStreamReader( new BufferedInputStream( HopVfs.getInputStream( file ) ) );
        CSVParser csvParser = new CSVParser( reader, CSVFormat.DEFAULT );
      ) {
        for ( CSVRecord csvRecord : csvParser ) {
          if ( csvRecord.getRecordNumber() > 1 ) {
            Object[] row = RowDataUtil.allocateRowData( dataSetFieldIndexes.length );

            // Only get certain values...
            //
            for ( int i = 0; i < dataSetFieldIndexes.length; i++ ) {
              int index = dataSetFieldIndexes[ i ];

              IValueMeta valueMeta = setRowMeta.getValueMeta( index );
              constantValueMeta.setConversionMetadata( valueMeta );
              String value = csvRecord.get( index );
              row[ i ] = valueMeta.convertData( constantValueMeta, value );
            }
            rows.add( row );
          }
        }
      }

      // Which fields are we sorting on (if any)
      //
      int[] sortIndexes = new int[ sortFields.size() ];
      for ( int i = 0; i < sortIndexes.length; i++ ) {
        sortIndexes[ i ] = outputRowMeta.indexOfValue( sortFields.get( i ) );
      }

      if ( outputRowMeta.isEmpty() ) {
        log.logError( "WARNING: No field mappings selected for data set '" + dataSet.getName() + "', returning empty set of rows" );
        return new ArrayList<>();
      }

      if ( !sortFields.isEmpty() ) {

        // Sort the rows...
        //
        Collections.sort( rows, ( o1, o2 ) -> {
          try {
            return outputRowMeta.compare( o1, o2, sortIndexes );
          } catch ( HopValueException e ) {
            throw new RuntimeException( "Unable to compare 2 rows", e );
          }
        } );
      }

      return rows;

    } catch (
      Exception e ) {
      throw new HopException( "Unable to get all rows for database data set '" + dataSet.getName() + "'", e );
    }
  }

  public static final void writeDataSetData( IVariables variables, DataSet dataSet, IRowMeta rowMeta, List<Object[]> rows ) throws HopException {

    String dataSetFilename = dataSet.getActualDataSetFilename(variables);

    IRowMeta setRowMeta = rowMeta.clone(); // just making sure
    setValueFormats( setRowMeta );

    OutputStream outputStream = null;
    BufferedWriter writer = null;
    CSVPrinter csvPrinter = null;
    try {

      FileObject file = HopVfs.getFileObject( dataSetFilename );
      outputStream = HopVfs.getOutputStream( file, false );
      writer = new BufferedWriter( new OutputStreamWriter( outputStream ) );
      CSVFormat csvFormat = getCsvFormat( rowMeta );
      csvPrinter = new CSVPrinter( writer, csvFormat );

      for ( Object[] row : rows ) {
        List<String> strings = new ArrayList<>();
        for ( int i = 0; i < setRowMeta.size(); i++ ) {
          IValueMeta valueMeta = setRowMeta.getValueMeta( i );
          String string = valueMeta.getString( row[ i ] );
          strings.add( string );
        }
        csvPrinter.printRecord( strings );
      }
      csvPrinter.flush();


    } catch ( Exception e ) {
      throw new HopException( "Unable to write data set to file '" + dataSetFilename + "'", e );
    } finally {
      try {
        if ( csvPrinter != null ) {
          csvPrinter.close();
        }
        if ( writer != null ) {
          writer.close();
        }
        if ( outputStream != null ) {
          outputStream.close();
        }
      } catch ( IOException e ) {
        throw new HopException( "Error closing file " + dataSetFilename + " : ", e );
      }
    }
  }

  public static CSVFormat getCsvFormat( IRowMeta rowMeta ) {
    return CSVFormat.DEFAULT.withHeader( rowMeta.getFieldNames() ).withQuote( '\"' ).withQuoteMode( QuoteMode.MINIMAL );
  }

}
