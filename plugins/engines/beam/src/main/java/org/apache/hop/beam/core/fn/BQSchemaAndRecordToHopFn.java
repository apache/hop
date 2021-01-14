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

package org.apache.hop.beam.core.fn;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.List;

/**
 * BigQuery Avro SchemaRecord to HopRow
 */
public class BQSchemaAndRecordToHopFn implements SerializableFunction<SchemaAndRecord, HopRow> {

  private String transformName;
  private String rowMetaJson;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;

  private transient Counter initCounter;
  private transient Counter inputCounter;
  private transient Counter writtenCounter;
  private transient Counter errorCounter;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( BQSchemaAndRecordToHopFn.class );

  private transient IRowMeta rowMeta;
  private transient SimpleDateFormat simpleDateTimeFormat;
  private transient SimpleDateFormat simpleDateFormat;

  public BQSchemaAndRecordToHopFn( String transformName, String rowMetaJson, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    this.transformName = transformName;
    this.rowMetaJson = rowMetaJson;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  public HopRow apply( SchemaAndRecord schemaAndRecord ) {

    try {

      GenericRecord record = schemaAndRecord.getRecord();
      TableSchema tableSchema = schemaAndRecord.getTableSchema();

      if ( rowMeta == null ) {

        inputCounter = Metrics.counter( Pipeline.METRIC_NAME_INPUT, transformName );
        writtenCounter = Metrics.counter( Pipeline.METRIC_NAME_WRITTEN, transformName );
        errorCounter = Metrics.counter( Pipeline.METRIC_NAME_ERROR, transformName );

        // Initialize Hop
        //
        BeamHop.init( transformPluginClasses, xpPluginClasses );
        rowMeta = JsonRowMeta.fromJson( rowMetaJson );

        int[] valueTypes = new int[rowMeta.size()];

        List<TableFieldSchema> fields = tableSchema.getFields();
        for (int i=0;i<fields.size();i++) {
          TableFieldSchema fieldSchema = fields.get( i );
          String name = fieldSchema.getName();
          int index = rowMeta.indexOfValue( name );
          // Ignore everything we didn't ask for.
          //
          if (index>=0) {
            String avroTypeString = fieldSchema.getType();
            try {
              AvroType avroType = AvroType.valueOf( avroTypeString );
              valueTypes[index] = avroType.getHopType();
            } catch(IllegalArgumentException e) {
              throw new RuntimeException( "Unable to recognize data type '"+avroTypeString+"'", e );
            }
          }
        }

        // See that we got all the fields covered...
        //
        for (int i = 0;i<rowMeta.size();i++) {
          if (valueTypes[i]==0) {
            IValueMeta valueMeta = rowMeta.getValueMeta( i );
            throw new RuntimeException( "Unable to find field '"+valueMeta.getName()+"'" );
          }
        }

        simpleDateTimeFormat = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss" );
        simpleDateTimeFormat.setLenient( true );
        simpleDateFormat = new SimpleDateFormat( "yyyy-MM-dd" );
        simpleDateFormat.setLenient( true );
        Metrics.counter( Pipeline.METRIC_NAME_INIT, transformName ).inc();
      }

      inputCounter.inc();

      // Convert to the requested Hop Data types
      //
      Object[] row = RowDataUtil.allocateRowData( rowMeta.size() );
      for (int index=0; index < rowMeta.size() ; index++) {
        IValueMeta valueMeta = rowMeta.getValueMeta( index );
        Object srcData = record.get(valueMeta.getName());
        if (srcData!=null) {
          switch(valueMeta.getType()) {
            case IValueMeta.TYPE_STRING:
              row[index] = srcData.toString();
              break;
            case IValueMeta.TYPE_INTEGER:
              row[index] = (Long)srcData;
              break;
            case IValueMeta.TYPE_NUMBER:
              row[index] = (Double)srcData;
              break;
            case IValueMeta.TYPE_BOOLEAN:
              row[index] = (Boolean)srcData;
              break;
            case IValueMeta.TYPE_DATE:
              // We get a Long back
              //
              String datetimeString = ((Utf8) srcData).toString();
              if (datetimeString.length()==10) {
                row[index] = simpleDateFormat.parse( datetimeString );
              } else {
                row[ index ] = simpleDateTimeFormat.parse( datetimeString );
              }
              break;
            default:
              throw new RuntimeException("Conversion from Avro JSON to Hop is not yet supported for Hop data type '"+valueMeta.getTypeDesc()+"'");
          }
        }
      }

      // Pass the row to the process context
      //
      writtenCounter.inc();
      return new HopRow( row );

    } catch ( Exception e ) {
      errorCounter.inc();
      LOG.error( "Error converting BQ Avro data into Hop rows : " + e.getMessage() );
      throw new RuntimeException( "Error converting BQ Avro data into Hop rows", e );

    }
  }

  //  From:
  //         https://cloud.google.com/dataprep/docs/html/BigQuery-Data-Type-Conversions_102563896
  //
  public enum AvroType {
    STRING(IValueMeta.TYPE_STRING),
    BYTES(IValueMeta.TYPE_STRING),
    INTEGER(IValueMeta.TYPE_INTEGER),
    INT64(IValueMeta.TYPE_INTEGER),
    FLOAT(IValueMeta.TYPE_NUMBER),
    FLOAT64(IValueMeta.TYPE_NUMBER),
    BOOLEAN(IValueMeta.TYPE_BOOLEAN),
    BOOL(IValueMeta.TYPE_BOOLEAN),
    TIMESTAMP(IValueMeta.TYPE_DATE),
    DATE(IValueMeta.TYPE_DATE),
    TIME(IValueMeta.TYPE_DATE),
    DATETIME(IValueMeta.TYPE_DATE),
    ;

    private int hopType;

    private AvroType(int hopType) {
      this.hopType = hopType;
    }

    /**
     * Gets hopType
     *
     * @return value of hopType
     */
    public int getHopType() {
      return hopType;
    }
  }

}
