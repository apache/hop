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

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class HopToBQTableRowFn implements SerializableFunction<HopRow, TableRow> {

  private String counterName;
  private String rowMetaJson;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;

  private transient IRowMeta rowMeta;
  private transient Counter initCounter;
  private transient Counter readCounter;
  private transient Counter outputCounter;
  private transient Counter errorCounter;

  private transient SimpleDateFormat simpleDateFormat;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( HopToBQTableRowFn.class );

  public HopToBQTableRowFn( String counterName, String rowMetaJson, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    this.counterName = counterName;
    this.rowMetaJson = rowMetaJson;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Override public TableRow apply( HopRow inputRow ) {

    try {
      if ( rowMeta == null ) {
        readCounter = Metrics.counter( Pipeline.METRIC_NAME_READ, counterName );
        outputCounter = Metrics.counter( Pipeline.METRIC_NAME_OUTPUT, counterName );
        errorCounter = Metrics.counter( Pipeline.METRIC_NAME_ERROR, counterName );

        // Initialize Hop Beam
        //
        BeamHop.init( transformPluginClasses, xpPluginClasses );
        rowMeta = JsonRowMeta.fromJson( rowMetaJson );

        simpleDateFormat = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" );
        Metrics.counter( Pipeline.METRIC_NAME_INIT, counterName ).inc();
      }

      readCounter.inc();

      TableRow tableRow = new TableRow();
      for (int i=0;i<rowMeta.size();i++) {
        IValueMeta valueMeta = rowMeta.getValueMeta( i );
        Object valueData = inputRow.getRow()[i];
        if (!valueMeta.isNull( valueData )) {
          switch ( valueMeta.getType() ) {
            case IValueMeta.TYPE_STRING: tableRow.put( valueMeta.getName(), valueMeta.getString( valueData ) ); break;
            case IValueMeta.TYPE_INTEGER: tableRow.put( valueMeta.getName(), valueMeta.getInteger( valueData ) ); break;
            case IValueMeta.TYPE_DATE:
              Date date = valueMeta.getDate( valueData );
              String formattedDate = simpleDateFormat.format( date );
              tableRow.put( valueMeta.getName(), formattedDate);
              break;
            case IValueMeta.TYPE_BOOLEAN: tableRow.put( valueMeta.getName(), valueMeta.getBoolean( valueData ) ); break;
            case IValueMeta.TYPE_NUMBER: tableRow.put( valueMeta.getName(), valueMeta.getNumber( valueData ) ); break;
            default:
              throw new RuntimeException( "Data type conversion from Hop to BigQuery TableRow not supported yet: " +valueMeta.toString());
          }
        }
      }

      // Pass the row to the process context
      //
      outputCounter.inc();

      return tableRow;

    } catch ( Exception e ) {
      errorCounter.inc();
      LOG.info( "Conversion error HopRow to BigQuery TableRow : " + e.getMessage() );
      throw new RuntimeException( "Error converting HopRow to BigQuery TableRow", e );
    }
  }


}
