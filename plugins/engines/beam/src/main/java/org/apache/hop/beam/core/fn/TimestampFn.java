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

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.pipeline.Pipeline;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class TimestampFn extends DoFn<HopRow, HopRow> {

  private String transformName;
  private String rowMetaJson;
  private String fieldName;
  private final boolean getTimestamp;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;

  private transient Counter readCounter;
  private transient Counter writtenCounter;
  private transient Counter errorCounter;

  private transient int fieldIndex;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( TimestampFn.class );

  private transient IRowMeta inputRowMeta;
  private transient IValueMeta fieldValueMeta;

  public TimestampFn( String transformName, String rowMetaJson, String fieldName, boolean getTimestamp, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    this.transformName = transformName;
    this.rowMetaJson = rowMetaJson;
    this.fieldName = fieldName;
    this.getTimestamp = getTimestamp;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Setup
  public void setUp() {
    try {
      // Initialize Hop Beam
      //
      BeamHop.init( transformPluginClasses, xpPluginClasses );

      inputRowMeta = JsonRowMeta.fromJson( rowMetaJson );

      readCounter = Metrics.counter( Pipeline.METRIC_NAME_READ, transformName );
      writtenCounter = Metrics.counter( Pipeline.METRIC_NAME_WRITTEN, transformName );
      errorCounter = Metrics.counter( Pipeline.METRIC_NAME_ERROR, transformName );

      fieldIndex = -1;
      if ( !getTimestamp && StringUtils.isNotEmpty( fieldName ) ) {
        fieldIndex = inputRowMeta.indexOfValue( fieldName );
        if ( fieldIndex < 0 ) {
          throw new RuntimeException( "Field '" + fieldName + "' couldn't be found in put : " + inputRowMeta.toString() );
        }
        fieldValueMeta = inputRowMeta.getValueMeta( fieldIndex );
      }

      Metrics.counter( Pipeline.METRIC_NAME_INIT, transformName ).inc();
    } catch(Exception e) {
      errorCounter.inc();
      LOG.error( "Error in setup of adding timestamp to rows : " + e.getMessage() );
      throw new RuntimeException( "Error setup of adding timestamp to rows", e );
    }
  }


  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      HopRow hopRow = processContext.element();
      readCounter.inc();

      // The instant
      //
      Instant instant;

      if ( getTimestamp ) {
        instant = processContext.timestamp();

        // Add one row to the stream.
        //
        Object[] outputRow = RowDataUtil.createResizedCopy( hopRow.getRow(), inputRowMeta.size() + 1 );

        // Hop "Date" type field output: java.util.Date.
        // Use the last field in the output
        //
        outputRow[ inputRowMeta.size() ] = instant.toDate();
        hopRow = new HopRow( outputRow );
      } else {
        if ( fieldIndex < 0 ) {
          instant = Instant.now();
        } else {
          Object fieldData = hopRow.getRow()[ fieldIndex ];
          if ( IValueMeta.TYPE_TIMESTAMP == fieldValueMeta.getType() ) {
            java.sql.Timestamp timestamp = ( (ValueMetaTimestamp) fieldValueMeta ).getTimestamp( fieldData );
            instant = new Instant( timestamp.toInstant() );
          } else {
            Date date = fieldValueMeta.getDate( fieldData );
            if (date==null) {
              throw new HopException( "Timestamp field contains a null value, this can't be used to set a timestamp on a bounded/unbounded collection of data" );
            }
            instant = new Instant( date.getTime() );
          }
        }
      }

      // Pass the row to the process context
      //
      processContext.outputWithTimestamp( hopRow, instant );
      writtenCounter.inc();

    } catch ( Exception e ) {
      errorCounter.inc();
      LOG.error( "Error adding timestamp to rows : " + processContext.element() + ", " + e.getMessage() );
      throw new RuntimeException( "Error adding timestamp to rows", e );
    }
  }

  @Override public Duration getAllowedTimestampSkew() {
    return Duration.standardMinutes( 120 );
  }
}
