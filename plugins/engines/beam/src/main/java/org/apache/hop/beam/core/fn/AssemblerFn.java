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
import org.apache.beam.sdk.values.KV;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AssemblerFn extends DoFn<KV<HopRow, KV<HopRow, HopRow>>, HopRow> {

  private String outputRowMetaJson;
  private String leftKRowMetaJson;
  private String leftVRowMetaJson;
  private String rightVRowMetaJson;
  private String counterName;
  private List<String> transformPluginClasses;
  private List<String>xpPluginClasses;

  private static final Logger LOG = LoggerFactory.getLogger( AssemblerFn.class );

  private transient IRowMeta outputRowMeta;
  private transient IRowMeta leftKRowMeta;
  private transient IRowMeta leftVRowMeta;
  private transient IRowMeta rightVRowMeta;

  private transient Counter initCounter;
  private transient Counter writtenCounter;
  private transient Counter errorCounter;

  public AssemblerFn() {
  }

  public AssemblerFn( String outputRowMetaJson, String leftKRowMetaJson, String leftVRowMetaJson, String rightVRowMetaJson, String counterName,
                      List<String> transformPluginClasses, List<String>xpPluginClasses) {
    this.outputRowMetaJson = outputRowMetaJson;
    this.leftKRowMetaJson = leftKRowMetaJson;
    this.leftVRowMetaJson = leftVRowMetaJson;
    this.rightVRowMetaJson = rightVRowMetaJson;
    this.counterName = counterName;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Setup
  public void setUp() {
    try {
      writtenCounter = Metrics.counter( Pipeline.METRIC_NAME_WRITTEN, counterName );
      errorCounter = Metrics.counter( Pipeline.METRIC_NAME_ERROR, counterName );

      // Initialize Hop Beam
      //
      BeamHop.init( transformPluginClasses, xpPluginClasses );
      outputRowMeta = JsonRowMeta.fromJson( outputRowMetaJson );
      leftKRowMeta = JsonRowMeta.fromJson( leftKRowMetaJson );
      leftVRowMeta = JsonRowMeta.fromJson( leftVRowMetaJson );
      rightVRowMeta = JsonRowMeta.fromJson( rightVRowMetaJson );

      Metrics.counter( Pipeline.METRIC_NAME_INIT, counterName ).inc();
    } catch(Exception e) {
      errorCounter.inc();
      LOG.error( "Error initializing assembling rows", e);
      throw new RuntimeException( "Error initializing assembling output KV<row, KV<row, row>>", e );
    }
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      KV<HopRow, KV<HopRow, HopRow>> element = processContext.element();
      KV<HopRow, HopRow> value = element.getValue();

      HopRow key = element.getKey();
      HopRow leftValue = value.getKey();
      HopRow rightValue = value.getValue();

      Object[] outputRow = RowDataUtil.allocateRowData( outputRowMeta.size() );
      int index = 0;

      // Hop style, first the left values
      //
      if (leftValue.allNull()) {
        index+=leftVRowMeta.size();
      } else {
        for ( int i = 0; i < leftVRowMeta.size(); i++ ) {
          outputRow[ index++ ] = leftValue.getRow()[ i ];
        }
      }

      // Now the left key
      //
      if (leftValue.allNull()) {
        index+=leftKRowMeta.size();
      } else {
        for ( int i = 0; i < leftKRowMeta.size(); i++ ) {
          outputRow[ index++ ] = key.getRow()[ i ];
        }
      }

      // Then the right key
      //
      if (rightValue.allNull()) {
        // No right key given if the value is null
        //
        index+=leftKRowMeta.size();
      } else {
        for ( int i = 0; i < leftKRowMeta.size(); i++ ) {
          outputRow[ index++ ] = key.getRow()[ i ];
        }
      }

      // Finally the right values
      //
      if (rightValue.allNull()) {
        index+=rightVRowMeta.size();
      } else {
        for ( int i = 0; i < rightVRowMeta.size(); i++ ) {
          outputRow[ index++ ] = rightValue.getRow()[ i ];
        }
      }

      // System.out.println("Assembled row : "+outputRowMeta.getString(outputRow));

      processContext.output( new HopRow( outputRow ) );
      writtenCounter.inc();

    } catch(Exception e) {
      errorCounter.inc();
      LOG.error( "Error assembling rows", e);
      throw new RuntimeException( "Error assembling output KV<row, KV<row, row>>", e );
    }
  }
}

