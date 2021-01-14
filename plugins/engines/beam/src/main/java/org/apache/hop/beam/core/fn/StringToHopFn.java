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
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class StringToHopFn extends DoFn<String, HopRow> {

  private String transformName;
  private String rowMetaJson;
  private String separator;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;

  private transient Counter inputCounter;
  private transient Counter writtenCounter;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( StringToHopFn.class );

  private transient IRowMeta rowMeta;

  public StringToHopFn( String transformName, String rowMetaJson, String separator, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    this.transformName = transformName;
    this.rowMetaJson = rowMetaJson;
    this.separator = separator;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Setup
  public void setUp() {
    try {
      inputCounter = Metrics.counter( Pipeline.METRIC_NAME_INPUT, transformName );
      writtenCounter = Metrics.counter( Pipeline.METRIC_NAME_WRITTEN, transformName );

      // Initialize Hop Beam
      //
      BeamHop.init( transformPluginClasses, xpPluginClasses );
      rowMeta = JsonRowMeta.fromJson( rowMetaJson );

      Metrics.counter( Pipeline.METRIC_NAME_INIT, transformName ).inc();
    } catch ( Exception e ) {
      Metrics.counter( Pipeline.METRIC_NAME_ERROR, transformName ).inc();
      LOG.error( "Error in setup of converting input data into Hop rows : " + e.getMessage() );
      throw new RuntimeException( "Error in setup of converting input data into Hop rows", e );
    }
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      String inputString = processContext.element();
      inputCounter.inc();

      String[] components = inputString.split( separator, -1 );

      // TODO: implement enclosure in FileDefinition
      //

      Object[] row = RowDataUtil.allocateRowData( rowMeta.size() );
      int index = 0;
      while ( index < rowMeta.size() && index < components.length ) {
        String sourceString = components[ index ];
        IValueMeta valueMeta = rowMeta.getValueMeta( index );
        IValueMeta stringMeta = new ValueMetaString( "SourceString" );
        stringMeta.setConversionMask( valueMeta.getConversionMask() );
        try {
          row[ index ] = valueMeta.convertDataFromString( sourceString, stringMeta, null, null, IValueMeta.TRIM_TYPE_NONE );
        } catch ( HopValueException ve ) {
          throw new HopException( "Unable to convert value '" + sourceString + "' to value : " + valueMeta.toStringMeta(), ve );
        }
        index++;
      }

      // Pass the row to the process context
      //
      processContext.output( new HopRow( row ) );
      writtenCounter.inc();

    } catch ( Exception e ) {
      Metrics.counter( Pipeline.METRIC_NAME_ERROR, transformName ).inc();
      LOG.error( "Error converting input data into Hop rows " + processContext.element() + ", " + e.getMessage() );
      throw new RuntimeException( "Error converting input data into Hop rows", e );

    }
  }


}
