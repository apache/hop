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
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HopToStringFn extends DoFn<HopRow, String> {

  private String counterName;
  private String outputLocation;
  private String separator;
  private String enclosure;
  private String rowMetaJson;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;

  private transient IRowMeta rowMeta;
  private transient Counter initCounter;
  private transient Counter readCounter;
  private transient Counter outputCounter;
  private transient Counter errorCounter;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( HopToStringFn.class );

  public HopToStringFn( String counterName, String outputLocation, String separator, String enclosure, String rowMetaJson, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    this.counterName = counterName;
    this.outputLocation = outputLocation;
    this.separator = separator;
    this.enclosure = enclosure;
    this.rowMetaJson = rowMetaJson;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Setup
  public void setUp() {
    try {
      readCounter = Metrics.counter( Pipeline.METRIC_NAME_READ, counterName );
      outputCounter = Metrics.counter( Pipeline.METRIC_NAME_OUTPUT, counterName );
      errorCounter = Metrics.counter( Pipeline.METRIC_NAME_ERROR, counterName );

      // Initialize Hop Beam
      //
      BeamHop.init( transformPluginClasses, xpPluginClasses );
      rowMeta = JsonRowMeta.fromJson( rowMetaJson );

      Metrics.counter( Pipeline.METRIC_NAME_INIT, counterName ).inc();
    } catch ( Exception e ) {
      errorCounter.inc();
      LOG.info( "Parse error on setup of Hop data to string lines : " + e.getMessage() );
      throw new RuntimeException( "Error on setup of converting Hop data to string lines", e );
    }
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      HopRow inputRow = processContext.element();
      readCounter.inc();

      // Just a quick and dirty output for now...
      // TODO: refine with multiple output formats, Avro, Parquet, ...
      //
      StringBuffer line = new StringBuffer();

      for ( int i = 0; i < rowMeta.size(); i++ ) {

        if ( i > 0 ) {
          line.append( separator );
        }

        String valueString = rowMeta.getString( inputRow.getRow(), i );

        if ( valueString != null ) {
          boolean enclose = false;
          if ( StringUtils.isNotEmpty( enclosure ) ) {
            enclose = valueString.contains( enclosure );
          }
          if ( enclose ) {
            line.append( enclosure );
          }
          line.append( valueString );
          if ( enclose ) {
            line.append( enclosure );
          }
        }
      }

      // Pass the row to the process context
      //
      processContext.output( line.toString() );
      outputCounter.inc();

    } catch ( Exception e ) {
      errorCounter.inc();
      LOG.info( "Parse error on " + processContext.element() + ", " + e.getMessage() );
      throw new RuntimeException( "Error converting Hop data to string lines", e );
    }
  }


}
