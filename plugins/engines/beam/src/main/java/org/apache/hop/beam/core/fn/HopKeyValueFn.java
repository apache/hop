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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

// Split a Hop row into key and values parts
//
public class HopKeyValueFn extends DoFn<HopRow, KV<HopRow, HopRow>> {

  private String inputRowMetaJson;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;
  private String[] keyFields;
  private String[] valueFields;
  private String counterName;

  private static final Logger LOG = LoggerFactory.getLogger( HopKeyValueFn.class );

  private transient IRowMeta inputRowMeta;
  private transient int[] keyIndexes;
  private transient int[] valueIndexes;

  private transient Counter initCounter;
  private transient Counter readCounter;
  private transient Counter errorCounter;

  public HopKeyValueFn() {
  }

  public HopKeyValueFn( String inputRowMetaJson, List<String> transformPluginClasses, List<String> xpPluginClasses,
                        String[] keyFields, String[] valueFields, String counterName) {
    this.inputRowMetaJson = inputRowMetaJson;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
    this.keyFields = keyFields;
    this.valueFields = valueFields;
    this.counterName = counterName;
  }

  @Setup
  public void setUp() {
    try {
      readCounter = Metrics.counter( Pipeline.METRIC_NAME_READ, counterName );
      errorCounter = Metrics.counter( Pipeline.METRIC_NAME_ERROR, counterName );

      // Initialize Hop Beam
      //
      BeamHop.init(transformPluginClasses, xpPluginClasses);
      inputRowMeta = JsonRowMeta.fromJson( inputRowMetaJson );

      // Calculate key indexes
      //
      if ( keyFields.length==0) {
        throw new HopException( "There are no group fields" );
      }
      keyIndexes = new int[ keyFields.length];
      for ( int i = 0; i< keyFields.length; i++) {
        keyIndexes[i]=inputRowMeta.indexOfValue( keyFields[i] );
        if ( keyIndexes[i]<0) {
          throw new HopException( "Unable to find group by field '"+ keyFields[i]+"' in input "+inputRowMeta.toString() );
        }
      }

      // Calculate the value indexes
      //
      valueIndexes =new int[ valueFields.length];
      for ( int i = 0; i< valueFields.length; i++) {
        valueIndexes[i] = inputRowMeta.indexOfValue( valueFields[i] );
        if ( valueIndexes[i]<0) {
          throw new HopException( "Unable to find subject by field '"+ valueFields[i]+"' in input "+inputRowMeta.toString() );
        }
      }

      // Now that we know everything, we can split the row...
      //
      Metrics.counter( Pipeline.METRIC_NAME_INIT, counterName ).inc();
    } catch(Exception e) {
      errorCounter.inc();
      LOG.error("Error setup of splitting row into key and value", e);
      throw new RuntimeException( "Unable to setup of split row into key and value", e );
    }
  }


  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      // Get an input row
      //
      HopRow inputHopRow = processContext.element();
      readCounter.inc();

      Object[] inputRow = inputHopRow.getRow();

      // Copy over the data...
      //
      Object[] keyRow = RowDataUtil.allocateRowData( keyIndexes.length );
      for ( int i = 0; i< keyIndexes.length; i++) {
        keyRow[i] = inputRow[ keyIndexes[i]];
      }

      // Copy over the values...
      //
      Object[] valueRow = RowDataUtil.allocateRowData( valueIndexes.length );
      for ( int i = 0; i< valueIndexes.length; i++) {
        valueRow[i] = inputRow[ valueIndexes[i]];
      }

      KV<HopRow, HopRow> keyValue = KV.of( new HopRow(keyRow), new HopRow( valueRow ) );
      processContext.output( keyValue );

    } catch(Exception e) {
      errorCounter.inc();
      LOG.error("Error splitting row into key and value", e);
      throw new RuntimeException( "Unable to split row into key and value", e );
    }
  }


  /**
   * Gets inputRowMetaJson
   *
   * @return value of inputRowMetaJson
   */
  public String getInputRowMetaJson() {
    return inputRowMetaJson;
  }

  /**
   * @param inputRowMetaJson The inputRowMetaJson to set
   */
  public void setInputRowMetaJson( String inputRowMetaJson ) {
    this.inputRowMetaJson = inputRowMetaJson;
  }

  /**
   * Gets keyFields
   *
   * @return value of keyFields
   */
  public String[] getKeyFields() {
    return keyFields;
  }

  /**
   * @param keyFields The keyFields to set
   */
  public void setKeyFields( String[] keyFields ) {
    this.keyFields = keyFields;
  }

  /**
   * Gets valueFields
   *
   * @return value of valueFields
   */
  public String[] getValueFields() {
    return valueFields;
  }

  /**
   * @param valueFields The valueFields to set
   */
  public void setValueFields( String[] valueFields ) {
    this.valueFields = valueFields;
  }


}
