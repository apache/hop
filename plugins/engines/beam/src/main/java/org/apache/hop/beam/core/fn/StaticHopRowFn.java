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
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.Pipeline;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.Date;
import java.util.List;

public class StaticHopRowFn extends DoFn<KV<byte[], byte[]>, HopRow> {

  private String transformName;
  private String rowMetaJson;
  private String rowDataXml;
  private boolean neverEnding;
  private int currentTimeFieldIndex;
  private int previousTimeFieldIndex;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;

  private transient RowMetaAndData rowMetaAndData;
  private transient Counter inputCounter;
  private transient Counter writtenCounter;
  private transient Date previousDate;

  public StaticHopRowFn( String transformName, String rowMetaJson, String rowDataXml, boolean neverEnding, int currentTimeFieldIndex, int previousTimeFieldIndex,
                         List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    this.transformName = transformName;
    this.rowMetaJson = rowMetaJson;
    this.rowDataXml = rowDataXml;
    this.neverEnding = neverEnding;
    this.currentTimeFieldIndex = currentTimeFieldIndex;
    this.previousTimeFieldIndex = previousTimeFieldIndex;
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

      IRowMeta rowMeta = JsonRowMeta.fromJson( rowMetaJson );
      Document document = XmlHandler.loadXmlString( rowDataXml );
      Node node = XmlHandler.getSubNode( document, RowMeta.XML_DATA_TAG );
      Object[] rowData = rowMeta.getRow( node );

      rowMetaAndData = new RowMetaAndData( rowMeta, rowData );

      Metrics.counter( org.apache.hop.pipeline.Pipeline.METRIC_NAME_INIT, transformName ).inc();
    } catch ( Exception e ) {
      Metrics.counter( org.apache.hop.pipeline.Pipeline.METRIC_NAME_ERROR, transformName ).inc();
      throw new RuntimeException( "Error in setup of converting row generator row into Hop rows", e );
    }
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {
    // Ignore the actual element here, we provide our own data driver
    //
    processContext.element();

    try {
      IRowMeta rowMeta = rowMetaAndData.getRowMeta();
      Object[] rowData = rowMetaAndData.getData();
      Object[] rowCopy = rowMeta.cloneRow( rowData );

      if ( neverEnding ) {

        Date currentDate = new Date();

        if ( currentTimeFieldIndex >= 0 ) {
          rowCopy[ currentTimeFieldIndex ] = currentDate;
        }
        if ( previousTimeFieldIndex >= 0 ) {
          rowCopy[ previousTimeFieldIndex ] = previousDate;
        }
        previousDate = currentDate;
      }

      processContext.output( new HopRow( rowCopy ) );
    } catch ( HopException e ) {
      throw new RuntimeException( "Unable to create copy of row", e );
    }
  }
}