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

package org.apache.hop.beam.core.transform;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.HopToBQTableRowFn;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BeamBQOutputTransform extends PTransform<PCollection<HopRow>, PDone> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String transformName;
  private String projectId;
  private String datasetId;
  private String tableId;
  private String rowMetaJson;
  private boolean createIfNeeded;
  private boolean truncateTable;
  private boolean failIfNotEmpty;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamBQOutputTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "BeamOutputError" );

  public BeamBQOutputTransform() {
  }

  public BeamBQOutputTransform( String transformName, String projectId, String datasetId, String tableId, boolean createIfNeeded, boolean truncateTable, boolean failIfNotEmpty, String rowMetaJson, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    this.transformName = transformName;
    this.projectId = projectId;
    this.datasetId = datasetId;
    this.tableId = tableId;
    this.createIfNeeded = createIfNeeded;
    this.truncateTable = truncateTable;
    this.failIfNotEmpty = failIfNotEmpty;
    this.rowMetaJson = rowMetaJson;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Override public PDone expand( PCollection<HopRow> input ) {

    try {
      // Only initialize once on this node/vm
      //
      BeamHop.init( transformPluginClasses, xpPluginClasses );

      // Inflate the metadata on the node where this is running...
      //
      IRowMeta rowMeta = JsonRowMeta.fromJson( rowMetaJson );


      // Which table do we write to?
      //
      TableReference tableReference = new TableReference();
      if ( StringUtils.isNotEmpty( projectId ) ) {
        tableReference.setProjectId( projectId );
      }
      tableReference.setDatasetId( datasetId );
      tableReference.setTableId( tableId );

      TableSchema tableSchema = new TableSchema();
      List<TableFieldSchema> schemaFields = new ArrayList<>();
      for ( IValueMeta valueMeta : rowMeta.getValueMetaList() ) {
        TableFieldSchema schemaField = new TableFieldSchema();
        schemaField.setName( valueMeta.getName() );
        switch(valueMeta.getType()){
          case IValueMeta.TYPE_STRING: schemaField.setType( "STRING" ); break;
          case IValueMeta.TYPE_INTEGER: schemaField.setType( "INTEGER" ); break;
          case IValueMeta.TYPE_DATE: schemaField.setType( "DATETIME" ); break;
          case IValueMeta.TYPE_BOOLEAN: schemaField.setType( "BOOLEAN" ); break;
          case IValueMeta.TYPE_NUMBER: schemaField.setType( "FLOAT" ); break;
          default:
            throw new RuntimeException( "Conversion from Hop value "+valueMeta.toString()+" to BigQuery TableRow isn't supported yet" );
        }
        schemaFields.add(schemaField);
      }
      tableSchema.setFields( schemaFields );

      SerializableFunction<HopRow, TableRow> formatFunction = new HopToBQTableRowFn( transformName, rowMetaJson, transformPluginClasses, xpPluginClasses );

      BigQueryIO.Write.CreateDisposition createDisposition;
      if (createIfNeeded) {
        createDisposition = BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
      }  else {
        createDisposition = BigQueryIO.Write.CreateDisposition.CREATE_NEVER;
      }

      BigQueryIO.Write.WriteDisposition writeDisposition;
      if (truncateTable) {
        writeDisposition = BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
      } else {
        if (failIfNotEmpty) {
          writeDisposition = BigQueryIO.Write.WriteDisposition.WRITE_EMPTY;
        } else {
          writeDisposition = BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
        }
      }

      BigQueryIO.Write<HopRow> bigQueryWrite = BigQueryIO
        .<HopRow>write()
        .to( tableReference )
        .withSchema( tableSchema )
        .withCreateDisposition( createDisposition )
        .withWriteDisposition( writeDisposition )
        .withFormatFunction( formatFunction );

      // TODO: pass the results along the way at some point
      //
      input.apply( transformName, bigQueryWrite );

      // End of the line
      //
      return PDone.in( input.getPipeline() );

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in Beam BigQuery output transform", e );
      throw new RuntimeException( "Error in Beam BigQuery output transform", e );
    }
  }
}
