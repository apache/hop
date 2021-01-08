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

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.HopRowToKVStringStringFn;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BeamKafkaOutputTransform extends PTransform<PCollection<HopRow>, PDone> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String transformName;
  private String bootstrapServers;
  private String topic;
  private String keyField;
  private String messageField;
  private String rowMetaJson;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamKafkaOutputTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "BeamKafkaOutputError" );

  public BeamKafkaOutputTransform() {
  }

  public BeamKafkaOutputTransform( String transformName, String bootstrapServers, String topic, String keyField, String messageField, String rowMetaJson, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    this.transformName = transformName;
    this.bootstrapServers = bootstrapServers;
    this.topic = topic;
    this.keyField = keyField;
    this.messageField = messageField;
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

      int keyIndex = rowMeta.indexOfValue( keyField );
      if (keyIndex<0) {
        throw new HopException( "Unable to find key field "+keyField+" in input row: "+rowMeta.toString() );
      }
      int messageIndex = rowMeta.indexOfValue( messageField );
      if (messageIndex<0) {
        throw new HopException( "Unable to find message field "+messageField+" in input row: "+rowMeta.toString() );
      }

      // First convert the input stream of HopRows to KV<String,String> for the keys and messages
      //
      HopRowToKVStringStringFn hopRowToKVStringStringFn = new HopRowToKVStringStringFn( transformName, keyIndex, messageIndex, rowMetaJson, transformPluginClasses, xpPluginClasses );

      // Then write to Kafka topic
      //
      KafkaIO.Write<String, String> stringsToKafka = KafkaIO.<String, String>write()
        .withBootstrapServers( bootstrapServers )
        .withTopic( topic )
        .withKeySerializer( StringSerializer.class )
        .withValueSerializer( StringSerializer.class );
      // TODO: add features like compression
      //

      PCollection<KV<String, String>> kvpCollection = input.apply( ParDo.of( hopRowToKVStringStringFn ) );
      return kvpCollection.apply( stringsToKafka );
    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in Beam Kafka output transform", e );
      throw new RuntimeException( "Error in Beam Kafka output transform", e );
    }
  }
}
