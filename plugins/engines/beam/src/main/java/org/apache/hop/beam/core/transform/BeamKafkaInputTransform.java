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
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.KVStringStringToHopRowFn;
import org.apache.hop.beam.transforms.kafka.ConfigOption;
import org.apache.hop.core.Const;
import org.apache.hop.core.row.IRowMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BeamKafkaInputTransform extends PTransform<PBegin, PCollection<HopRow>> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String transformName;
  private String bootstrapServers;
  private String topics;
  private String groupId;
  private boolean usingProcessingTime; // default
  private boolean usingLogAppendTime;
  private boolean usingCreateTime;
  private boolean restrictedToCommitted;
  private boolean allowingCommitOnConsumedOffset;
  private List<ConfigOption> configOptions;

  private String rowMetaJson;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamKafkaInputTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "BeamKafkaInputError" );

  private transient IRowMeta rowMeta;

  public BeamKafkaInputTransform() {
  }

  public BeamKafkaInputTransform( @Nullable String name, String transformName, String bootstrapServers, String topics, String groupId,
                                  boolean usingProcessingTime, boolean usingLogAppendTime, boolean usingCreateTime,
                                  boolean restrictedToCommitted, boolean allowingCommitOnConsumedOffset,
                                  String[] configOptionParameters, String[] configOptionValues, String configOptionTypes[],
                                  String rowMetaJson, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    super( name );
    this.transformName = transformName;
    this.bootstrapServers = bootstrapServers;
    this.topics = topics;
    this.groupId = groupId;
    this.usingProcessingTime = usingProcessingTime;
    this.usingLogAppendTime = usingLogAppendTime;
    this.usingCreateTime = usingCreateTime;
    this.restrictedToCommitted = restrictedToCommitted;
    this.allowingCommitOnConsumedOffset = allowingCommitOnConsumedOffset;
    this.configOptions = new ArrayList<>(  );
    for (int i=0;i<configOptionParameters.length;i++) {
      this.configOptions.add(new ConfigOption( configOptionParameters[i], configOptionValues[i], ConfigOption.Type.getTypeFromName( configOptionTypes[i] ) ));
    }
    this.rowMetaJson = rowMetaJson;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Override public PCollection<HopRow> expand( PBegin input ) {
    try {
      // Only initialize once on this node/vm
      //
      BeamHop.init( transformPluginClasses, xpPluginClasses );

      // What's the list of topics?
      //
      List<String> topicList = new ArrayList<>();
      for ( String topic : topics.split( "," ) ) {
        topicList.add( Const.trim( topic ) );
      }

      // TODO: add custom configuration options to this map:
      Map<String, Object> consumerConfigUpdates = new HashMap<>(  );
      consumerConfigUpdates.put( "group.id", groupId );
      for (ConfigOption configOption : configOptions) {
        Object value;
        String optionValue = configOption.getValue();
        switch(configOption.getType()) {
          case String:value=optionValue; break;
          case Short: value=Short.valueOf( optionValue ); break;
          case Int: value = Integer.valueOf( optionValue ); break;
          case Long: value = Long.valueOf( optionValue ); break;
          case Double: value = Double.valueOf( optionValue ); break;
          case Boolean: value = Boolean.valueOf( optionValue ); break;
          default:
            throw new RuntimeException( "Config option parameter "+configOption.getParameter()+" uses unsupported type "+configOption.getType().name() );
        }
        consumerConfigUpdates.put(configOption.getParameter(), value);
      }

      KafkaIO.Read<String, String> io = KafkaIO.<String, String>read()
        .withBootstrapServers( bootstrapServers )
        .withConsumerConfigUpdates( consumerConfigUpdates )
        .withTopics( topicList )
        .withKeyDeserializer( StringDeserializer.class )
        .withValueDeserializer( StringDeserializer.class );

      if (usingProcessingTime) {
        io = io.withProcessingTime();
      }
      if (usingLogAppendTime) {
        io = io.withLogAppendTime();
      }
      if (usingCreateTime) {
        io = io.withCreateTime( Duration.ZERO ); // TODO Configure this
      }

      if (restrictedToCommitted) {
        io = io.withReadCommitted();
      }
      if (allowingCommitOnConsumedOffset) {
        io = io.commitOffsetsInFinalize();
      }

      // Read keys and values from Kafka
      //
      PCollection<KV<String, String>> kafkaConsumerOutput = input.apply( io.withoutMetadata() );

      // Now convert this into Hop rows with a single String value in them
      //
      PCollection<HopRow> output = kafkaConsumerOutput.apply(
        ParDo.of(new KVStringStringToHopRowFn( transformName, rowMetaJson, transformPluginClasses, xpPluginClasses ))
      );

      return output;

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in Kafka input transform", e );
      throw new RuntimeException( "Error in Kafka input transform", e );
    }
  }
}
