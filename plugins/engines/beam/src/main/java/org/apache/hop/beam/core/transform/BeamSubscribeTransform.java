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

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.BeamDefaults;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.PubsubMessageToHopRowFn;
import org.apache.hop.beam.core.fn.StringToHopRowFn;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;

/**
 * A transform to read data from a Google Cloud Platform PubSub topic
 */
public class BeamSubscribeTransform extends PTransform<PBegin, PCollection<HopRow>> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String transformName;
  private String subscription;
  private String topic;
  private String messageType;
  private String rowMetaJson;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamSubscribeTransform.class );

  private transient IRowMeta rowMeta;
  private transient Counter initCounter;
  private transient Counter inputCounter;
  private transient Counter writtenCounter;
  private transient Counter errorCounter;

  public BeamSubscribeTransform() {
  }

  public BeamSubscribeTransform( @Nullable String name, String transformName, String subscription, String topic, String messageType, String rowMetaJson, List<String> transformPluginClasses,
                                 List<String> xpPluginClasses ) {
    super( name );
    this.transformName = transformName;
    this.subscription = subscription;
    this.topic = topic;
    this.messageType = messageType;
    this.rowMetaJson = rowMetaJson;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Override
  public PCollection<HopRow> expand( PBegin input ) {

    try {
      if ( rowMeta == null ) {
        // Only initialize once on this node/vm
        //
        BeamHop.init( transformPluginClasses, xpPluginClasses );

        rowMeta = JsonRowMeta.fromJson( rowMetaJson );

        inputCounter = Metrics.counter( Pipeline.METRIC_NAME_INPUT, transformName );
        writtenCounter = Metrics.counter( Pipeline.METRIC_NAME_WRITTEN, transformName );

        Metrics.counter( Pipeline.METRIC_NAME_INIT, transformName ).inc();
      }

      // This stuff only outputs a single field.
      // It's either a Serializable or a String
      //
      PCollection<HopRow> output;

      if ( BeamDefaults.PUBSUB_MESSAGE_TYPE_STRING.equalsIgnoreCase( messageType ) ) {

        PubsubIO.Read<String> stringRead = PubsubIO.readStrings();
        if ( StringUtils.isNotEmpty(subscription)) {
          stringRead = stringRead.fromSubscription( subscription );
        } else {
          stringRead = stringRead.fromTopic( topic);
        }
        PCollection<String> stringPCollection = stringRead.expand( input );
        output = stringPCollection.apply( transformName, ParDo.of(
          new StringToHopRowFn( transformName, rowMetaJson, transformPluginClasses, xpPluginClasses )
        ) );

      } else if ( BeamDefaults.PUBSUB_MESSAGE_TYPE_MESSAGE.equalsIgnoreCase( messageType ) ) {

        PubsubIO.Read<PubsubMessage> messageRead = PubsubIO.readMessages();
        if (StringUtils.isNotEmpty( subscription )) {
          messageRead = messageRead.fromSubscription( subscription );
        } else {
          messageRead = messageRead.fromTopic( topic );
        }
        PCollection<PubsubMessage> messagesPCollection = messageRead.expand( input );
        output = messagesPCollection.apply( transformName, ParDo.of(
          new PubsubMessageToHopRowFn( transformName, rowMetaJson, transformPluginClasses, xpPluginClasses )
        ) );

      } else {
        throw new RuntimeException( "Unsupported message type: " + messageType );
      }

      return output;
    } catch ( Exception e ) {
      Metrics.counter( Pipeline.METRIC_NAME_ERROR, transformName ).inc();
      LOG.error( "Error in beam subscribe transform", e );
      throw new RuntimeException( "Error in beam subscribe transform", e );
    }
  }

  /**
   * Gets transformName
   *
   * @return value of transformName
   */
  public String getTransformName() {
    return transformName;
  }

  /**
   * @param transformName The transformName to set
   */
  public void setTransformName( String transformName ) {
    this.transformName = transformName;
  }

  /**
   * Gets topic
   *
   * @return value of topic
   */
  public String getTopic() {
    return topic;
  }

  /**
   * @param topic The topic to set
   */
  public void setTopic( String topic ) {
    this.topic = topic;
  }

  /**
   * Gets messageType
   *
   * @return value of messageType
   */
  public String getMessageType() {
    return messageType;
  }

  /**
   * @param messageType The messageType to set
   */
  public void setMessageType( String messageType ) {
    this.messageType = messageType;
  }
}
