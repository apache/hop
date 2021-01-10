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
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hop.beam.core.BeamDefaults;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.PublishMessagesFn;
import org.apache.hop.beam.core.fn.PublishStringsFn;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BeamPublishTransform extends PTransform<PCollection<HopRow>, PDone> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String transformName;
  private String topic;
  private String messageType;
  private String messageField;
  private String rowMetaJson;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamPublishTransform.class );

  private transient IRowMeta rowMeta;
  private transient Counter initCounter;
  private transient Counter readCounter;
  private transient Counter outputCounter;
  private transient Counter errorCounter;
  private transient int fieldIndex;

  public BeamPublishTransform() {
  }

  public BeamPublishTransform( String transformName, String topic, String messageType, String messageField, String rowMetaJson, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    this.transformName = transformName;
    this.topic = topic;
    this.messageType = messageType;
    this.messageField = messageField;
    this.rowMetaJson = rowMetaJson;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Override public PDone expand( PCollection<HopRow> input ) {

    try {

      if (rowMeta==null) {
        // Only initialize once on this node/vm
        //
        BeamHop.init( transformPluginClasses, xpPluginClasses );

        // Inflate the metadata on the node where this is running...
        //
        IRowMeta rowMeta = JsonRowMeta.fromJson( rowMetaJson );

        rowMeta = JsonRowMeta.fromJson( rowMetaJson );

        initCounter = Metrics.counter( Pipeline.METRIC_NAME_INIT, transformName );
        readCounter = Metrics.counter( Pipeline.METRIC_NAME_READ, transformName );
        outputCounter = Metrics.counter( Pipeline.METRIC_NAME_OUTPUT, transformName );
        errorCounter = Metrics.counter( Pipeline.METRIC_NAME_ERROR, transformName );

        fieldIndex = rowMeta.indexOfValue( messageField );
        if (fieldIndex<0) {
          throw new RuntimeException( "Field '"+messageField+"' couldn't be found in the input row: "+rowMeta.toString() );
        }

        initCounter.inc();
      }

      // String messages...
      //
      if ( BeamDefaults.PUBSUB_MESSAGE_TYPE_STRING.equalsIgnoreCase( messageType ) ) {

        PublishStringsFn stringsFn = new PublishStringsFn( transformName, fieldIndex, rowMetaJson, transformPluginClasses, xpPluginClasses );
        PCollection<String> stringPCollection = input.apply( transformName, ParDo.of(stringsFn) );
        PDone done = PubsubIO.writeStrings().to( topic ).expand( stringPCollection );
        return done;
      }

      // PubsubMessages
      //
      if ( BeamDefaults.PUBSUB_MESSAGE_TYPE_MESSAGE.equalsIgnoreCase( messageType ) ) {
        PublishMessagesFn messagesFn = new PublishMessagesFn( transformName, fieldIndex, rowMetaJson, transformPluginClasses, xpPluginClasses );
        PCollection<PubsubMessage> messagesPCollection = input.apply( ParDo.of( messagesFn ) );
        PDone done = PubsubIO.writeMessages().to( topic ).expand( messagesPCollection );
        return done;
      }

      throw new RuntimeException( "Message type '"+messageType+"' is not yet supported" );

    } catch ( Exception e ) {
      errorCounter.inc();
      LOG.error( "Error in beam publish transform", e );
      throw new RuntimeException( "Error in beam publish transform", e );
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

  /**
   * Gets messageField
   *
   * @return value of messageField
   */
  public String getMessageField() {
    return messageField;
  }

  /**
   * @param messageField The messageField to set
   */
  public void setMessageField( String messageField ) {
    this.messageField = messageField;
  }
}
