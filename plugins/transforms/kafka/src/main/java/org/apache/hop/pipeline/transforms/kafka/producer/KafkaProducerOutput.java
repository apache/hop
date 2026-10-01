/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.kafka.producer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.kafka.consumer.KafkaConsumerField;
import org.apache.hop.pipeline.transforms.kafka.shared.KafkaFactory;
import org.apache.hop.pipeline.transforms.kafka.shared.KafkaHeaders;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

public class KafkaProducerOutput
    extends BaseTransform<KafkaProducerOutputMeta, KafkaProducerOutputData> {

  private static final Class<?> PKG = KafkaProducerOutputMeta.class;

  private KafkaFactory kafkaFactory;

  public KafkaProducerOutput(
      TransformMeta transformMeta,
      KafkaProducerOutputMeta meta,
      KafkaProducerOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline trans) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, trans);
    setKafkaFactory(KafkaFactory.defaultFactory());
  }

  void setKafkaFactory(KafkaFactory factory) {
    this.kafkaFactory = factory;
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow(); // get row, set busy!
    if (r == null) {
      // no more input to be expected...
      setOutputDone();
      if (data.kafkaProducer != null) {
        data.kafkaProducer.close();
      }
      return false;
    }
    if (first) {
      data.keyFieldIndex = getInputRowMeta().indexOfValue(resolve(meta.getKeyField()));
      data.messageFieldIndex = getInputRowMeta().indexOfValue(resolve(meta.getMessageField()));
      data.keyValueMeta = getInputRowMeta().getValueMeta(data.keyFieldIndex);
      data.msgValueMeta = getInputRowMeta().getValueMeta(data.messageFieldIndex);

      if (meta.isTopicInField()) {
        String topicField = resolve(meta.getTopicField());
        if (StringUtils.isEmpty(topicField)) {
          throw new HopException(
              BaseMessages.getString(PKG, "KafkaProducerOutput.Error.NoTopicFieldSpecified"));
        }
        data.topicFieldIndex = getInputRowMeta().indexOfValue(topicField);
        if (data.topicFieldIndex < 0) {
          throw new HopException(
              BaseMessages.getString(
                  PKG, "KafkaProducerOutput.Error.TopicFieldNotFound", topicField));
        }
      } else {
        data.topicFieldIndex = -1;
        data.topic = resolve(meta.getTopic());
      }

      data.headersFieldIndex = -1;
      String headersField = resolve(meta.getHeadersField());
      if (StringUtils.isNotEmpty(headersField)) {
        data.headersFieldIndex = getInputRowMeta().indexOfValue(headersField);
        if (data.headersFieldIndex < 0) {
          throw new HopException(
              BaseMessages.getString(
                  PKG, "KafkaProducerOutput.Error.HeadersFieldNotFound", headersField));
        }
      }

      data.kafkaProducer =
          kafkaFactory.producer(
              meta,
              this::resolve,
              KafkaConsumerField.Type.fromValueMeta(data.keyValueMeta),
              KafkaConsumerField.Type.fromValueMeta(data.msgValueMeta));

      data.isOpen = true;

      first = false;
    }

    if (!data.isOpen) {
      return false;
    }
    String topic = resolveTopic(r);
    Iterable<Header> headers = resolveHeaders(r);

    ProducerRecord<Object, Object> producerRecord;
    // allow for null keys
    if (data.keyFieldIndex < 0
        || getInputRowMeta().isNull(r, data.keyFieldIndex)
        || StringUtils.isEmpty(r[data.keyFieldIndex].toString())) {
      // Explicit casts pick the (topic, partition, timestamp, key, value, headers) constructor;
      // bare nulls leave it ambiguous against the shorter overloads.
      producerRecord =
          new ProducerRecord<>(
              topic, (Integer) null, (Long) null, null, r[data.messageFieldIndex], headers);
    } else {

      Object nativeObject =
          getInputRowMeta()
              .getValueMeta(data.messageFieldIndex)
              .getNativeDataType(r[data.messageFieldIndex]);

      producerRecord =
          new ProducerRecord<>(
              topic,
              (Integer) null,
              (Long) null,
              getInputRowMeta().getString(r, data.keyFieldIndex),
              nativeObject,
              headers);
    }

    data.kafkaProducer.send(producerRecord);
    incrementLinesOutput();

    putRow(getInputRowMeta(), r); // copy row to possible alternate rowset(s).

    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic(BaseMessages.getString(PKG, "KafkaConsumerOutput.Log.LineNumber") + getLinesRead());
    }

    return true;
  }

  /**
   * Determines the topic the given row is sent to. With "topic from field" disabled this is the
   * configured topic, resolved once when the first row arrives. With it enabled the topic is read
   * from the row itself, so a single transform can fan rows out across topics instead of needing
   * one Kafka Producer per topic.
   *
   * @param r the current input row
   * @return the topic name for this row
   * @throws HopException if the row carries no usable topic
   */
  private String resolveTopic(Object[] r) throws HopException {
    if (data.topicFieldIndex < 0) {
      return data.topic;
    }
    String topic = getInputRowMeta().getString(r, data.topicFieldIndex);
    if (StringUtils.isEmpty(topic)) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "KafkaProducerOutput.Error.EmptyTopicInField",
              getInputRowMeta().getValueMeta(data.topicFieldIndex).getName(),
              getLinesRead()));
    }
    return topic;
  }

  /**
   * Reads the record headers for a row from the configured field, which holds the same JSON array
   * of {@code {"name":..,"value":..}} objects the Kafka Consumer produces, so headers survive a
   * consume-then-produce round trip.
   *
   * @param r the current input row
   * @return the headers to attach, or null when no headers field is configured or the value is
   *     empty
   * @throws HopException if the field does not hold a usable headers document
   */
  private Iterable<Header> resolveHeaders(Object[] r) throws HopException {
    if (data.headersFieldIndex < 0) {
      return null;
    }
    String json = getInputRowMeta().getString(r, data.headersFieldIndex);
    if (StringUtils.isBlank(json)) {
      return null;
    }
    return KafkaHeaders.fromJson(json);
  }

  @Override
  public void stopRunning() {
    if (data.kafkaProducer != null && data.isOpen) {
      data.isOpen = false;
      data.kafkaProducer.flush();
      data.kafkaProducer.close();
    }
  }
}
