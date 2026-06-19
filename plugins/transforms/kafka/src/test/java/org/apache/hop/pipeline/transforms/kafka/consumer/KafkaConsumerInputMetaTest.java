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

package org.apache.hop.pipeline.transforms.kafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.apache.hop.pipeline.transforms.kafka.shared.KafkaOption;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

class KafkaConsumerInputMetaTest {

  @Test
  void testKeySerialization() throws Exception {
    KafkaConsumerInputMeta.KeyConsumerField key = new KafkaConsumerInputMeta.KeyConsumerField();
    key.setOutputName("Key");
    key.setOutputType(KafkaConsumerField.Type.Avro);

    String xml = XmlHandler.aroundTag("key", XmlMetadataUtil.serializeObjectToXml(key));
    Node node = XmlHandler.loadXmlString(xml, "key");
    assertEquals("Key", XmlHandler.getTagValue(node, "outputName"));
    assertEquals("Avro", XmlHandler.getTagValue(node, "type"));

    KafkaConsumerInputMeta.KeyConsumerField copy =
        XmlMetadataUtil.deSerializeFromXml(
            node, KafkaConsumerInputMeta.KeyConsumerField.class, new MemoryMetadataProvider());
    assertEquals("Key", copy.getOutputName());
    assertEquals(KafkaConsumerField.Type.Avro, copy.getOutputType());
    assertEquals(KafkaConsumerField.Name.KEY, copy.getKafkaName());
  }

  @Test
  void testMessageSerialization() throws Exception {
    KafkaConsumerInputMeta.MessageConsumerField message =
        new KafkaConsumerInputMeta.MessageConsumerField();
    message.setOutputName("Message");
    message.setOutputType(KafkaConsumerField.Type.Avro);

    String xml = XmlHandler.aroundTag("message", XmlMetadataUtil.serializeObjectToXml(message));
    Node node = XmlHandler.loadXmlString(xml, "message");
    assertEquals("Message", XmlHandler.getTagValue(node, "outputName"));
    assertEquals("Avro", XmlHandler.getTagValue(node, "type"));

    KafkaConsumerInputMeta.MessageConsumerField copy =
        XmlMetadataUtil.deSerializeFromXml(
            node, KafkaConsumerInputMeta.MessageConsumerField.class, new MemoryMetadataProvider());
    assertEquals("Message", copy.getOutputName());
    assertEquals(KafkaConsumerField.Type.Avro, copy.getOutputType());
    assertEquals(KafkaConsumerField.Name.MESSAGE, copy.getKafkaName());
  }

  @Test
  void testTopicSerialization() throws Exception {
    KafkaConsumerInputMeta.TopicConsumerField topic =
        new KafkaConsumerInputMeta.TopicConsumerField("TheTopic");

    String xml = XmlHandler.aroundTag("topicField", XmlMetadataUtil.serializeObjectToXml(topic));
    Node node = XmlHandler.loadXmlString(xml, "topicField");
    assertEquals("TheTopic", XmlHandler.getTagValue(node, "outputName"));
    // Don't serialize type and name since these are not modifiable.
    assertNull(XmlHandler.getTagValue(node, "type"));
    assertNull(XmlHandler.getTagValue(node, "kafkaName"));

    KafkaConsumerInputMeta.TopicConsumerField copy =
        XmlMetadataUtil.deSerializeFromXml(
            node, KafkaConsumerInputMeta.TopicConsumerField.class, new MemoryMetadataProvider());
    assertEquals("TheTopic", copy.getOutputName());
    assertEquals(KafkaConsumerField.Type.String, copy.getOutputType());
    assertEquals(KafkaConsumerField.Name.TOPIC, copy.getKafkaName());
  }

  @Test
  void testSerialization() throws Exception {
    KafkaConsumerInputMeta meta = new KafkaConsumerInputMeta();
    meta.setFilename("filename");
    meta.getTopics().addAll(List.of("topic1", "topic2"));
    meta.setSubTransform("transform-name");
    meta.setExecutionInformationLocation("location");
    meta.setExecutionDataProfile("profile");
    meta.setBatchSize("111");
    meta.setBatchDuration("222");
    meta.setAutoCommit(true);
    meta.getOptions().clear();
    meta.getOptions().add(new KafkaOption("auto.offset.reset", "latest"));
    meta.getOptions().add(new KafkaOption("ssl.key.password", ""));
    meta.getOptions().add(new KafkaOption("ssl.keystore.location", ""));
    meta.getOptions().add(new KafkaOption("ssl.keystore.password", ""));
    meta.getOptions().add(new KafkaOption("ssl.truststore.location", ""));
    meta.getOptions().add(new KafkaOption("ssl.truststore.password", ""));

    meta.setKeyField(new KafkaConsumerInputMeta.KeyConsumerField());
    meta.getKeyField().setOutputType(KafkaConsumerField.Type.Binary);
    meta.getKeyField().setOutputName("message-key");

    meta.setMessageField(new KafkaConsumerInputMeta.MessageConsumerField());
    meta.getMessageField().setOutputType(KafkaConsumerField.Type.Avro);
    meta.getMessageField().setOutputName("avro-message");

    meta.setTopicField(new KafkaConsumerInputMeta.TopicConsumerField("message-topic"));
    meta.setPartitionField(new KafkaConsumerInputMeta.PartitionConsumerField("message-partition"));
    meta.setOffsetField(new KafkaConsumerInputMeta.OffsetConsumerField("message-offset"));
    meta.setTimestampField(new KafkaConsumerInputMeta.TimestampConsumerField("message-timestamp"));

    meta.getMessageField().setOutputType(KafkaConsumerField.Type.Avro);
    meta.getMessageField().setOutputName("avro-message");

    String xml = XmlHandler.aroundTag("transform", XmlMetadataUtil.serializeObjectToXml(meta));
    Node node = XmlHandler.loadXmlString(xml, "transform");
    KafkaConsumerInputMeta copy =
        XmlMetadataUtil.deSerializeFromXml(
            node, KafkaConsumerInputMeta.class, new MemoryMetadataProvider());

    assertEquals(6, copy.getOptions().size());
    assertEquals("auto.offset.reset", copy.getOptions().getFirst().getProperty());
    assertEquals("ssl.truststore.password", copy.getOptions().getLast().getProperty());
    assertEquals("filename", copy.getFilename());
    assertEquals("transform-name", copy.getSubTransform());
    assertEquals("location", copy.getExecutionInformationLocation());
    assertEquals("profile", copy.getExecutionDataProfile());
    assertEquals("111", copy.getBatchSize());
    assertEquals("222", copy.getBatchDuration());
    assertTrue(copy.getTopics().contains("topic1"));
    assertTrue(copy.getTopics().contains("topic2"));

    KafkaConsumerField f = copy.getKeyField();
    assertEquals("message-key", f.getOutputName());
    assertEquals(KafkaConsumerField.Type.Binary, f.getOutputType());
    assertEquals(KafkaConsumerField.Name.KEY, f.getKafkaName());

    f = copy.getMessageField();
    assertEquals("avro-message", f.getOutputName());
    assertEquals(KafkaConsumerField.Type.Avro, f.getOutputType());
    assertEquals(KafkaConsumerField.Name.MESSAGE, f.getKafkaName());

    f = copy.getTopicField();
    assertEquals("message-topic", f.getOutputName());
    assertEquals(KafkaConsumerField.Type.String, f.getOutputType());
    assertEquals(KafkaConsumerField.Name.TOPIC, f.getKafkaName());

    f = copy.getPartitionField();
    assertEquals("message-partition", f.getOutputName());
    assertEquals(KafkaConsumerField.Type.Integer, f.getOutputType());
    assertEquals(KafkaConsumerField.Name.PARTITION, f.getKafkaName());

    f = copy.getOffsetField();
    assertEquals("message-offset", f.getOutputName());
    assertEquals(KafkaConsumerField.Type.Integer, f.getOutputType());
    assertEquals(KafkaConsumerField.Name.OFFSET, f.getKafkaName());

    f = copy.getTimestampField();
    assertEquals("message-timestamp", f.getOutputName());
    assertEquals(KafkaConsumerField.Type.Integer, f.getOutputType());
    assertEquals(KafkaConsumerField.Name.TIMESTAMP, f.getKafkaName());
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    KafkaConsumerInputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/kafka-consumer-input.xml", KafkaConsumerInputMeta.class);

    assertTrue(meta.getTopics().contains("${KAFKA_TOPIC}"));
    assertTrue(StringUtils.isEmpty(meta.getConsumerGroup()));
    assertEquals("${PROJECT_HOME}/kafka-consumer-child.hpl", meta.getFilename());
    assertEquals("tmp-executions", meta.getExecutionInformationLocation());
    assertEquals("first-last", meta.getExecutionDataProfile());
    assertEquals("OUTPUT", meta.getSubTransform());
    assertEquals("10", meta.getBatchSize());
    assertEquals("10000", meta.getBatchDuration());
    assertEquals("${KAFKA_SERVER}", meta.getDirectBootstrapServers());
    assertFalse(meta.isAutoCommit());

    assertEquals(6, meta.getOptions().size());
    assertEquals("auto.offset.reset", meta.getOptions().getFirst().getProperty());
    assertEquals("ssl.truststore.password", meta.getOptions().getLast().getProperty());

    KafkaConsumerField f = meta.getKeyField();
    assertEquals("Key", f.getOutputName());
    assertEquals(KafkaConsumerField.Type.String, f.getOutputType());
    assertEquals(KafkaConsumerField.Name.KEY, f.getKafkaName());

    f = meta.getMessageField();
    assertEquals("Message", f.getOutputName());
    assertEquals(KafkaConsumerField.Type.String, f.getOutputType());
    assertEquals(KafkaConsumerField.Name.MESSAGE, f.getKafkaName());

    f = meta.getTopicField();
    assertEquals("Topic", f.getOutputName());
    assertEquals(KafkaConsumerField.Type.String, f.getOutputType());
    assertEquals(KafkaConsumerField.Name.TOPIC, f.getKafkaName());

    f = meta.getPartitionField();
    assertEquals("Partition", f.getOutputName());
    assertEquals(KafkaConsumerField.Type.Integer, f.getOutputType());
    assertEquals(KafkaConsumerField.Name.PARTITION, f.getKafkaName());

    f = meta.getOffsetField();
    assertEquals("Offset", f.getOutputName());
    assertEquals(KafkaConsumerField.Type.Integer, f.getOutputType());
    assertEquals(KafkaConsumerField.Name.OFFSET, f.getKafkaName());

    f = meta.getTimestampField();
    assertEquals("Timestamp", f.getOutputName());
    assertEquals(KafkaConsumerField.Type.Integer, f.getOutputType());
    assertEquals(KafkaConsumerField.Name.TIMESTAMP, f.getKafkaName());
  }
}
