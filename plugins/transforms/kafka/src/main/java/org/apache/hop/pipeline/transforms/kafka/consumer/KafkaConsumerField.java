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

package org.apache.hop.pipeline.transforms.kafka.consumer;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

@Getter
@Setter
public class KafkaConsumerField {
  private static final Class<?> PKG = KafkaConsumerField.class;

  protected Name kafkaName;

  protected String outputName;

  protected Type outputType = Type.String;

  public KafkaConsumerField() {}

  public KafkaConsumerField(KafkaConsumerField orig) {
    this.kafkaName = orig.kafkaName;
    this.outputName = orig.outputName;
    this.outputType = orig.outputType;
  }

  public KafkaConsumerField(Name kafkaName, String outputName) {
    this(kafkaName, outputName, Type.String);
  }

  public KafkaConsumerField(Name kafkaName, String outputName, Type outputType) {
    this.kafkaName = kafkaName;
    this.outputName = outputName;
    this.outputType = outputType;
  }

  @Getter
  public enum Type implements IEnumHasCode {
    String(
        "String",
        IValueMeta.TYPE_STRING,
        StringSerializer.class.getName(),
        StringDeserializer.class.getName()),
    Integer(
        "Integer",
        IValueMeta.TYPE_INTEGER,
        LongSerializer.class.getName(),
        LongDeserializer.class.getName()),
    Binary(
        "Binary",
        IValueMeta.TYPE_BINARY,
        ByteArraySerializer.class.getName(),
        ByteArrayDeserializer.class.getName()),
    Number(
        "Number",
        IValueMeta.TYPE_NUMBER,
        DoubleSerializer.class.getName(),
        DoubleDeserializer.class.getName()),
    Avro(
        "Avro",
        IValueMeta.TYPE_AVRO,
        "io.confluent.kafka.serializers.KafkaAvroSerializer",
        "io.confluent.kafka.serializers.KafkaAvroDeserializer");

    private final String value;
    private final int valueMetaType;
    private final String kafkaSerializerClass;
    private final String kafkaDeserializerClass;

    Type(
        String value,
        int valueMetaType,
        String kafkaSerializerClass,
        String kafkaDeserializerClass) {
      this.value = value;
      this.valueMetaType = valueMetaType;
      this.kafkaSerializerClass = kafkaSerializerClass;
      this.kafkaDeserializerClass = kafkaDeserializerClass;
    }

    @Override
    public String getCode() {
      return value;
    }

    public static Type fromValueMeta(IValueMeta vmi) {
      if (vmi != null) {
        for (Type t : Type.values()) {
          if (vmi.getType() == t.getValueMetaType()) {
            return t;
          }
        }
        throw new IllegalArgumentException(
            BaseMessages.getString(
                PKG,
                "KafkaConsumerField.Type.ERROR.NoIValueMetaMapping",
                vmi.getName(),
                vmi.getType()));
      }
      // if it's null, just default to string
      return String;
    }
  }

  @Getter
  public enum Name implements IEnumHasCode {
    KEY("key"),
    MESSAGE("message"),
    TOPIC("topic"),
    PARTITION("partition"),
    OFFSET("offset"),
    TIMESTAMP("timestamp");

    private final String code;

    Name(String code) {
      this.code = code;
    }
  }
}
