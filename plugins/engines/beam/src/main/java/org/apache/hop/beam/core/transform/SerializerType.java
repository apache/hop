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
 *
 */

package org.apache.hop.beam.core.transform;

import org.apache.hop.core.row.IValueMeta;
import org.apache.kafka.common.serialization.*;

public enum SerializerType {
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
    private final int valueMetaInterfaceType;
    private String kafkaSerializerClass;
    private String kafkaDeserializerClass;

    SerializerType(
        String value,
        int valueMetaInterfaceType,
        String kafkaSerializerClass,
        String kafkaDeserializerClass) {
      this.value = value;
      this.valueMetaInterfaceType = valueMetaInterfaceType;
      this.kafkaSerializerClass = kafkaSerializerClass;
      this.kafkaDeserializerClass = kafkaDeserializerClass;
    }

    @Override
    public String toString() {
      return value;
    }

    boolean isEqual(String value) {
      return this.value.equals(value);
    }

    public int getIValueMetaType() {
      return valueMetaInterfaceType;
    }

    public String getKafkaSerializerClass() {
      return kafkaSerializerClass;
    }

    public String getKafkaDeserializerClass() {
      return kafkaDeserializerClass;
    }

    public static SerializerType fromValueMeta(IValueMeta vmi) {
      if (vmi != null) {
        for (SerializerType t : SerializerType.values()) {
          if (vmi.getType() == t.getIValueMetaType()) {
            return t;
          }
        }
        throw new IllegalArgumentException(
            "There is no known serializer type for value "+vmi.getName()+" and type "+vmi.getType());
      }
      // if it's null, just default to string
      return String;
    }
  }