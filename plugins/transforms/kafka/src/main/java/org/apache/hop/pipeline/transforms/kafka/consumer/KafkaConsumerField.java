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

import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Created by rfellows on 6/15/17.
 */
public class KafkaConsumerField {
  private static final Class<?> PKG = KafkaConsumerField.class; // For Translator

  private Name kafkaName;

  @Injection( name = "OUTPUT_NAME" )
  private String outputName;

  @Injection( name = "TYPE" )
  private Type outputType = Type.String;

  public KafkaConsumerField() {
  }

  public KafkaConsumerField( KafkaConsumerField orig ) {
    this.kafkaName = orig.kafkaName;
    this.outputName = orig.outputName;
    this.outputType = orig.outputType;
  }

  public KafkaConsumerField( Name kafkaName, String outputName ) {
    this( kafkaName, outputName, Type.String );
  }

  public KafkaConsumerField( Name kafkaName, String outputName, Type outputType ) {
    this.kafkaName = kafkaName;
    this.outputName = outputName;
    this.outputType = outputType;
  }

  public Name getKafkaName() {
    return kafkaName;
  }

  public void setKafkaName( Name kafkaName ) {
    this.kafkaName = kafkaName;
  }

  public String getOutputName() {
    return outputName;
  }

  public void setOutputName( String outputName ) {
    this.outputName = outputName;
  }

  public Type getOutputType() {
    return outputType;
  }

  public void setOutputType( Type outputType ) {
    this.outputType = outputType;
  }

  public enum Type {
    String( "String", IValueMeta.TYPE_STRING, StringSerializer.class.getName(), StringDeserializer.class.getName() ),
    Integer( "Integer", IValueMeta.TYPE_INTEGER, LongSerializer.class.getName(), LongDeserializer.class.getName() ),
    Binary( "Binary", IValueMeta.TYPE_BINARY, ByteArraySerializer.class.getName(), ByteArrayDeserializer.class.getName() ),
    Number( "Number", IValueMeta.TYPE_NUMBER, DoubleSerializer.class.getName(), DoubleDeserializer.class.getName() ),
    Avro( "Avro", IValueMeta.TYPE_STRING, "io.confluent.kafka.serializers.KafkaAvroSerializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer" );

    private final String value;
    private final int valueMetaInterfaceType;
    private String kafkaSerializerClass;
    private String kafkaDeserializerClass;

    Type( String value, int valueMetaInterfaceType, String kafkaSerializerClass, String kafkaDeserializerClass ) {
      this.value = value;
      this.valueMetaInterfaceType = valueMetaInterfaceType;
      this.kafkaSerializerClass = kafkaSerializerClass;
      this.kafkaDeserializerClass = kafkaDeserializerClass;
    }

    public String toString() {
      return value;
    }

    boolean isEqual( String value ) {
      return this.value.equals( value );
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

    public static Type fromValueMeta( IValueMeta vmi ) {
      if ( vmi != null ) {
        for ( Type t : Type.values() ) {
          if ( vmi.getType() == t.getIValueMetaType() ) {
            return t;
          }
        }
        throw new IllegalArgumentException( BaseMessages.getString( PKG, "KafkaConsumerField.Type.ERROR.NoIValueMetaMapping", vmi.getName(), vmi.getType() ) );
      }
      // if it's null, just default to string
      return String;
    }
  }

  public enum Name {
    KEY( "key" ) {
      @Override public void setFieldOnMeta( KafkaConsumerInputMeta meta, KafkaConsumerField field ) {
        meta.setKeyField( field );
      }

      @Override public KafkaConsumerField getFieldFromMeta( KafkaConsumerInputMeta meta ) {
        return meta.getKeyField();
      }
    },
    MESSAGE( "message" ) {
      @Override public void setFieldOnMeta( KafkaConsumerInputMeta meta, KafkaConsumerField field ) {
        meta.setMessageField( field );
      }

      @Override public KafkaConsumerField getFieldFromMeta( KafkaConsumerInputMeta meta ) {
        return meta.getMessageField();
      }
    },
    TOPIC( "topic" ) {
      @Override public void setFieldOnMeta( KafkaConsumerInputMeta meta, KafkaConsumerField field ) {
        meta.setTopicField( field );
      }

      @Override public KafkaConsumerField getFieldFromMeta( KafkaConsumerInputMeta meta ) {
        return meta.getTopicField();
      }
    },
    PARTITION( "partition" ) {
      @Override public void setFieldOnMeta( KafkaConsumerInputMeta meta, KafkaConsumerField field ) {
        meta.setPartitionField( field );
      }

      @Override public KafkaConsumerField getFieldFromMeta( KafkaConsumerInputMeta meta ) {
        return meta.getPartitionField();
      }
    },
    OFFSET( "offset" ) {
      @Override public void setFieldOnMeta( KafkaConsumerInputMeta meta, KafkaConsumerField field ) {
        meta.setOffsetField( field );
      }

      @Override public KafkaConsumerField getFieldFromMeta( KafkaConsumerInputMeta meta ) {
        return meta.getOffsetField();
      }
    },
    TIMESTAMP( "timestamp" ) {
      @Override public void setFieldOnMeta( KafkaConsumerInputMeta meta, KafkaConsumerField field ) {
        meta.setTimestampField( field );
      }

      @Override public KafkaConsumerField getFieldFromMeta( KafkaConsumerInputMeta meta ) {
        return meta.getTimestampField();
      }
    };

    private final String name;

    Name( String name ) {
      this.name = name;
    }

    public String toString() {
      return name.toString();
    }
    boolean isEqual( String name ) {
      return this.name.equals( name );
    }

    public abstract void setFieldOnMeta( KafkaConsumerInputMeta meta, KafkaConsumerField field );
    public abstract KafkaConsumerField getFieldFromMeta( KafkaConsumerInputMeta meta );
  }

}
