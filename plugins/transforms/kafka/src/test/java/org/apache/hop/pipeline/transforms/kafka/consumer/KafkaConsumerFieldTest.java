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

import org.apache.hop.core.row.IValueMeta;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class KafkaConsumerFieldTest {
  KafkaConsumerField field;
  @Mock IValueMeta vmi;

  @Test
  public void testEmptyConstructor() throws Exception {
    field = new KafkaConsumerField();

    assertNull( field.getKafkaName() );
    assertNull( field.getOutputName() );
    assertEquals( KafkaConsumerField.Type.String, field.getOutputType() );
  }

  @Test
  public void testSettersGetters() throws Exception {
    field = new KafkaConsumerField();
    field.setKafkaName( KafkaConsumerField.Name.MESSAGE );
    field.setOutputName( "MSG" );
    field.setOutputType( KafkaConsumerField.Type.Integer );

    assertEquals( KafkaConsumerField.Name.MESSAGE, field.getKafkaName() );
    assertEquals( "MSG", field.getOutputName() );
    assertEquals( KafkaConsumerField.Type.Integer, field.getOutputType() );
  }

  @Test
  public void testConstructor_noType() throws Exception {
    field = new KafkaConsumerField( KafkaConsumerField.Name.KEY, "Test Name" );

    assertEquals( KafkaConsumerField.Name.KEY, field.getKafkaName() );
    assertEquals( "Test Name", field.getOutputName() );
    assertEquals( KafkaConsumerField.Type.String, field.getOutputType() );
  }

  @Test
  public void testConstructor_allProps() throws Exception {
    field = new KafkaConsumerField( KafkaConsumerField.Name.KEY, "Test Name", KafkaConsumerField.Type.Binary );

    assertEquals( KafkaConsumerField.Name.KEY, field.getKafkaName() );
    assertEquals( "Test Name", field.getOutputName() );
    assertEquals( KafkaConsumerField.Type.Binary, field.getOutputType() );
  }

  @Test
  public void testSerializersSet() throws Exception {
    field = new KafkaConsumerField( KafkaConsumerField.Name.KEY, "Test Name" );
    assertEquals( "org.apache.kafka.common.serialization.StringSerializer", field.getOutputType().getKafkaSerializerClass() );
    assertEquals( "org.apache.kafka.common.serialization.StringDeserializer", field.getOutputType().getKafkaDeserializerClass() );

    field = new KafkaConsumerField( KafkaConsumerField.Name.KEY, "Test Name", KafkaConsumerField.Type.Integer );
    assertEquals( "org.apache.kafka.common.serialization.LongSerializer", field.getOutputType().getKafkaSerializerClass() );
    assertEquals( "org.apache.kafka.common.serialization.LongDeserializer", field.getOutputType().getKafkaDeserializerClass() );

    field = new KafkaConsumerField( KafkaConsumerField.Name.KEY, "Test Name", KafkaConsumerField.Type.Binary );
    assertEquals( "org.apache.kafka.common.serialization.ByteArraySerializer", field.getOutputType().getKafkaSerializerClass() );
    assertEquals( "org.apache.kafka.common.serialization.ByteArrayDeserializer", field.getOutputType().getKafkaDeserializerClass() );

    field = new KafkaConsumerField( KafkaConsumerField.Name.KEY, "Test Name", KafkaConsumerField.Type.Number );
    assertEquals( "org.apache.kafka.common.serialization.DoubleSerializer", field.getOutputType().getKafkaSerializerClass() );
    assertEquals( "org.apache.kafka.common.serialization.DoubleDeserializer", field.getOutputType().getKafkaDeserializerClass() );
  }

  @Test
  public void testFromIValueMeta() throws Exception {
    when( vmi.getType() ).thenReturn( IValueMeta.TYPE_STRING );
    KafkaConsumerField.Type t = KafkaConsumerField.Type.fromValueMeta( vmi );
    assertEquals( "String", t.toString() );

    when( vmi.getType() ).thenReturn( IValueMeta.TYPE_INTEGER );
    t = KafkaConsumerField.Type.fromValueMeta( vmi );
    assertEquals( "Integer", t.toString() );

    when( vmi.getType() ).thenReturn( IValueMeta.TYPE_BINARY );
    t = KafkaConsumerField.Type.fromValueMeta( vmi );
    assertEquals( "Binary", t.toString() );

    when( vmi.getType() ).thenReturn( IValueMeta.TYPE_NUMBER );
    t = KafkaConsumerField.Type.fromValueMeta( vmi );
    assertEquals( "Number", t.toString() );
  }
}