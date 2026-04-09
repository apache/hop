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

package org.apache.hop.avro.transforms.avrodecode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

/** Unit test for {@link AvroDecode} */
class AvroDecodeTests {

  @Test
  void testNullValue() {
    Schema schema = Schema.create(Schema.Type.STRING);
    Object result = AvroDecode.getStandardHopObject(field("f", schema), null);
    assertNull(result);
  }

  @Test
  void testNullType() {
    Schema schema = Schema.create(Schema.Type.NULL);
    Object result = AvroDecode.getStandardHopObject(field("f", schema), "anything");
    assertNull(result);
  }

  @Test
  void testEnum() {
    Schema schema = SchemaBuilder.enumeration("E").symbols("A", "B");
    GenericData.EnumSymbol enumVal = new GenericData.EnumSymbol(schema, "A");

    Object result = AvroDecode.getStandardHopObject(field("f", schema), enumVal);
    assertEquals("A", result);
  }

  @Test
  void testString() {
    Schema schema = Schema.create(Schema.Type.STRING);
    Utf8 utf8 = new Utf8("abc");

    Object result = AvroDecode.getStandardHopObject(field("f", schema), utf8);
    assertEquals("abc", result);
  }

  @Test
  void testBytes() {
    Schema schema = Schema.create(Schema.Type.BYTES);
    byte[] data = new byte[] {1, 2, 3};

    Object result = AvroDecode.getStandardHopObject(field("f", schema), data);
    assertSame(data, result);
  }

  @Test
  void testLongAndDouble() {
    assertEquals(
        123L, AvroDecode.getStandardHopObject(field("f", Schema.create(Schema.Type.LONG)), 123L));

    assertEquals(
        1.23, AvroDecode.getStandardHopObject(field("f", Schema.create(Schema.Type.DOUBLE)), 1.23));
  }

  @Test
  void testInt() {
    Object result = AvroDecode.getStandardHopObject(field("f", Schema.create(Schema.Type.INT)), 10);

    assertEquals(10L, result);
    assertInstanceOf(Long.class, result);
  }

  @Test
  void testFloat() {
    Object result =
        AvroDecode.getStandardHopObject(field("f", Schema.create(Schema.Type.FLOAT)), 1.5f);

    assertEquals(1.5d, result);
    assertInstanceOf(Double.class, result);
  }

  @Test
  void testBoolean() {
    Object result =
        AvroDecode.getStandardHopObject(field("f", Schema.create(Schema.Type.BOOLEAN)), true);
    assertEquals(true, result);
  }

  @Test
  void testRecord() {
    Schema schema = SchemaBuilder.record("R").fields().requiredString("f").endRecord();
    GenericRecord rec = new GenericData.Record(schema);
    rec.put("f", "v");

    Object result = AvroDecode.getStandardHopObject(field("f", schema), rec);
    assertEquals(rec.toString(), result);
  }

  @Test
  void testArray() {
    Schema schema = Schema.createArray(Schema.create(Schema.Type.STRING));
    GenericData.Array<String> array = new GenericData.Array<>(schema, List.of("a", "b"));

    Object result = AvroDecode.getStandardHopObject(field("f", schema), array);
    assertEquals(array.toString(), result);
  }

  @Test
  void testMap() {
    Schema schema = Schema.createMap(Schema.create(Schema.Type.STRING));
    Map<Utf8, Object> map = new HashMap<>();
    map.put(new Utf8("k"), "v");

    Object result = AvroDecode.getStandardHopObject(field("f", schema), map);
    assertEquals(map.toString(), result);
  }

  @Test
  void testUnion() {
    Schema unionSchema =
        Schema.createUnion(
            List.of(
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.LONG),
                Schema.create(Schema.Type.STRING)));

    Schema.Field f = field("f", unionSchema);

    // Long
    assertEquals(10L, AvroDecode.getStandardHopObject(f, 10L));
    // Double
    assertEquals(1.2, AvroDecode.getStandardHopObject(f, 1.2));
    // String
    assertEquals("abc", AvroDecode.getStandardHopObject(f, "abc"));
    // Boolean
    assertEquals(true, AvroDecode.getStandardHopObject(f, true));
    // byte[]
    byte[] bytes = new byte[] {1};
    assertSame(bytes, AvroDecode.getStandardHopObject(f, bytes));

    // Float → Double
    Object fResult = AvroDecode.getStandardHopObject(f, 1.5f);
    assertEquals(1.5d, fResult);

    // Integer → Long
    Object iResult = AvroDecode.getStandardHopObject(f, 5);
    assertEquals(5L, iResult);

    // other → toString
    Object other = new Object();
    assertEquals(other.toString(), AvroDecode.getStandardHopObject(f, other));
  }

  @Test
  void testFixed() {
    Schema schema = SchemaBuilder.fixed("F").size(4);
    GenericData.Fixed fixed = new GenericData.Fixed(schema, new byte[] {1, 2, 3, 4});

    Object result = AvroDecode.getStandardHopObject(field("A", schema), fixed);
    assertEquals(fixed.toString(), result);
  }

  @Test
  void testDefaultException() {
    assertThrows(
        Exception.class,
        () -> AvroDecode.getStandardHopObject(field("A", Schema.create(Schema.Type.ENUM)), null));
  }

  private Schema.Field field(String name, Schema schema) {
    return new Schema.Field(name, schema, null, null);
  }
}
