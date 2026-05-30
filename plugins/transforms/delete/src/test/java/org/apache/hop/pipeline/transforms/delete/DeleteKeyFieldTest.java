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

package org.apache.hop.pipeline.transforms.delete;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

/** Unit test for {@link DeleteKeyField} */
class DeleteKeyFieldTest {

  @Test
  void testDefaultConstructor() {
    DeleteKeyField field = new DeleteKeyField();
    assertNotNull(field);
  }

  @Test
  void testParameterizedConstructorAndGetters() {
    DeleteKeyField field = new DeleteKeyField("id", "=", "streamId", "streamId2");
    assertEquals("id", field.getKeyLookup());
    assertEquals("=", field.getKeyCondition());
    assertEquals("streamId", field.getKeyStream());
    assertEquals("streamId2", field.getKeyStream2());
  }

  @Test
  void testCopyConstructor() {
    DeleteKeyField original = new DeleteKeyField("name", "LIKE", "streamName", null);
    DeleteKeyField copy = new DeleteKeyField(original);
    assertEquals(original, copy);
    assertEquals(original.hashCode(), copy.hashCode());
  }

  @Test
  void testSetters() {
    DeleteKeyField field = new DeleteKeyField();
    field.setKeyLookup("col");
    field.setKeyCondition(">=");
    field.setKeyStream("s1");
    field.setKeyStream2("s2");
    assertEquals("col", field.getKeyLookup());
    assertEquals(">=", field.getKeyCondition());
    assertEquals("s1", field.getKeyStream());
    assertEquals("s2", field.getKeyStream2());
  }

  @Test
  void testEqualsAndHashCodeWithNullStream2() {
    DeleteKeyField a = new DeleteKeyField("id", "=", "streamId", null);
    DeleteKeyField b = new DeleteKeyField("id", "=", "streamId", null);
    DeleteKeyField c = new DeleteKeyField("id", "<>", "streamId", null);

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
  }

  @Test
  void testEqualsAndHashCodeWithBetweenCondition() {
    DeleteKeyField a = new DeleteKeyField("age", "BETWEEN", "minAge", "maxAge");
    DeleteKeyField b = new DeleteKeyField("age", "BETWEEN", "minAge", "maxAge");
    DeleteKeyField c = new DeleteKeyField("age", "BETWEEN", "minAge", "otherMax");

    assertEquals(a, b);
    assertNotEquals(a, c);
  }

  @Test
  void testEqualsSameInstance() {
    DeleteKeyField field = new DeleteKeyField("id", "=", "streamId", null);
    assertEquals(field, field);
  }

  @Test
  void testNotEqualsDifferentClass() {
    DeleteKeyField field = new DeleteKeyField("id", "=", "streamId", null);
    assertNotEquals("not a DeleteKeyField", field);
  }
}
