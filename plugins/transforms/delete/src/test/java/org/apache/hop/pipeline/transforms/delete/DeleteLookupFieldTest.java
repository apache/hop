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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/** Unit test for {@link DeleteKeyField} */
class DeleteLookupFieldTest {

  @Test
  void testDefaultConstructorInitializesFieldsList() {
    DeleteLookupField lookup = new DeleteLookupField();
    assertNotNull(lookup.getFields());
    assertTrue(lookup.getFields().isEmpty());
  }

  @Test
  void testParameterizedConstructor() {
    DeleteKeyField key = new DeleteKeyField("id", "=", "streamId", null);
    DeleteLookupField lookup =
        new DeleteLookupField("public", "customers", Collections.singletonList(key));

    assertEquals("public", lookup.getSchemaName());
    assertEquals("customers", lookup.getTableName());
    assertEquals(1, lookup.getFields().size());
    assertEquals(key, lookup.getFields().getFirst());
  }

  @Test
  void testCopyConstructorDeepCopiesFields() {
    DeleteKeyField key = new DeleteKeyField("id", "=", "streamId", null);
    DeleteLookupField original =
        new DeleteLookupField("dbo", "orders", Collections.singletonList(key));

    DeleteLookupField copy = new DeleteLookupField(original);
    assertEquals(original, copy);
    assertEquals(original.hashCode(), copy.hashCode());

    copy.getFields().getFirst().setKeyLookup("changed");
    assertNotEquals(
        original.getFields().getFirst().getKeyLookup(), copy.getFields().getFirst().getKeyLookup());
  }

  @Test
  void testSetters() {
    DeleteLookupField lookup = new DeleteLookupField();
    lookup.setSchemaName("schema1");
    lookup.setTableName("table1");
    lookup.setFields(
        Arrays.asList(
            new DeleteKeyField("a", "=", "b", null), new DeleteKeyField("c", "<>", "d", null)));

    assertEquals("schema1", lookup.getSchemaName());
    assertEquals("table1", lookup.getTableName());
    assertEquals(2, lookup.getFields().size());
  }

  @Test
  void testEqualsAndHashCode() {
    DeleteKeyField key = new DeleteKeyField("id", "=", "streamId", null);
    DeleteLookupField a =
        new DeleteLookupField("public", "customers", Collections.singletonList(key));
    DeleteLookupField b =
        new DeleteLookupField(
            "public", "customers", Collections.singletonList(new DeleteKeyField(key)));
    DeleteLookupField c =
        new DeleteLookupField(
            "public", "orders", Collections.singletonList(new DeleteKeyField(key)));

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
  }

  @Test
  void testEqualsSameInstance() {
    DeleteLookupField lookup = new DeleteLookupField();
    assertEquals(lookup, lookup);
  }
}
