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

package org.apache.hop.pipeline.transforms.mongodbdelete;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MongoDbDeleteFieldTest {

  private MongoDbDeleteField field;
  private IVariables variables;

  @BeforeEach
  void setUp() {
    field = new MongoDbDeleteField();
    variables = new Variables();
  }

  @Test
  void testDefaultValues() {
    assertEquals("", field.getIncomingField1());
    assertEquals("", field.getIncomingField2());
    assertEquals("", field.getMongoDocPath());
    assertEquals("", field.getComparator());
  }

  @Test
  void testSetAndGetIncomingField1() {
    field.setIncomingField1("field1");
    assertEquals("field1", field.getIncomingField1());
  }

  @Test
  void testSetAndGetIncomingField2() {
    field.setIncomingField2("field2");
    assertEquals("field2", field.getIncomingField2());
  }

  @Test
  void testSetAndGetMongoDocPath() {
    field.setMongoDocPath("path.to.field");
    assertEquals("path.to.field", field.getMongoDocPath());
  }

  @Test
  void testSetAndGetComparator() {
    field.setComparator("=");
    assertEquals("=", field.getComparator());
  }

  @Test
  void testCopy() {
    field.setIncomingField1("field1");
    field.setIncomingField2("field2");
    field.setMongoDocPath("path.to.doc");
    field.setComparator("<>");

    MongoDbDeleteField copy = field.copy();

    assertNotNull(copy);
    assertNotSame(field, copy);
    assertEquals(field.getIncomingField1(), copy.getIncomingField1());
    assertEquals(field.getIncomingField2(), copy.getIncomingField2());
    assertEquals(field.getMongoDocPath(), copy.getMongoDocPath());
    assertEquals(field.getComparator(), copy.getComparator());
  }

  @Test
  void testCopyIsIndependent() {
    field.setIncomingField1("original");
    MongoDbDeleteField copy = field.copy();

    // Modify original
    field.setIncomingField1("modified");

    // Copy should remain unchanged
    assertEquals("original", copy.getIncomingField1());
  }

  @Test
  void testInit() {
    field.setMongoDocPath("test.path");
    field.init(variables);

    // After init, pathList should contain the resolved path
    // We can verify by resetting and checking the internal state is properly set
    field.reset();
    // No exception means success
  }

  @Test
  void testInitWithVariable() {
    variables.setVariable("MY_PATH", "resolved.path");
    field.setMongoDocPath("${MY_PATH}");
    field.init(variables);

    // The path should be resolved
    field.reset();
  }

  @Test
  void testReset() {
    field.setMongoDocPath("test.path");
    field.init(variables);

    // First reset
    field.reset();

    // Second reset should also work
    field.reset();
  }

  @Test
  void testResetWithoutInit() {
    // Reset without init should not throw
    field.reset();
  }

  @Test
  void testDirectFieldAccess() {
    // Test direct field access (public fields)
    field.incomingField1 = "direct1";
    field.incomingField2 = "direct2";
    field.mongoDocPath = "direct.path";
    field.comparator = ">";

    assertEquals("direct1", field.incomingField1);
    assertEquals("direct2", field.incomingField2);
    assertEquals("direct.path", field.mongoDocPath);
    assertEquals(">", field.comparator);
  }

  @Test
  void testCopyWithEmptyValues() {
    MongoDbDeleteField copy = field.copy();

    assertNotNull(copy);
    assertEquals("", copy.getIncomingField1());
    assertEquals("", copy.getIncomingField2());
    assertEquals("", copy.getMongoDocPath());
    assertEquals("", copy.getComparator());
  }

  @Test
  void testInitWithEmptyPath() {
    field.setMongoDocPath("");
    field.init(variables);
    field.reset();
  }

  @Test
  void testAllComparators() {
    // Test that all comparator values can be set
    String[] comparators = {"=", "<>", "<", "<=", ">", ">=", "BETWEEN", "IS NULL", "IS NOT NULL"};

    for (String comp : comparators) {
      field.setComparator(comp);
      assertEquals(comp, field.getComparator());
    }
  }
}
