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
package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.pipeline.transforms.userdefinedjavaclass.UserDefinedJavaClassDef.ClassType;
import org.junit.jupiter.api.Test;

class UserDefinedJavaClassDefTest {

  private static UserDefinedJavaClassDef normal(String name, String src) {
    return new UserDefinedJavaClassDef(ClassType.NORMAL_CLASS, name, src);
  }

  private static UserDefinedJavaClassDef transform(String name, String src) {
    return new UserDefinedJavaClassDef(ClassType.TRANSFORM_CLASS, name, src);
  }

  // ------------------------------------------------------------------ constructors

  @Test
  void defaultConstructor_fieldsAreNull() {
    UserDefinedJavaClassDef def = new UserDefinedJavaClassDef();
    assertNull(def.getClassType());
    assertNull(def.getClassName());
    assertNull(def.getSource());
  }

  @Test
  void fullConstructor_setsAllFields() {
    UserDefinedJavaClassDef def = normal("Foo", "int x = 1;");
    assertEquals(ClassType.NORMAL_CLASS, def.getClassType());
    assertEquals("Foo", def.getClassName());
    assertEquals("int x = 1;", def.getSource());
  }

  @Test
  void copyConstructor_copiesAllFields() {
    UserDefinedJavaClassDef original = transform("Bar", "void run(){}");
    UserDefinedJavaClassDef copy = new UserDefinedJavaClassDef(original);
    assertNotSame(original, copy);
    assertEquals(original.getClassType(), copy.getClassType());
    assertEquals(original.getClassName(), copy.getClassName());
    assertEquals(original.getSource(), copy.getSource());
  }

  // ------------------------------------------------------------------ clone

  @Test
  void clone_returnsDistinctEqualInstance() throws CloneNotSupportedException {
    UserDefinedJavaClassDef def = normal("Cloneable", "body");
    UserDefinedJavaClassDef cloned = (UserDefinedJavaClassDef) def.clone();
    assertNotSame(def, cloned);
    assertEquals(def.getClassType(), cloned.getClassType());
    assertEquals(def.getClassName(), cloned.getClassName());
    assertEquals(def.getSource(), cloned.getSource());
  }

  // ------------------------------------------------------------------ isTransformClass

  @Test
  void isTransformClass_normalClass_returnsFalse() {
    assertFalse(normal("X", "").isTransformClass());
  }

  @Test
  void isTransformClass_transformClass_returnsTrue() {
    assertTrue(transform("X", "").isTransformClass());
  }

  // ------------------------------------------------------------------ getChecksum

  @Test
  void getChecksum_stableForSameContent() throws Exception {
    UserDefinedJavaClassDef a = normal("Foo", "int x = 1;");
    UserDefinedJavaClassDef b = normal("Foo", "int x = 1;");
    assertEquals(a.getChecksum(), b.getChecksum());
  }

  @Test
  void getChecksum_differsForDifferentSource() throws Exception {
    UserDefinedJavaClassDef a = normal("Foo", "int x = 1;");
    UserDefinedJavaClassDef b = normal("Foo", "int y = 2;");
    assertNotEquals(a.getChecksum(), b.getChecksum());
  }

  @Test
  void getChecksum_differsForDifferentClassName() throws Exception {
    UserDefinedJavaClassDef a = normal("Alpha", "int x = 1;");
    UserDefinedJavaClassDef b = normal("Beta", "int x = 1;");
    assertNotEquals(a.getChecksum(), b.getChecksum());
  }

  @Test
  void getChecksum_isHexString() throws Exception {
    String checksum = normal("Foo", "src").getChecksum();
    assertTrue(checksum.matches("[0-9a-f]+"), "Expected lowercase hex but got: " + checksum);
  }

  // ------------------------------------------------------------------ getTransformedSource

  @Test
  void getTransformedSource_containsOriginalSource() {
    UserDefinedJavaClassDef def = transform("MyClass", "public void run(){}");
    assertTrue(def.getTransformedSource().contains("public void run(){}"));
  }

  @Test
  void getTransformedSource_containsClassName() {
    UserDefinedJavaClassDef def = transform("Processor", "");
    String transformed = def.getTransformedSource();
    assertTrue(transformed.contains("Processor"));
  }

  @Test
  void getTransformedSource_containsSuperConstructorCall() {
    UserDefinedJavaClassDef def = transform("MyTransform", "");
    assertTrue(def.getTransformedSource().contains("super(parent,meta,data)"));
  }

  // ------------------------------------------------------------------ setters (Lombok)

  @Test
  void setters_updateFields() {
    UserDefinedJavaClassDef def = new UserDefinedJavaClassDef();
    def.setClassType(ClassType.TRANSFORM_CLASS);
    def.setClassName("Updated");
    def.setSource("new source");
    assertEquals(ClassType.TRANSFORM_CLASS, def.getClassType());
    assertEquals("Updated", def.getClassName());
    assertEquals("new source", def.getSource());
  }
}
