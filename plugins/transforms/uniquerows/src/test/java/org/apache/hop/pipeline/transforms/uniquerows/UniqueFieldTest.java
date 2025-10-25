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

package org.apache.hop.pipeline.transforms.uniquerows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class UniqueFieldTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void testDefaultConstructor() {
    UniqueField field = new UniqueField();

    assertNotNull(field, "UniqueField instance should be created successfully.");
    assertNull(field.getName(), "name should be null by default");
    assertFalse(field.isCaseInsensitive(), "caseInsensitive should be false by default");
  }

  @Test
  void testParameterizedConstructor() {
    String fieldName = "testField";
    boolean caseInsensitive = true;

    UniqueField field = new UniqueField(fieldName, caseInsensitive);

    assertNotNull(field, "UniqueField instance should be created successfully.");
    assertEquals(fieldName, field.getName(), "name should match constructor parameter");
    assertEquals(
        caseInsensitive,
        field.isCaseInsensitive(),
        "caseInsensitive should match constructor parameter");
  }

  @Test
  void testParameterizedConstructorWithFalseCaseInsensitive() {
    String fieldName = "testField";
    boolean caseInsensitive = false;

    UniqueField field = new UniqueField(fieldName, caseInsensitive);

    assertNotNull(field, "UniqueField instance should be created successfully.");
    assertEquals(fieldName, field.getName(), "name should match constructor parameter");
    assertEquals(
        caseInsensitive,
        field.isCaseInsensitive(),
        "caseInsensitive should match constructor parameter");
  }

  @Test
  void testParameterizedConstructorWithNullName() {
    String fieldName = null;
    boolean caseInsensitive = true;

    UniqueField field = new UniqueField(fieldName, caseInsensitive);

    assertNotNull(field, "UniqueField instance should be created successfully.");
    assertNull(field.getName(), "name should be null");
    assertEquals(
        caseInsensitive,
        field.isCaseInsensitive(),
        "caseInsensitive should match constructor parameter");
  }

  @Test
  void testParameterizedConstructorWithEmptyName() {
    String fieldName = "";
    boolean caseInsensitive = false;

    UniqueField field = new UniqueField(fieldName, caseInsensitive);

    assertNotNull(field, "UniqueField instance should be created successfully.");
    assertEquals(fieldName, field.getName(), "name should match constructor parameter");
    assertEquals(
        caseInsensitive,
        field.isCaseInsensitive(),
        "caseInsensitive should match constructor parameter");
  }

  @Test
  void testGetName() {
    UniqueField field = new UniqueField();
    String testName = "testField";

    field.setName(testName);
    assertEquals(testName, field.getName(), "getName should return the set name");
  }

  @Test
  void testSetName() {
    UniqueField field = new UniqueField();
    String testName = "testField";

    field.setName(testName);
    assertEquals(testName, field.getName(), "setName should set the name correctly");
  }

  @Test
  void testSetNameWithNull() {
    UniqueField field = new UniqueField();
    field.setName("initialName");
    field.setName(null);

    assertNull(field.getName(), "setName with null should set name to null");
  }

  @Test
  void testSetNameWithEmptyString() {
    UniqueField field = new UniqueField();
    String emptyName = "";

    field.setName(emptyName);
    assertEquals(emptyName, field.getName(), "setName with empty string should work");
  }

  @Test
  void testIsCaseInsensitive() {
    UniqueField field = new UniqueField();
    boolean caseInsensitive = true;

    field.setCaseInsensitive(caseInsensitive);
    assertEquals(
        caseInsensitive,
        field.isCaseInsensitive(),
        "isCaseInsensitive should return the set value");
  }

  @Test
  void testSetCaseInsensitive() {
    UniqueField field = new UniqueField();
    boolean caseInsensitive = true;

    field.setCaseInsensitive(caseInsensitive);
    assertEquals(
        caseInsensitive,
        field.isCaseInsensitive(),
        "setCaseInsensitive should set the value correctly");
  }

  @Test
  void testSetCaseInsensitiveFalse() {
    UniqueField field = new UniqueField();
    boolean caseInsensitive = false;

    field.setCaseInsensitive(caseInsensitive);
    assertEquals(
        caseInsensitive,
        field.isCaseInsensitive(),
        "setCaseInsensitive should set the value correctly");
  }

  @Test
  void testEqualsWithSameObject() {
    UniqueField field = new UniqueField("testField", true);

    assertTrue(field.equals(field), "field should equal itself");
  }

  @Test
  void testEqualsWithNull() {
    UniqueField field = new UniqueField("testField", true);

    assertFalse(field.equals(null), "field should not equal null");
  }

  @Test
  void testEqualsWithDifferentClass() {
    UniqueField field = new UniqueField("testField", true);
    String otherObject = "not a UniqueField";

    assertFalse(field.equals(otherObject), "field should not equal object of different class");
  }

  @Test
  void testEqualsWithSameValues() {
    UniqueField field1 = new UniqueField("testField", true);
    UniqueField field2 = new UniqueField("testField", true);

    assertTrue(field1.equals(field2), "fields with same values should be equal");
    assertTrue(field2.equals(field1), "equals should be symmetric");
  }

  @Test
  void testEqualsWithDifferentName() {
    UniqueField field1 = new UniqueField("testField1", true);
    UniqueField field2 = new UniqueField("testField2", true);

    assertFalse(field1.equals(field2), "fields with different names should not be equal");
    assertFalse(field2.equals(field1), "equals should be symmetric");
  }

  @Test
  void testEqualsWithDifferentCaseInsensitive() {
    UniqueField field1 = new UniqueField("testField", true);
    UniqueField field2 = new UniqueField("testField", false);

    assertFalse(field1.equals(field2), "fields with different caseInsensitive should not be equal");
    assertFalse(field2.equals(field1), "equals should be symmetric");
  }

  @Test
  void testEqualsWithNullName() {
    UniqueField field1 = new UniqueField(null, true);
    UniqueField field2 = new UniqueField(null, true);

    assertTrue(
        field1.equals(field2), "fields with null names should be equal if other values match");
  }

  @Test
  void testEqualsWithOneNullName() {
    UniqueField field1 = new UniqueField(null, true);
    UniqueField field2 = new UniqueField("testField", true);

    assertFalse(field1.equals(field2), "fields with one null name should not be equal");
    assertFalse(field2.equals(field1), "equals should be symmetric");
  }

  @Test
  void testHashCodeWithSameValues() {
    UniqueField field1 = new UniqueField("testField", true);
    UniqueField field2 = new UniqueField("testField", true);

    assertEquals(
        field1.hashCode(), field2.hashCode(), "fields with same values should have same hashCode");
  }

  @Test
  void testHashCodeWithDifferentName() {
    UniqueField field1 = new UniqueField("testField1", true);
    UniqueField field2 = new UniqueField("testField2", true);

    assertNotEquals(
        field1.hashCode(),
        field2.hashCode(),
        "fields with different names should have different hashCode");
  }

  @Test
  void testHashCodeWithDifferentCaseInsensitive() {
    UniqueField field1 = new UniqueField("testField", true);
    UniqueField field2 = new UniqueField("testField", false);

    assertNotEquals(
        field1.hashCode(),
        field2.hashCode(),
        "fields with different caseInsensitive should have different hashCode");
  }

  @Test
  void testHashCodeWithNullName() {
    UniqueField field1 = new UniqueField(null, true);
    UniqueField field2 = new UniqueField(null, true);

    assertEquals(
        field1.hashCode(),
        field2.hashCode(),
        "fields with null names should have same hashCode if other values match");
  }

  @Test
  void testHashCodeConsistency() {
    UniqueField field = new UniqueField("testField", true);
    int hashCode1 = field.hashCode();
    int hashCode2 = field.hashCode();

    assertEquals(hashCode1, hashCode2, "hashCode should be consistent across multiple calls");
  }

  @Test
  void testHashCodeAfterModification() {
    UniqueField field = new UniqueField("testField", true);
    int originalHashCode = field.hashCode();

    field.setName("newField");
    int newHashCode = field.hashCode();

    assertNotEquals(
        originalHashCode, newHashCode, "hashCode should change after name modification");
  }

  @Test
  void testHashCodeAfterCaseInsensitiveModification() {
    UniqueField field = new UniqueField("testField", true);
    int originalHashCode = field.hashCode();

    field.setCaseInsensitive(false);
    int newHashCode = field.hashCode();

    assertNotEquals(
        originalHashCode, newHashCode, "hashCode should change after caseInsensitive modification");
  }

  @Test
  void testToString() {
    UniqueField field = new UniqueField("testField", true);
    String toString = field.toString();

    assertNotNull(toString, "toString should not return null");
    // The default toString() method from Object doesn't contain the field name
    // This test just verifies that toString() doesn't return null
  }

  @Test
  void testToStringWithNullName() {
    UniqueField field = new UniqueField(null, true);
    String toString = field.toString();

    assertNotNull(toString, "toString should not return null even with null name");
  }

  @Test
  void testFieldModification() {
    UniqueField field = new UniqueField("initialName", false);

    // Test name modification
    field.setName("newName");
    assertEquals("newName", field.getName(), "name should be updated");
    assertFalse(field.isCaseInsensitive(), "caseInsensitive should remain unchanged");

    // Test caseInsensitive modification
    field.setCaseInsensitive(true);
    assertEquals("newName", field.getName(), "name should remain unchanged");
    assertTrue(field.isCaseInsensitive(), "caseInsensitive should be updated");
  }

  @Test
  void testFieldEqualityAfterModification() {
    UniqueField field1 = new UniqueField("testField", true);
    UniqueField field2 = new UniqueField("testField", true);

    // Initially equal
    assertTrue(field1.equals(field2), "fields should be equal initially");

    // Modify one field
    field1.setName("newName");

    // Should no longer be equal
    assertFalse(field1.equals(field2), "fields should not be equal after modification");
  }
}
