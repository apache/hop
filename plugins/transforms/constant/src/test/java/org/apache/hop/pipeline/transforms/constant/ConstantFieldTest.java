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

package org.apache.hop.pipeline.transforms.constant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * {@link ConstantField} is a plain metadata holder, but its {@code equals}/{@code hashCode} back
 * both the serialization round-trip test and the "has this transform changed?" checks in the GUI,
 * so they are worth pinning down.
 */
class ConstantFieldTest {

  @Test
  void testValueConstructorSetsNameTypeAndValue() {
    ConstantField field = new ConstantField("name", "String", "value");

    assertEquals("name", field.getFieldName());
    assertEquals("String", field.getFieldType());
    assertEquals("value", field.getValue());
    assertFalse(field.isEmptyString());
  }

  @Test
  void testEmptyStringConstructorClearsTheValue() {
    ConstantField field = new ConstantField("name", "String", true);

    assertEquals("name", field.getFieldName());
    assertEquals("String", field.getFieldType());
    assertTrue(field.isEmptyString());
    assertEquals("", field.getValue(), "the empty-string flag replaces any value");
  }

  @Test
  void testAccessorsRoundTrip() {
    ConstantField field = new ConstantField();

    field.setFieldName("name");
    field.setFieldType("Number");
    field.setFieldFormat("#.##");
    field.setFieldLength(9);
    field.setFieldPrecision(2);
    field.setValue("1.23");
    field.setCurrency("EUR");
    field.setDecimal(".");
    field.setGroup(",");
    field.setEmptyString(true);

    assertEquals("name", field.getFieldName());
    assertEquals("Number", field.getFieldType());
    assertEquals("#.##", field.getFieldFormat());
    assertEquals(9, field.getFieldLength());
    assertEquals(2, field.getFieldPrecision());
    assertEquals("1.23", field.getValue());
    assertEquals("EUR", field.getCurrency());
    assertEquals(".", field.getDecimal());
    assertEquals(",", field.getGroup());
    assertTrue(field.isEmptyString());
  }

  @Test
  void testEqualFieldsShareAHashCode() {
    ConstantField field = fullyPopulated();
    ConstantField same = fullyPopulated();

    assertEquals(field, same);
    assertEquals(field.hashCode(), same.hashCode());
    assertEquals(field, field, "a field equals itself");
  }

  @Test
  void testFieldsDifferingInAnyPropertyAreNotEqual() {
    assertNotEquals(fullyPopulated(), withName("other"));
    assertNotEquals(fullyPopulated(), withType("Integer"));
    assertNotEquals(fullyPopulated(), withValue("other"));

    ConstantField differentFormat = fullyPopulated();
    differentFormat.setFieldFormat("0.0");
    assertNotEquals(fullyPopulated(), differentFormat);

    ConstantField differentLength = fullyPopulated();
    differentLength.setFieldLength(1);
    assertNotEquals(fullyPopulated(), differentLength);

    ConstantField differentPrecision = fullyPopulated();
    differentPrecision.setFieldPrecision(1);
    assertNotEquals(fullyPopulated(), differentPrecision);

    ConstantField differentCurrency = fullyPopulated();
    differentCurrency.setCurrency("USD");
    assertNotEquals(fullyPopulated(), differentCurrency);

    ConstantField differentDecimal = fullyPopulated();
    differentDecimal.setDecimal(",");
    assertNotEquals(fullyPopulated(), differentDecimal);

    ConstantField differentGroup = fullyPopulated();
    differentGroup.setGroup(".");
    assertNotEquals(fullyPopulated(), differentGroup);

    ConstantField differentEmptyString = fullyPopulated();
    differentEmptyString.setEmptyString(true);
    assertNotEquals(fullyPopulated(), differentEmptyString);
  }

  @Test
  void testNotEqualToNullOrOtherTypes() {
    ConstantField field = fullyPopulated();

    assertNotEquals(null, field);
    assertNotEquals("not a ConstantField", field);
  }

  private static ConstantField fullyPopulated() {
    ConstantField field = new ConstantField("name", "Number", "1.23");
    field.setFieldFormat("#.##");
    field.setFieldLength(9);
    field.setFieldPrecision(2);
    field.setCurrency("EUR");
    field.setDecimal(".");
    field.setGroup(",");
    return field;
  }

  private static ConstantField withName(String name) {
    ConstantField field = fullyPopulated();
    field.setFieldName(name);
    return field;
  }

  private static ConstantField withType(String type) {
    ConstantField field = fullyPopulated();
    field.setFieldType(type);
    return field;
  }

  private static ConstantField withValue(String value) {
    ConstantField field = fullyPopulated();
    field.setValue(value);
    return field;
  }
}
