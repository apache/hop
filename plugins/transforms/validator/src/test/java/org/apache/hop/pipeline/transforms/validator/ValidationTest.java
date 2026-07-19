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

package org.apache.hop.pipeline.transforms.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link Validation}. */
class ValidationTest {

  @Test
  void defaultConstructorSetsSafeDefaults() {
    Validation validation = new Validation();

    assertTrue(validation.isNullAllowed());
    assertFalse(validation.isOnlyNullAllowed());
    assertFalse(validation.isOnlyNumericAllowed());
    assertEquals("", validation.getMaximumLength());
    assertEquals("", validation.getMinimumLength());
    assertNotNull(validation.getAllowedValues());
    assertTrue(validation.getAllowedValues().isEmpty());
  }

  @Test
  void nameConstructorSetsFieldName() {
    Validation validation = new Validation("email");
    assertEquals("email", validation.getFieldName());
    assertTrue(validation.isNullAllowed());
  }

  @Test
  void copyConstructorAndCloneAreDeepForAllowedValues() {
    Validation original = new Validation();
    original.setName("rule");
    original.setFieldName("field");
    original.setMaximumLength("10");
    original.setMinimumLength("1");
    original.setNullAllowed(false);
    original.setOnlyNullAllowed(true);
    original.setOnlyNumericAllowed(true);
    original.setDataType("String");
    original.setDataTypeVerified(true);
    original.setConversionMask("#");
    original.setDecimalSymbol(".");
    original.setGroupingSymbol(",");
    original.setSourcingValues(true);
    original.setSourcingTransformName("source");
    original.setSourcingField("allowed");
    original.setMinimumValue("1");
    original.setMaximumValue("9");
    original.setStartString("A");
    original.setEndString("Z");
    original.setStartStringNotAllowed("X");
    original.setEndStringNotAllowed("Y");
    original.setRegularExpression(".*");
    original.setRegularExpressionNotAllowed("bad");
    original.setErrorCode("E1");
    original.setErrorDescription("desc");
    original.setAllowedValues(new ArrayList<>(List.of("A", "B")));
    TransformMeta sourcing = new TransformMeta();
    sourcing.setName("source");
    original.setSourcingTransform(sourcing);

    Validation copy = new Validation(original);
    Validation clone = original.clone();

    assertNotSame(original, copy);
    assertNotSame(original.getAllowedValues(), copy.getAllowedValues());
    assertEquals(original.getName(), copy.getName());
    assertEquals(original.getFieldName(), copy.getFieldName());
    assertEquals(original.getMaximumLength(), copy.getMaximumLength());
    assertEquals(original.getMinimumLength(), copy.getMinimumLength());
    assertEquals(original.isNullAllowed(), copy.isNullAllowed());
    assertEquals(original.isOnlyNullAllowed(), copy.isOnlyNullAllowed());
    assertEquals(original.isOnlyNumericAllowed(), copy.isOnlyNumericAllowed());
    assertEquals(original.getDataType(), copy.getDataType());
    assertEquals(original.isDataTypeVerified(), copy.isDataTypeVerified());
    assertEquals(original.getConversionMask(), copy.getConversionMask());
    assertEquals(original.getDecimalSymbol(), copy.getDecimalSymbol());
    assertEquals(original.getGroupingSymbol(), copy.getGroupingSymbol());
    assertEquals(original.isSourcingValues(), copy.isSourcingValues());
    assertEquals(original.getSourcingTransformName(), copy.getSourcingTransformName());
    assertEquals(original.getSourcingField(), copy.getSourcingField());
    assertEquals(original.getMinimumValue(), copy.getMinimumValue());
    assertEquals(original.getMaximumValue(), copy.getMaximumValue());
    assertEquals(original.getStartString(), copy.getStartString());
    assertEquals(original.getEndString(), copy.getEndString());
    assertEquals(original.getStartStringNotAllowed(), copy.getStartStringNotAllowed());
    assertEquals(original.getEndStringNotAllowed(), copy.getEndStringNotAllowed());
    assertEquals(original.getRegularExpression(), copy.getRegularExpression());
    assertEquals(original.getRegularExpressionNotAllowed(), copy.getRegularExpressionNotAllowed());
    assertEquals(original.getErrorCode(), copy.getErrorCode());
    assertEquals(original.getErrorDescription(), copy.getErrorDescription());
    assertEquals(original.getAllowedValues(), copy.getAllowedValues());
    assertSameSourcingTransform(original, copy);

    assertEquals(original.getName(), clone.getName());
    assertNotSame(original.getAllowedValues(), clone.getAllowedValues());

    copy.getAllowedValues().add("C");
    assertEquals(2, original.getAllowedValues().size());
    assertEquals(3, copy.getAllowedValues().size());
  }

  @Test
  void equalsIgnoresCaseOnName() {
    Validation left = new Validation();
    left.setName("RuleOne");
    Validation right = new Validation();
    right.setName("ruleone");

    assertTrue(left.equals(right));
    assertTrue(right.equals(left));
  }

  @Test
  void findValidationMatchesCaseInsensitively() {
    Validation first = new Validation();
    first.setName("Alpha");
    Validation second = new Validation();
    second.setName("Beta");
    List<Validation> list = List.of(first, second);

    assertEquals(first, Validation.findValidation(list, "alpha"));
    assertEquals(second, Validation.findValidation(list, "BETA"));
    assertNull(Validation.findValidation(list, "gamma"));
  }

  private static void assertSameSourcingTransform(Validation original, Validation copy) {
    assertSame(original.getSourcingTransform(), copy.getSourcingTransform());
  }
}
