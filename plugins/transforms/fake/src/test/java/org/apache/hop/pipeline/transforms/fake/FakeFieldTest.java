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

package org.apache.hop.pipeline.transforms.fake;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit coverage for the {@link FakeField} and {@link FakeArgument} value objects. */
class FakeFieldTest {

  @Test
  void newFieldStartsWithAnEmptyMutableArgumentList() {
    FakeField field = new FakeField();
    assertNotNull(field.getArguments());
    assertTrue(field.getArguments().isEmpty());
    // getArguments() must hand back a live list that can be appended to.
    field.getArguments().add(new FakeArgument("int", "1"));
    assertEquals(1, field.getArguments().size());
  }

  @Test
  void convenienceConstructorLeavesNoArguments() {
    FakeField field = new FakeField("age", "number", "randomDigit");
    assertEquals("age", field.getName());
    assertEquals("number", field.getType());
    assertEquals("randomDigit", field.getTopic());
    assertTrue(field.getArguments().isEmpty());
  }

  @Test
  void nullArgumentListIsNormalisedToEmpty() {
    FakeField field = new FakeField("age", "number", "randomDigit", null);
    assertNotNull(field.getArguments());
    assertTrue(field.getArguments().isEmpty());

    field.setArguments(null);
    assertNotNull(field.getArguments(), "getArguments() must re-initialise a null list");
    assertTrue(field.getArguments().isEmpty());
  }

  @Test
  void isValidRequiresName_typeAndTopic() {
    assertTrue(new FakeField("n", "t", "p").isValid());
    assertFalse(new FakeField(null, "t", "p").isValid());
    assertFalse(new FakeField("n", null, "p").isValid());
    assertFalse(new FakeField("n", "t", null).isValid());
    assertFalse(new FakeField("", "t", "p").isValid());
    assertFalse(new FakeField("n", "", "p").isValid());
    assertFalse(new FakeField("n", "t", "").isValid());
  }

  @Test
  void copyConstructorDeepCopiesArguments() {
    List<FakeArgument> args = new ArrayList<>();
    args.add(new FakeArgument("int", "18"));
    FakeField original = new FakeField("age", "number", "numberBetween", args);

    FakeField copy = new FakeField(original);
    assertEquals("age", copy.getName());
    assertEquals("number", copy.getType());
    assertEquals("numberBetween", copy.getTopic());
    assertEquals(1, copy.getArguments().size());

    // The argument list and each argument must be independent instances.
    assertNotSame(original.getArguments(), copy.getArguments());
    assertNotSame(original.getArguments().get(0), copy.getArguments().get(0));

    // Mutating the copy must not leak into the original.
    copy.getArguments().get(0).setValue("99");
    copy.setName("changed");
    assertEquals("18", original.getArguments().get(0).getValue());
    assertEquals("age", original.getName());
  }

  @Test
  void fieldSettersRoundTrip() {
    FakeField field = new FakeField();
    field.setName("n");
    field.setType("t");
    field.setTopic("p");
    field.setArguments(List.of(new FakeArgument("string", "x")));
    assertEquals("n", field.getName());
    assertEquals("t", field.getType());
    assertEquals("p", field.getTopic());
    assertEquals(1, field.getArguments().size());
  }

  @Test
  void argumentConstructorsAndSettersRoundTrip() {
    FakeArgument argument = new FakeArgument("int", "5");
    assertEquals("int", argument.getType());
    assertEquals("5", argument.getValue());

    argument.setType("long");
    argument.setValue("7");
    assertEquals("long", argument.getType());
    assertEquals("7", argument.getValue());
  }

  @Test
  void argumentCopyConstructorCopiesTypeAndValue() {
    FakeArgument original = new FakeArgument("string", "hello");
    FakeArgument copy = new FakeArgument(original);
    assertEquals("string", copy.getType());
    assertEquals("hello", copy.getValue());

    copy.setValue("changed");
    assertEquals("hello", original.getValue(), "Copy must not share state with the original");
  }

  @Test
  void argumentNoArgConstructorLeavesNulls() {
    FakeArgument argument = new FakeArgument();
    assertNull(argument.getType());
    assertNull(argument.getValue());
  }
}
