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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Coverage for the {@code FakeDialog} grid-cell encoding, the pure string logic behind the single
 * editable "Data generator" column ({@code accessor.topic(argType=argValue, ...)}). Exercised
 * without a display so it runs in the normal build.
 */
class FakeDialogEncodingTest {

  @Test
  void encodesAZeroArgumentGenerator() {
    assertEquals("name.firstName()", FakeDialog.encode(new FakeField("n", "name", "firstName")));
  }

  @Test
  void encodesArgumentsAsTypeEqualsValuePairs() {
    FakeField field =
        new FakeField(
            "age",
            "number",
            "numberBetween",
            List.of(new FakeArgument("int", "18"), new FakeArgument("int", "65")));
    assertEquals("number.numberBetween(int=18, int=65)", FakeDialog.encode(field));
  }

  @Test
  void encodesNullArgumentPartsAsEmptyStrings() {
    FakeField field = new FakeField("f", "a", "b", List.of(new FakeArgument(null, null)));
    assertEquals("a.b(=)", FakeDialog.encode(field));
  }

  @Test
  void encodingAnIncompleteFieldYieldsEmptyString() {
    assertEquals("", FakeDialog.encode(new FakeField(null, null, null)));
    assertEquals("", FakeDialog.encode(new FakeField("n", "name", null)));
    assertEquals("", FakeDialog.encode(new FakeField("n", null, "firstName")));
  }

  @Test
  void decodesTypeTopicAndArguments() {
    FakeField field = FakeDialog.decode("number.numberBetween(int=18, int=65)");
    assertEquals("number", field.getType());
    assertEquals("numberBetween", field.getTopic());
    assertEquals(2, field.getArguments().size());
    assertEquals("int", field.getArguments().get(0).getType());
    assertEquals("18", field.getArguments().get(0).getValue());
    assertEquals("int", field.getArguments().get(1).getType());
    assertEquals("65", field.getArguments().get(1).getValue());
  }

  @Test
  void decodesAZeroArgumentGenerator() {
    FakeField field = FakeDialog.decode("name.firstName()");
    assertEquals("name", field.getType());
    assertEquals("firstName", field.getTopic());
    assertTrue(field.getArguments().isEmpty());
  }

  @Test
  void decodingEmptyTextYieldsAnEmptyField() {
    FakeField field = FakeDialog.decode("");
    assertNull(field.getType());
    assertNull(field.getTopic());
    assertTrue(field.getArguments().isEmpty());
  }

  @Test
  void decodingHeadWithoutADotSetsOnlyTheType() {
    FakeField field = FakeDialog.decode("justAType");
    assertEquals("justAType", field.getType());
    assertNull(field.getTopic());
  }

  @Test
  void decodingIgnoresArgumentFragmentsWithoutASeparator() {
    FakeField field = FakeDialog.decode("a.b(garbage)");
    assertEquals("a", field.getType());
    assertEquals("b", field.getTopic());
    assertTrue(field.getArguments().isEmpty(), "A fragment with no '=' must be dropped");
  }

  @Test
  void encodeDecodeRoundTripsAParameterisedGenerator() {
    FakeField original =
        new FakeField(
            "age",
            "number",
            "numberBetween",
            List.of(new FakeArgument("int", "18"), new FakeArgument("long", "65")));
    FakeField roundTripped = FakeDialog.decode(FakeDialog.encode(original));

    assertEquals(original.getType(), roundTripped.getType());
    assertEquals(original.getTopic(), roundTripped.getTopic());
    assertEquals(original.getArguments().size(), roundTripped.getArguments().size());
    for (int i = 0; i < original.getArguments().size(); i++) {
      assertEquals(
          original.getArguments().get(i).getType(), roundTripped.getArguments().get(i).getType());
      assertEquals(
          original.getArguments().get(i).getValue(), roundTripped.getArguments().get(i).getValue());
    }
  }
}
