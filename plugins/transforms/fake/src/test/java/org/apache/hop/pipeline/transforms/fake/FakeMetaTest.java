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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class FakeMetaTest {

  @BeforeAll
  static void initLogStore() {
    // getFields() logs (and swallows) errors for unresolvable generators; the central log store
    // must exist for that logging path to work in a bare unit test.
    HopLogStore.init();
  }

  @Test
  void setDefaultUsesEnglishLocale() {
    FakeMeta meta = new FakeMeta();
    meta.setDefault();
    assertEquals("en", meta.getLocale());
  }

  @Test
  void newMetaHasAnEmptyMutableFieldList() {
    FakeMeta meta = new FakeMeta();
    assertTrue(meta.getFields().isEmpty());
    meta.getFields().add(new FakeField("f", "name", "firstName"));
    assertEquals(1, meta.getFields().size());
  }

  @Test
  void cloneDeepCopiesLocaleAndFields() {
    FakeMeta meta = new FakeMeta();
    meta.setLocale("fr");
    List<FakeArgument> args = new ArrayList<>();
    args.add(new FakeArgument("int", "18"));
    meta.getFields().add(new FakeField("age", "number", "numberBetween", args));

    FakeMeta clone = meta.clone();
    assertEquals("fr", clone.getLocale());
    assertEquals(1, clone.getFields().size());

    // Field list, each field and each argument must be independent instances.
    assertNotSame(meta.getFields(), clone.getFields());
    assertNotSame(meta.getFields().get(0), clone.getFields().get(0));
    assertNotSame(
        meta.getFields().get(0).getArguments().get(0),
        clone.getFields().get(0).getArguments().get(0));

    // Mutating the clone must leave the original untouched.
    clone.getFields().get(0).getArguments().get(0).setValue("99");
    clone.getFields().get(0).setName("changed");
    assertEquals("18", meta.getFields().get(0).getArguments().get(0).getValue());
    assertEquals("age", meta.getFields().get(0).getName());
  }

  @Test
  void getFieldsAppendsResolvableFieldsWithTheRightValueMetaType() throws HopException {
    FakeMeta meta = new FakeMeta();
    meta.getFields().add(new FakeField("full_name", "name", "firstName"));
    meta.getFields()
        .add(
            new FakeField(
                "age",
                "number",
                "numberBetween",
                List.of(new FakeArgument("int", "1"), new FakeArgument("int", "9"))));

    IRowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "fake", null, null, new Variables(), new MemoryMetadataProvider());

    assertEquals(2, rowMeta.size());
    assertEquals("full_name", rowMeta.getValueMeta(0).getName());
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.getValueMeta(0).getType());
    assertEquals("fake", rowMeta.getValueMeta(0).getOrigin());
    assertEquals("age", rowMeta.getValueMeta(1).getName());
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(1).getType());
  }

  @Test
  void getFieldsSkipsInvalidAndUnresolvableFieldsWithoutFailing() throws HopException {
    FakeMeta meta = new FakeMeta();
    meta.getFields().add(new FakeField("good", "name", "firstName"));
    // Missing topic -> not valid, silently skipped.
    meta.getFields().add(new FakeField("incomplete", "name", null));
    // Valid shape but no such generator -> caught, logged and skipped, never propagated.
    meta.getFields().add(new FakeField("bogus", "nope", "nope"));

    IRowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "fake", null, null, new Variables(), new MemoryMetadataProvider());

    assertEquals(1, rowMeta.size());
    assertEquals("good", rowMeta.getValueMeta(0).getName());
  }

  @Test
  void fakerLocalesAreNonEmptyAndWellFormed() {
    String[] locales = FakeMeta.getFakerLocales();
    assertTrue(locales.length > 0);
    List<String> list = Arrays.asList(locales);
    assertTrue(list.contains("en"));
    assertTrue(list.contains("en-US"));
    for (String locale : locales) {
      assertFalse(locale == null || locale.isBlank(), "Locale list must not contain blanks");
    }
  }

  @Test
  void serializationRoundTripsLocaleFieldsAndArguments() throws Exception {
    FakeMeta meta =
        TransformSerializationTestUtil.testSerialization("/fake-transform.xml", FakeMeta.class);

    assertEquals("en-US", meta.getLocale());
    assertEquals(2, meta.getFields().size());

    FakeField first = meta.getFields().get(0);
    assertEquals("full_name", first.getName());
    assertEquals("name", first.getType());
    assertEquals("fullName", first.getTopic());
    assertTrue(first.getArguments().isEmpty());

    FakeField second = meta.getFields().get(1);
    assertEquals("age", second.getName());
    assertEquals("numberBetween", second.getTopic());
    assertEquals(2, second.getArguments().size());
    assertEquals("int", second.getArguments().get(0).getType());
    assertEquals("18", second.getArguments().get(0).getValue());
    assertEquals("65", second.getArguments().get(1).getValue());
  }
}
