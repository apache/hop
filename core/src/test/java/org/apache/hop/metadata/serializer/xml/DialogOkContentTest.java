/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.metadata.serializer.xml;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.metadata.serializer.xml.classes.Field;
import org.apache.hop.metadata.serializer.xml.classes.Info;
import org.apache.hop.metadata.serializer.xml.classes.MetaData;
import org.apache.hop.metadata.serializer.xml.classes.TestEnum;
import org.junit.jupiter.api.Test;

/** Unit test for {@link DialogOkContent} */
class DialogOkContentTest {

  @Test
  void widgetEmptyLeavesNullOnlyWhenCurrentIsNull() {
    assertTrue(DialogOkContent.widgetEmptyLeavesNull(null, ""));
    assertFalse(DialogOkContent.widgetEmptyLeavesNull("", ""));
    assertFalse(DialogOkContent.widgetEmptyLeavesNull(null, "typed"));
    assertFalse(DialogOkContent.widgetEmptyLeavesNull("hello", ""));
    assertFalse(DialogOkContent.widgetEmptyLeavesNull(null, null));
  }

  @Test
  void nullAndEmptyStringAreTheSameContent() {
    assertTrue(DialogOkContent.same(null, ""));
    assertTrue(DialogOkContent.same("", null));
    assertTrue(DialogOkContent.same("", ""));
    assertTrue(DialogOkContent.same(null, null));
  }

  @Test
  void realStringChangeIsDetected() {
    assertFalse(DialogOkContent.same(null, "stop"));
    assertFalse(DialogOkContent.same("", "stop"));
    assertFalse(DialogOkContent.same("old", "new"));
    assertFalse(DialogOkContent.same("old", ""));
  }

  @Test
  void whitespaceIsNotTreatedAsEmpty() {
    assertFalse(DialogOkContent.same(null, " "));
    assertFalse(DialogOkContent.same("", " "));
  }

  @Test
  void fieldNullFormatEqualsEmptyFormat() {
    Field withNull = new Field("a", "String", 50, -1, null, TestEnum.ONE);
    Field withEmpty = new Field("a", "String", 50, -1, "", TestEnum.ONE);
    assertTrue(DialogOkContent.same(withNull, withEmpty));
  }

  @Test
  void fieldFormatTextChangeIsDetected() {
    Field before = new Field("a", "String", 50, -1, null, TestEnum.ONE);
    Field after = new Field("a", "String", 50, -1, "yyyy", TestEnum.ONE);
    assertFalse(DialogOkContent.same(before, after));
  }

  @Test
  void extraEmptyListItemIsAChange() {
    MetaData oneField = new MetaData();
    oneField.getFields().add(new Field("a", "String", 50, -1, null, TestEnum.ONE));

    MetaData trailingEmpty = new MetaData();
    trailingEmpty.getFields().add(new Field("a", "String", 50, -1, null, TestEnum.ONE));
    trailingEmpty.getFields().add(new Field());

    assertFalse(DialogOkContent.same(oneField, trailingEmpty));
  }

  @Test
  void emptyListAndNullListAreTheSame() {
    MetaData withList = new MetaData();
    MetaData withNullList = new MetaData();
    withNullList.setFields(null);
    assertTrue(DialogOkContent.same(withList, withNullList));
  }

  @Test
  void nestedNullStringEqualsEmptyString() {
    MetaData left = new MetaData();
    left.setInfo(new Info(null, "b"));
    MetaData right = new MetaData();
    right.setInfo(new Info("", "b"));
    assertTrue(DialogOkContent.same(left, right));
  }

  @Test
  void filenameChangeIsDetected() {
    MetaData left = new MetaData();
    left.setFilename(null);
    MetaData right = new MetaData();
    right.setFilename("file.csv");
    assertFalse(DialogOkContent.same(left, right));
  }

  @Test
  void enumChangeIsDetected() {
    Field left = new Field("a", "String", 50, -1, null, TestEnum.ONE);
    Field right = new Field("a", "String", 50, -1, null, TestEnum.TWO);
    assertFalse(DialogOkContent.same(left, right));
  }

  @Test
  void integerChangeIsDetected() {
    Field left = new Field("a", "String", 50, -1, null, TestEnum.ONE);
    Field right = new Field("a", "String", 80, -1, null, TestEnum.ONE);
    assertFalse(DialogOkContent.same(left, right));
  }

  @Test
  void stringListExtraEmptyItemIsAChange() {
    MetaData left = new MetaData();
    left.setValues(new ArrayList<>(List.of("v1")));
    MetaData right = new MetaData();
    right.setValues(new ArrayList<>(List.of("v1", "")));
    assertFalse(DialogOkContent.same(left, right));
  }

  @Test
  void nullAndEmptyStillSerializeDifferently() throws Exception {
    Field withNullFormat = new Field("a", "String", 50, -1, null, TestEnum.ONE);
    Field withEmptyFormat = new Field("a", "String", 50, -1, "", TestEnum.ONE);

    String nullXml = XmlMetadataUtil.serializeObjectToXml(withNullFormat);
    String emptyXml = XmlMetadataUtil.serializeObjectToXml(withEmptyFormat);

    assertFalse(nullXml.contains("<format"), "null is omitted from XML");
    assertTrue(emptyXml.contains("<format"), "empty string is written as a tag");
    assertNotEquals(nullXml, emptyXml, "disk format still distinguishes null from empty string");
    assertTrue(
        DialogOkContent.same(withNullFormat, withEmptyFormat),
        "dialog OK that turns null into empty string is not a content change");
  }
}
