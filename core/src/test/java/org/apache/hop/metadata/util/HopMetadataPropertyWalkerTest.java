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
package org.apache.hop.metadata.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.util.HopMetadataPropertyWalker.StringProperty;
import org.junit.jupiter.api.Test;

class HopMetadataPropertyWalkerTest {

  static class SimpleMeta {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String connection = "warehouse";

    @HopMetadataProperty(key = "sql")
    String sql = "SELECT 1";

    String unannotated = "ignored";
  }

  static class NestedItem {
    @HopMetadataProperty(
        key = "name",
        hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String name;

    NestedItem(String name) {
      this.name = name;
    }
  }

  static class NestedMeta {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String connection = "primary";

    @HopMetadataProperty
    List<NestedItem> items = List.of(new NestedItem("second"), new NestedItem("third"));
  }

  static class TwoConnectionsMeta {
    @HopMetadataProperty(
        key = "referenceConnection",
        hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String reference = "ref-db";

    @HopMetadataProperty(
        key = "compareConnection",
        hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String compare = "cmp-db";
  }

  static class UnannotatedConnectionMeta {
    @HopMetadataProperty(key = "connection")
    String connection = "hidden";
  }

  @Test
  void collectsAnnotatedConnectionStrings() {
    List<StringProperty> found =
        HopMetadataPropertyWalker.collectStrings(
            new SimpleMeta(), HopMetadataPropertyType.RDBMS_CONNECTION);

    assertEquals(1, found.size());
    assertEquals("connection", found.get(0).key());
    assertEquals("warehouse", found.get(0).value());
  }

  @Test
  void descendsIntoNestedLists() {
    List<StringProperty> found =
        HopMetadataPropertyWalker.collectStrings(
            new NestedMeta(), HopMetadataPropertyType.RDBMS_CONNECTION);

    assertEquals(3, found.size());
    assertEquals(
        List.of("primary", "second", "third"), found.stream().map(StringProperty::value).toList());
  }

  @Test
  void collectsTwoConnectionFieldsOnOneObject() {
    List<StringProperty> found =
        HopMetadataPropertyWalker.collectStrings(
            new TwoConnectionsMeta(), HopMetadataPropertyType.RDBMS_CONNECTION);

    assertEquals(2, found.size());
    assertTrue(found.stream().anyMatch(p -> "referenceConnection".equals(p.key())));
    assertTrue(found.stream().anyMatch(p -> "compareConnection".equals(p.key())));
  }

  @Test
  void ignoresConnectionFieldsWithoutThePropertyType() {
    List<StringProperty> found =
        HopMetadataPropertyWalker.collectStrings(
            new UnannotatedConnectionMeta(), HopMetadataPropertyType.RDBMS_CONNECTION);

    assertTrue(found.isEmpty());
  }

  @Test
  void nullRootYieldsNothing() {
    assertTrue(
        HopMetadataPropertyWalker.collectStrings(null, HopMetadataPropertyType.RDBMS_CONNECTION)
            .isEmpty());
  }
}
