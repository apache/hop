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
package org.apache.hop.pipeline.transforms.plugincatalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.junit.jupiter.api.Test;

class PluginCatalogReaderTest {

  /** Fixture nested group element. */
  static class Mapping {
    @HopMetadataProperty(key = "source")
    private String source;

    @HopMetadataProperty(key = "target")
    private String target;
  }

  /** Fixture metadata class with a password, a default-keyed field, and a nested list group. */
  static class SampleMeta {
    @HopMetadataProperty(key = "message")
    private String messageField;

    @HopMetadataProperty(key = "secret", password = true)
    private String secret;

    // No key() -> xml key falls back to the field name.
    @HopMetadataProperty private int batchSize;

    @HopMetadataProperty(key = "mappings")
    private List<Mapping> mappings;
  }

  static class ChildMeta extends SampleMeta {
    @HopMetadataProperty(key = "extra")
    private String extra;
  }

  private static PropertyRecord byXmlKey(List<PropertyRecord> props, String key) {
    Optional<PropertyRecord> found = props.stream().filter(p -> p.xmlKey().equals(key)).findFirst();
    assertTrue(found.isPresent(), "expected property with xml_key=" + key);
    return found.get();
  }

  @Test
  void extractsAnnotatedFieldsWithKeyFallback() {
    List<PropertyRecord> props = PluginCatalogReader.extractProperties(SampleMeta.class);

    PropertyRecord message = byXmlKey(props, "message");
    assertEquals("messageField", message.field());
    assertEquals("String", message.javaType());
    assertEquals("", message.group());
    assertEquals(false, message.password());

    // Field without key() uses the Java field name as xml key.
    PropertyRecord batch = byXmlKey(props, "batchSize");
    assertEquals("int", batch.javaType());
  }

  @Test
  void detectsPasswordProperties() {
    List<PropertyRecord> props = PluginCatalogReader.extractProperties(SampleMeta.class);
    assertTrue(byXmlKey(props, "secret").password());
  }

  @Test
  void descendsOneLevelIntoNestedGroups() {
    List<PropertyRecord> props = PluginCatalogReader.extractProperties(SampleMeta.class);

    // The list field itself is present...
    assertNotNull(byXmlKey(props, "mappings"));
    // ...and its element's properties are tagged with the parent key as their group.
    PropertyRecord source = byXmlKey(props, "source");
    assertEquals("mappings", source.group());
    assertEquals("target", byXmlKey(props, "target").xmlKey());
  }

  @Test
  void includesInheritedFields() {
    List<PropertyRecord> props = PluginCatalogReader.extractProperties(ChildMeta.class);
    assertNotNull(byXmlKey(props, "extra"));
    assertNotNull(byXmlKey(props, "message"));
  }

  @Test
  void serializesPropertiesToJson() {
    List<PropertyRecord> props =
        List.of(new PropertyRecord("secret", "secret", "String", true, "auth"));
    String json = PluginCatalogReader.propertiesToJson(props);
    assertEquals(
        "[{\"field\":\"secret\",\"xml_key\":\"secret\",\"java_type\":\"String\","
            + "\"password\":true,\"group\":\"auth\"}]",
        json);
  }

  @Test
  void jsonEscapesSpecialCharacters() {
    List<PropertyRecord> props = List.of(new PropertyRecord("a\"b", "x", "String", false, ""));
    String json = PluginCatalogReader.propertiesToJson(props);
    assertTrue(json.contains("a\\\"b"), "quotes should be escaped: " + json);
  }

  @Test
  void emptyPropertyListSerializesToEmptyArray() {
    assertEquals("[]", PluginCatalogReader.propertiesToJson(List.of()));
  }

  @Test
  void jsonQuoterIsNullSafe() {
    // null fields must not blow up serialization.
    String json =
        PluginCatalogReader.propertiesToJson(
            List.of(new PropertyRecord(null, "k", null, false, null)));
    assertTrue(json.contains("\"field\":null"), json);
    assertTrue(json.contains("\"java_type\":null"), json);
  }

  @Test
  void reportsAResolvableLocaleTag() {
    String tag = PluginCatalogReader.currentLocaleTag();
    // Deliberately not asserting a specific locale: this must hold on any machine, and the point
    // of the column is that consumers can tell which locale the labels came from.
    assertNotNull(tag);
    assertTrue(!tag.isBlank(), "locale tag should not be blank");
    assertEquals(tag, java.util.Locale.forLanguageTag(tag).toLanguageTag());
  }
}
