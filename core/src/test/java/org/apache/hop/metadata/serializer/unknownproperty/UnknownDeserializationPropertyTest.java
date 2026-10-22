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

package org.apache.hop.metadata.serializer.unknownproperty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.OnUnknownDeserializationProperty;
import org.apache.hop.metadata.api.UnknownProperty;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.metadata.util.ReflectionUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.w3c.dom.Node;

/**
 * Verifies that the {@link org.apache.hop.metadata.api.OnUnknownDeserializationProperty} callback
 * is invoked for properties that do not map to any annotated field, in both the XML and the JSON
 * deserialization paths. This is how legacy/renamed keys are translated into current fields.
 */
class UnknownDeserializationPropertyTest {

  private final MemoryMetadataProvider provider = new MemoryMetadataProvider();

  @Test
  void xmlFiresCallbackForUnknownTag() throws Exception {
    Node node =
        XmlHandler.loadXmlString(
            XmlHandler.aroundTag(
                "element",
                "<value>new</value><legacy_flag>Y</legacy_flag><legacy_other>42</legacy_other>"),
            "element");
    PropertyWithLegacy loaded =
        XmlMetadataUtil.deSerializeFromXml(node, PropertyWithLegacy.class, provider);

    assertEquals("new", loaded.getValue());
    // The two unknown tags were handed to the callback.
    assertEquals("Y", loaded.getUnknown("legacy_flag"));
    assertEquals("42", loaded.getUnknown("legacy_other"));
  }

  @Test
  void xmlDoesNotFireForAnnotatedTags() throws Exception {
    Node node =
        XmlHandler.loadXmlString(
            XmlHandler.aroundTag("element", "<value>known</value><name>my-name</name>"), "element");
    PropertyWithLegacy loaded =
        XmlMetadataUtil.deSerializeFromXml(node, PropertyWithLegacy.class, provider);

    assertEquals("known", loaded.getValue());
    assertNull(loaded.getUnknown("value"));
    // name is an inherited annotated field (HopMetadataBase), not an unknown property.
    assertEquals("my-name", loaded.getName());
    assertNull(loaded.getUnknown("name"));
  }

  @Test
  void jsonFiresCallbackForUnknownKey() throws Exception {
    PropertyWithLegacy loaded =
        jsonRoundTrip("{\"name\":\"a\",\"value\":\"v\",\"legacy\":\"true\"}");

    assertEquals("v", loaded.getValue());
    assertEquals("true", loaded.getUnknown("legacy"));
    assertEquals("a", loaded.getName());
  }

  @Test
  void jsonDoesNotFireForAnnotatedKeys() throws Exception {
    PropertyWithLegacy loaded = jsonRoundTrip("{\"name\":\"b\",\"value\":\"w\"}");

    assertEquals("w", loaded.getValue());
    assertEquals("b", loaded.getName());
    assertEquals(0, loaded.getUnknownProperties().size());
  }

  private PropertyWithLegacy jsonRoundTrip(String json) throws IOException, HopException {
    JsonFactory factory = new JsonFactory();
    try (JsonParser parser = factory.createParser(json)) {
      // Move to the START_OBJECT so the parser is positioned like the serializer expects.
      parser.nextToken();
      JsonMetadataParser<PropertyWithLegacy> jsonParser =
          new JsonMetadataParser<>(PropertyWithLegacy.class, provider);
      return jsonParser.loadJsonObject(PropertyWithLegacy.class, parser);
    }
  }

  @ParameterizedTest
  @MethodSource("unknownStructuredValues")
  void jsonSkipsUnknownStructuredValueAndKeepsLaterFields(String unknownJson) throws Exception {
    // An unknown object/array value must be fully consumed so a subsequent known field ("name")
    // still loads. Without skipping the subtree, the nested closing brace/box would end the outer
    // object and the trailing "name" would be dropped.
    PropertyWithLegacy loaded =
        jsonRoundTrip("{\"value\":\"v\",\"legacy\":" + unknownJson + ",\"name\":\"tail\"}");

    assertEquals("v", loaded.getValue());
    assertEquals("tail", loaded.getName());
    // The unknown structured value cannot be represented as a scalar: the callback saw null.
    assertNull(loaded.getUnknown("legacy"));
  }

  static Stream<String> unknownStructuredValues() {
    return Stream.of("{\"a\":1}", "[1, 2, 3]", "{}", "[]");
  }

  @ParameterizedTest
  @ValueSource(strings = {"null", "true", "42", "\"null\"", "\"some text\"", "\"\""})
  void jsonScalarUnchangedExceptRealNull(String jsonValue) throws Exception {
    // Scalars are passed through unchanged. Only a real JSON null (no quotes) becomes Java null.
    PropertyWithLegacy loaded =
        jsonRoundTrip("{\"value\":\"v\",\"legacy\":" + jsonValue + ",\"name\":\"tail\"}");

    if ("null".equals(jsonValue)) {
      assertNull(loaded.getUnknown("legacy"));
    } else if (jsonValue.startsWith("\"") && jsonValue.endsWith("\"")) {
      // A JSON string literal ("null", "some text", ...) keeps its text, quotes stripped.
      assertEquals(jsonValue.substring(1, jsonValue.length() - 1), loaded.getUnknown("legacy"));
    } else {
      // Numbers and booleans keep their text representation.
      assertEquals(jsonValue, loaded.getUnknown("legacy"));
    }
    assertEquals("tail", loaded.getName());
  }

  @Test
  void jsonStringLiteralNullStaysNonNull() throws Exception {
    // A JSON string that reads "null" (quoted) is a real string, not a JSON null, so it must not
    // collapse to a Java null.
    PropertyWithLegacy loaded =
        jsonRoundTrip("{\"value\":\"v\",\"legacy\":\"null\",\"name\":\"tail\"}");
    assertEquals("null", loaded.getUnknown("legacy"));
  }

  @Test
  void multipleCallbacksAllFire() throws Exception {
    Node node =
        XmlHandler.loadXmlString(XmlHandler.aroundTag("element", "<a>1</a><b>2</b>"), "element");
    MultipleCallbacks loaded =
        XmlMetadataUtil.deSerializeFromXml(node, MultipleCallbacks.class, provider);
    assertEquals("1", loaded.getA());
    assertEquals("2", loaded.getB());
  }

  @Test
  void publicOverrideMasksParentCallback() throws Exception {
    Node node =
        XmlHandler.loadXmlString(XmlHandler.aroundTag("element", "<legacy>x</legacy>"), "element");
    Child loaded = XmlMetadataUtil.deSerializeFromXml(node, Child.class, provider);
    // The child override runs; the parent callback must NOT run.
    assertEquals("child:x", loaded.mapped);
    assertNull(loaded.parentSaw);
  }

  @Test
  void invalidSignatureThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ReflectionUtil.onUnknownDeserializationPropertyCallbacks(new BadSignature()));
  }

  @Test
  void nonPublicAnnotatedMethodIsIgnored() {
    List<Consumer<UnknownProperty>> callbacks =
        ReflectionUtil.onUnknownDeserializationPropertyCallbacks(new NonPublicCallback());
    // Only the public callback is discovered; the private and protected ones are ignored: exactly
    // one callback.
    assertEquals(1, callbacks.size());
  }

  // ---------------------------------------------------------------------------------------------
  // Helper metadata types used by the tests above.

  public static class MultipleCallbacks extends HopMetadataBase {
    @OnUnknownDeserializationProperty
    public void onA(UnknownProperty p) {
      if ("a".equals(p.getName())) {
        a = p.getValue();
      }
    }

    @OnUnknownDeserializationProperty
    public void onB(UnknownProperty p) {
      if ("b".equals(p.getName())) {
        b = p.getValue();
      }
    }

    @Getter private String a;
    @Getter private String b;
  }

  public static class ParentWithCallback extends HopMetadataBase {
    String parentSaw;

    @OnUnknownDeserializationProperty
    public void onUnknown(UnknownProperty p) {
      parentSaw = p.getValue();
    }
  }

  public static class Child extends ParentWithCallback {
    String mapped;

    @Override
    @OnUnknownDeserializationProperty
    public void onUnknown(UnknownProperty p) {
      mapped = "child:" + p.getValue();
    }
  }

  public static class BadSignature extends HopMetadataBase {
    @OnUnknownDeserializationProperty
    public void bad(String wrong) {
      // Wrong parameter type.
    }
  }

  public static class NonPublicCallback extends HopMetadataBase {
    private String seen;

    @OnUnknownDeserializationProperty
    private void onPrivate(UnknownProperty p) {
      seen = p.getValue();
    }

    @OnUnknownDeserializationProperty
    public void onPublic(UnknownProperty p) {
      seen = p.getValue();
    }

    @SuppressWarnings("unused")
    private String getSeen() {
      return seen;
    }
  }
}
