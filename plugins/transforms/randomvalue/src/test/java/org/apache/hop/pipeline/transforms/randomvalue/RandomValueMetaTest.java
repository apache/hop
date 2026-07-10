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
package org.apache.hop.pipeline.transforms.randomvalue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Unit test for {@link RandomValueMeta}. */
class RandomValueMetaTest {

  @Test
  void testSerialization() throws Exception {
    RandomValueMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/generate-random-values-transform.xml", RandomValueMeta.class);
    assertEquals("12345", meta.getSeed());
    assertEquals(7, meta.getFields().size());

    List<RandomValueMeta.RVField> fields = meta.getFields();

    assertEquals(RandomValueMeta.RandomType.NUMBER, fields.get(0).getType());
    assertEquals("num", fields.get(0).getName());
    assertEquals(RandomValueMeta.RandomType.INTEGER, fields.get(1).getType());
    assertEquals("int", fields.get(1).getName());
    assertEquals(RandomValueMeta.RandomType.STRING, fields.get(2).getType());
    assertEquals("str", fields.get(2).getName());
    assertEquals(RandomValueMeta.RandomType.UUID, fields.get(3).getType());
    assertEquals("uuid", fields.get(3).getName());
    assertEquals(RandomValueMeta.RandomType.UUID4, fields.get(4).getType());
    assertEquals("uuid4", fields.get(4).getName());
    assertEquals(RandomValueMeta.RandomType.HMAC_MD5, fields.get(5).getType());
    assertEquals("hmac_md5", fields.get(5).getName());
    assertEquals(RandomValueMeta.RandomType.HMAC_SHA1, fields.get(6).getType());
    assertEquals("hmac_sha1", fields.get(6).getName());
  }

  @Test
  void testLoadSaveRoundTrip() throws Exception {
    Path path =
        Paths.get(
            Objects.requireNonNull(getClass().getResource("/generate-random-values-transform.xml"))
                .toURI());
    String xml = Files.readString(path);
    RandomValueMeta meta = new RandomValueMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        RandomValueMeta.class,
        meta,
        new MemoryMetadataProvider());

    validateLoadedMeta(meta);

    String xmlCopy =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    RandomValueMeta metaCopy = new RandomValueMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xmlCopy, TransformMeta.XML_TAG),
        RandomValueMeta.class,
        metaCopy,
        new MemoryMetadataProvider());

    validateLoadedMeta(metaCopy);
  }

  @Test
  void testRvFieldDefaultsToNone() {
    RandomValueMeta.RVField field = new RandomValueMeta.RVField();

    assertEquals(RandomValueMeta.RandomType.NONE, field.getType());
  }

  @Test
  void testRvFieldCopyConstructor() {
    RandomValueMeta.RVField original = new RandomValueMeta.RVField();
    original.setName("field1");
    original.setType(RandomValueMeta.RandomType.STRING);

    RandomValueMeta.RVField copy = new RandomValueMeta.RVField(original);

    assertEquals("field1", copy.getName());
    assertEquals(RandomValueMeta.RandomType.STRING, copy.getType());
    assertNotSame(original, copy);
  }

  @Test
  void testCloneDeepCopy() {
    RandomValueMeta meta = sampleMeta();

    RandomValueMeta cloned = meta.clone();

    assertNotSame(meta, cloned);
    assertEquals(meta.getSeed(), cloned.getSeed());
    assertEquals(meta.getFields().size(), cloned.getFields().size());
    assertNotSame(meta.getFields().getFirst(), cloned.getFields().getFirst());
    assertEquals(meta.getFields().getFirst().getName(), cloned.getFields().getFirst().getName());
    assertEquals(meta.getFields().getFirst().getType(), cloned.getFields().getFirst().getType());
  }

  @Test
  void testCopyConstructor() {
    RandomValueMeta meta = sampleMeta();

    RandomValueMeta copy = new RandomValueMeta(meta);

    assertEquals(meta.getSeed(), copy.getSeed());
    assertEquals(1, copy.getFields().size());
    assertNotSame(meta.getFields().getFirst(), copy.getFields().getFirst());
  }

  @ParameterizedTest
  @EnumSource(RandomValueMeta.RandomType.class)
  void testGetFieldsProducesExpectedValueMetaType(RandomValueMeta.RandomType type) {
    RandomValueMeta meta = new RandomValueMeta();
    RandomValueMeta.RVField field = new RandomValueMeta.RVField();
    field.setName("f1");
    field.setType(type);
    meta.getFields().add(field);

    RowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "transform", null, null, new Variables(), null);

    assertEquals(1, rowMeta.size());
    IValueMeta valueMeta = rowMeta.getValueMeta(0);
    assertEquals("f1", valueMeta.getName());
    assertEquals("transform", valueMeta.getOrigin());

    switch (type) {
      case NUMBER -> {
        assertEquals(IValueMeta.TYPE_NUMBER, valueMeta.getType());
        assertEquals(10, valueMeta.getLength());
        assertEquals(5, valueMeta.getPrecision());
      }
      case INTEGER -> {
        assertEquals(IValueMeta.TYPE_INTEGER, valueMeta.getType());
        assertEquals(10, valueMeta.getLength());
        assertEquals(0, valueMeta.getPrecision());
      }
      case STRING -> {
        assertEquals(IValueMeta.TYPE_STRING, valueMeta.getType());
        assertEquals(13, valueMeta.getLength());
      }
      case UUID, UUID4 -> {
        assertEquals(IValueMeta.TYPE_STRING, valueMeta.getType());
        assertEquals(36, valueMeta.getLength());
      }
      case HMAC_MD5, HMAC_SHA1, HMAC_SHA256, HMAC_SHA512, HMAC_SHA384 -> {
        assertEquals(IValueMeta.TYPE_STRING, valueMeta.getType());
        assertEquals(100, valueMeta.getLength());
      }
      case NONE -> assertEquals(IValueMeta.TYPE_NONE, valueMeta.getType());
    }
  }

  @Test
  void testCheckErrorWhenTypeNone() {
    RandomValueMeta meta = new RandomValueMeta();
    RandomValueMeta.RVField field = new RandomValueMeta.RVField();
    field.setName("missing-type");
    field.setType(RandomValueMeta.RandomType.NONE);
    meta.getFields().add(field);

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, null, null, null, null, null, new Variables(), null);

    assertFalse(remarks.isEmpty());
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.getFirst().getType());
  }

  @Test
  void testCheckOkWhenAllTypesSpecified() {
    RandomValueMeta meta = sampleMeta();

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, null, null, null, null, null, new Variables(), null);

    assertEquals(1, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.getFirst().getType());
  }

  @Test
  void testCheckReportsOnlyMissingTypes() {
    RandomValueMeta meta = new RandomValueMeta();

    RandomValueMeta.RVField valid = new RandomValueMeta.RVField();
    valid.setName("valid");
    valid.setType(RandomValueMeta.RandomType.NUMBER);
    meta.getFields().add(valid);

    RandomValueMeta.RVField invalid = new RandomValueMeta.RVField();
    invalid.setName("invalid");
    invalid.setType(RandomValueMeta.RandomType.NONE);
    meta.getFields().add(invalid);

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, null, null, null, null, null, new Variables(), null);

    assertEquals(1, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.getFirst().getType());
  }

  private static RandomValueMeta sampleMeta() {
    RandomValueMeta meta = new RandomValueMeta();
    meta.setSeed("seed");

    RandomValueMeta.RVField field = new RandomValueMeta.RVField();
    field.setName("num");
    field.setType(RandomValueMeta.RandomType.NUMBER);
    meta.getFields().add(field);

    return meta;
  }

  private static void validateLoadedMeta(RandomValueMeta meta) {
    assertNotNull(meta.getFields());
    assertFalse(meta.getFields().isEmpty());
    assertEquals("12345", meta.getSeed());
    assertEquals(7, meta.getFields().size());
    assertEquals(RandomValueMeta.RandomType.NUMBER, meta.getFields().getFirst().getType());
    assertEquals("num", meta.getFields().getFirst().getName());
  }
}
