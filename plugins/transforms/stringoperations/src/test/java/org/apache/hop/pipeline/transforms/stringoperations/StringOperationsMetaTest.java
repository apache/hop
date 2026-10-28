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

package org.apache.hop.pipeline.transforms.stringoperations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.injection.bean.BeanInjectionInfo;
import org.apache.hop.core.injection.bean.BeanInjector;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

class StringOperationsMetaTest {
  @Test
  void testLoadSave() throws Exception {
    Path path = Paths.get(Objects.requireNonNull(getClass().getResource("/transform.xml")).toURI());
    String xml = Files.readString(path);
    StringOperationsMeta meta = new StringOperationsMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        StringOperationsMeta.class,
        meta,
        new MemoryMetadataProvider());

    validate(meta);

    // Do a round trip:
    //
    String xmlCopy =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    StringOperationsMeta metaCopy = new StringOperationsMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xmlCopy, TransformMeta.XML_TAG),
        StringOperationsMeta.class,
        metaCopy,
        new MemoryMetadataProvider());
    validate(metaCopy);
  }

  private static void validate(StringOperationsMeta meta) {
    assertNotNull(meta.getOperations());
    assertFalse(meta.getOperations().isEmpty());
    assertEquals(4, meta.getOperations().size());
    StringOperationsMeta.StringOperation o1 = meta.getOperations().get(0);
    assertEquals("desc_t", o1.getFieldInStream());
    assertEquals("desc_trimmed", o1.getFieldOutStream());
    assertEquals(StringOperationsMeta.TrimType.BOTH, o1.getTrimType());
    assertEquals(StringOperationsMeta.LowerUpper.NONE, o1.getLowerUpper());
    assertEquals(StringOperationsMeta.Padding.NONE, o1.getPaddingType());
    assertTrue(StringUtils.isEmpty(o1.getPadChar()));
    assertTrue(StringUtils.isEmpty(o1.getPadLen()));
    assertEquals(StringOperationsMeta.InitCap.NO, o1.getInitCap());
    assertEquals(StringOperationsMeta.MaskXml.NONE, o1.getMaskXml());
    assertEquals(StringOperationsMeta.Digits.NONE, o1.getDigits());
    assertEquals(StringOperationsMeta.RemoveSpecialChars.NONE, o1.getRemoveSpecialChars());

    StringOperationsMeta.StringOperation o2 = meta.getOperations().get(1);
    assertEquals("desc_u", o2.getFieldInStream());
    assertEquals("desc_upper", o2.getFieldOutStream());
    assertEquals(StringOperationsMeta.TrimType.NONE, o2.getTrimType());
    assertEquals(StringOperationsMeta.LowerUpper.UPPER, o2.getLowerUpper());
    assertEquals(StringOperationsMeta.Padding.NONE, o2.getPaddingType());
    assertTrue(StringUtils.isEmpty(o2.getPadChar()));
    assertTrue(StringUtils.isEmpty(o2.getPadLen()));
    assertEquals(StringOperationsMeta.InitCap.NO, o2.getInitCap());
    assertEquals(StringOperationsMeta.MaskXml.NONE, o2.getMaskXml());
    assertEquals(StringOperationsMeta.Digits.NONE, o2.getDigits());
    assertEquals(StringOperationsMeta.RemoveSpecialChars.NONE, o2.getRemoveSpecialChars());

    StringOperationsMeta.StringOperation o3 = meta.getOperations().get(2);
    assertEquals("desc_p", o3.getFieldInStream());
    assertEquals("desc_padded", o3.getFieldOutStream());
    assertEquals(StringOperationsMeta.TrimType.NONE, o3.getTrimType());
    assertEquals(StringOperationsMeta.LowerUpper.NONE, o3.getLowerUpper());
    assertEquals(StringOperationsMeta.Padding.LEFT, o3.getPaddingType());
    assertEquals("#", o3.getPadChar());
    assertEquals("25", o3.getPadLen());
    assertEquals(StringOperationsMeta.InitCap.NO, o3.getInitCap());
    assertEquals(StringOperationsMeta.MaskXml.NONE, o3.getMaskXml());
    assertEquals(StringOperationsMeta.Digits.NONE, o3.getDigits());
    assertEquals(StringOperationsMeta.RemoveSpecialChars.NONE, o3.getRemoveSpecialChars());

    StringOperationsMeta.StringOperation o4 = meta.getOperations().get(3);
    assertEquals("desc_i", o4.getFieldInStream());
    assertEquals("desc_initcapped", o4.getFieldOutStream());
    assertEquals(StringOperationsMeta.TrimType.NONE, o4.getTrimType());
    assertEquals(StringOperationsMeta.LowerUpper.NONE, o4.getLowerUpper());
    assertEquals(StringOperationsMeta.Padding.NONE, o4.getPaddingType());
    assertTrue(StringUtils.isEmpty(o2.getPadChar()));
    assertTrue(StringUtils.isEmpty(o2.getPadLen()));
    assertEquals(StringOperationsMeta.InitCap.YES, o4.getInitCap());
    assertEquals(StringOperationsMeta.MaskXml.NONE, o4.getMaskXml());
    assertEquals(StringOperationsMeta.Digits.NONE, o4.getDigits());
    assertEquals(StringOperationsMeta.RemoveSpecialChars.NONE, o4.getRemoveSpecialChars());
  }

  @Test
  void testLoadSave2() throws Exception {
    Path path =
        Paths.get(Objects.requireNonNull(getClass().getResource("/transform2.xml")).toURI());
    String xml = Files.readString(path);
    StringOperationsMeta meta = new StringOperationsMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        StringOperationsMeta.class,
        meta,
        new MemoryMetadataProvider());

    validate2(meta);

    // Do a round trip:
    //
    String xmlCopy =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    StringOperationsMeta metaCopy = new StringOperationsMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xmlCopy, TransformMeta.XML_TAG),
        StringOperationsMeta.class,
        metaCopy,
        new MemoryMetadataProvider());
    validate2(metaCopy);
  }

  private void validate2(StringOperationsMeta meta) {
    assertNotNull(meta.getOperations());
    assertFalse(meta.getOperations().isEmpty());
    assertEquals(1, meta.getOperations().size());
    StringOperationsMeta.StringOperation o1 = meta.getOperations().get(0);
    assertEquals("in_field", o1.getFieldInStream());
    assertEquals("out_field", o1.getFieldOutStream());
    assertEquals(StringOperationsMeta.TrimType.BOTH, o1.getTrimType());
    assertEquals(StringOperationsMeta.LowerUpper.UPPER, o1.getLowerUpper());
    assertEquals(StringOperationsMeta.Padding.RIGHT, o1.getPaddingType());
    assertEquals(" ", o1.getPadChar());
    assertEquals("20", o1.getPadLen());
    assertEquals(StringOperationsMeta.InitCap.YES, o1.getInitCap());
    assertEquals(StringOperationsMeta.MaskXml.UNESCAPE_XML, o1.getMaskXml());
    assertEquals(StringOperationsMeta.Digits.DIGITS_REMOVE, o1.getDigits());
    assertEquals(StringOperationsMeta.RemoveSpecialChars.CRLF, o1.getRemoveSpecialChars());
  }

  /** Issue #9120: the operations must be offered to ETL Metadata Injection. */
  @Test
  void testInjectionMetadata() {
    BeanInjectionInfo<StringOperationsMeta> injectionInfo =
        new BeanInjectionInfo<>(StringOperationsMeta.class);

    assertEquals(
        Set.of(
            "SOURCEFIELDS",
            "TARGETFIELDS",
            "TRIMTYPE",
            "LOWERUPPER",
            "INITCAP",
            "MASKXML",
            "DIGITS",
            "SPECIALCHARS",
            "PADDING",
            "PADLEN",
            "PADCHAR"),
        injectionInfo.getProperties().keySet());
    assertEquals("FIELDS", injectionInfo.getProperties().get("SOURCEFIELDS").getGroupKey());
    assertEquals(
        "In stream fields",
        injectionInfo.getProperties().get("SOURCEFIELDS").getTranslatedDescription());
  }

  @Test
  void testInjection() throws Exception {
    BeanInjectionInfo<StringOperationsMeta> injectionInfo =
        new BeanInjectionInfo<>(StringOperationsMeta.class);
    BeanInjector<StringOperationsMeta> injector =
        new BeanInjector<>(injectionInfo, new MemoryMetadataProvider());

    IRowMeta rowMeta =
        new RowMetaBuilder().addString("in").addString("trim").addString("case").build();
    List<RowMetaAndData> rows =
        List.of(
            new RowMetaAndData(rowMeta, "name", "both", "upper"),
            new RowMetaAndData(rowMeta, "city", "left", "lower"));

    StringOperationsMeta meta = new StringOperationsMeta();
    injector.setProperty(meta, "SOURCEFIELDS", rows, "in");
    injector.setProperty(meta, "TRIMTYPE", rows, "trim");
    injector.setProperty(meta, "LOWERUPPER", rows, "case");

    assertEquals(2, meta.getOperations().size());
    StringOperationsMeta.StringOperation o0 = meta.getOperations().get(0);
    assertEquals("name", o0.getFieldInStream());
    assertEquals(StringOperationsMeta.TrimType.BOTH, o0.getTrimType());
    assertEquals(StringOperationsMeta.LowerUpper.UPPER, o0.getLowerUpper());
    StringOperationsMeta.StringOperation o1 = meta.getOperations().get(1);
    assertEquals("city", o1.getFieldInStream());
    assertEquals(StringOperationsMeta.TrimType.LEFT, o1.getTrimType());
    assertEquals(StringOperationsMeta.LowerUpper.LOWER, o1.getLowerUpper());
  }
}
