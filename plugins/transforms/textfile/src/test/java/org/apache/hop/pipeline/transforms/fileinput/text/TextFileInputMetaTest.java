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

package org.apache.hop.pipeline.transforms.fileinput.text;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TextFileInputMetaTest {

  @BeforeEach
  void beforeEach() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    String[] classNames = {
      ValueMetaString.class.getName(), ValueMetaInteger.class.getName(),
      ValueMetaDate.class.getName(), ValueMetaNumber.class.getName()
    };
    for (String className : classNames) {
      registry.registerPluginClass(className, ValueMetaPluginType.class, ValueMetaPlugin.class);
    }
  }

  @Test
  void testLoadSave() throws Exception {
    Path path =
        Paths.get(Objects.requireNonNull(getClass().getResource("/text-file-input.xml")).toURI());
    String xml = Files.readString(path);
    TextFileInputMeta meta = new TextFileInputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        TextFileInputMeta.class,
        meta,
        new MemoryMetadataProvider());

    validate(meta);

    // Do a round trip:
    //
    String xmlCopy =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    TextFileInputMeta metaCopy = new TextFileInputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xmlCopy, TransformMeta.XML_TAG),
        TextFileInputMeta.class,
        metaCopy,
        new MemoryMetadataProvider());
    validate(metaCopy);
  }

  private static void validate(TextFileInputMeta meta) {
    assertTrue(meta.getFileInput().isAcceptingFilenames());
    assertEquals("acceptField", meta.getFileInput().getAcceptingField());
    assertEquals("acceptTransformName", meta.getFileInput().getAcceptingTransformName());
    assertEquals(";", meta.getDelimiter());
    assertEquals("\"", meta.getEnclosure());
    assertTrue(meta.isBreakInEnclosureAllowed());
    assertEquals("\\", meta.getEscapeCharacter());
    assertTrue(meta.getContent().isHeader());
    assertTrue(meta.getContent().isPrependFileName());
    assertEquals(2, meta.getNrHeaderLines());
    assertTrue(meta.getContent().isFooter());
    assertEquals(3, meta.getContent().getNrFooterLines());
    assertTrue(meta.getContent().isLineWrapped());
    assertEquals(3, meta.getContent().getNrWraps());
    assertTrue(meta.getContent().isLayoutPaged());
    assertEquals(80, meta.getContent().getNrLinesPerPage());
    assertTrue(meta.getContent().isNoEmptyLines());
    assertTrue(meta.getContent().isIncludeFilename());
    assertEquals("includeField", meta.getContent().getFilenameField());
    assertTrue(meta.getContent().isIncludeRowNumber());
    assertTrue(meta.getContent().isRowNumberByFile());
    assertEquals("rowNumField", meta.getContent().getRowNumberField());
    assertEquals("mixed", meta.getContent().getFileFormat());
    assertEquals("UTF-8", meta.getContent().getEncoding());
    assertEquals("Characters", meta.getContent().getLength());
    assertTrue(meta.getFileInput().isAddingResult());
    assertEquals("CSV", meta.getContent().getFileType());
    assertEquals("None", meta.getContent().getFileCompression());

    assertNotNull(meta.getFileInput().getInputFiles());
    assertEquals(2, meta.getFileInput().getInputFiles().size());

    assertNotNull(meta.getInputFields());
    TextFileInputField f1 = meta.getInputFields().get(0);
    assertEquals("f1", f1.getName());
    assertEquals(IValueMeta.TYPE_STRING, f1.getType());
    assertEquals("", f1.getFormat());
    TextFileInputField f2 = meta.getInputFields().get(1);
    assertEquals("f2", f2.getName());
    assertEquals(IValueMeta.TYPE_INTEGER, f2.getType());
    assertEquals("#", f2.getFormat());
    TextFileInputField f3 = meta.getInputFields().get(2);
    assertEquals("f3", f3.getName());
    assertEquals(IValueMeta.TYPE_DATE, f3.getType());
    assertEquals("yyyy/MM/dd", f3.getFormat());

    assertNotNull(meta.getFilters());
    assertEquals(1, meta.getFilters().size());
    TextFileFilter filter = meta.getFilters().getFirst();
    assertEquals("filterString", filter.getFilterString());
    assertTrue(filter.isFilterPositive());
    assertTrue(filter.isFilterLastLine());
  }
}
