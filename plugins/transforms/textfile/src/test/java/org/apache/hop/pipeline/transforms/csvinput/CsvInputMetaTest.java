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

package org.apache.hop.pipeline.transforms.csvinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.inject.HopMetadataInjector;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CsvInputMetaTest {
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
    HopClientEnvironment.init();
    Path path =
        Paths.get(Objects.requireNonNull(getClass().getResource("/csv-file-input.xml")).toURI());
    String xml = Files.readString(path);
    CsvInputMeta meta = new CsvInputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        CsvInputMeta.class,
        meta,
        new MemoryMetadataProvider());

    validate(meta);

    // Do a round trip:
    //
    String xmlCopy =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    CsvInputMeta metaCopy = new CsvInputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xmlCopy, TransformMeta.XML_TAG),
        CsvInputMeta.class,
        metaCopy,
        new MemoryMetadataProvider());
    validate(metaCopy);
  }

  private static void validate(CsvInputMeta meta) {
    assertEquals("filename.csv", meta.getFilename());
    assertEquals("filenameField", meta.getFilenameField());
    assertEquals("rowNum", meta.getRowNumField());
    assertTrue(meta.isIncludingFilename());
    assertEquals(",", meta.getDelimiter());
    assertEquals("\"", meta.getEnclosure());
    assertTrue(meta.isHeaderPresent());
    assertEquals("50000", meta.getBufferSize());
    assertEquals("fields", meta.getSchemaDefinition());
    assertTrue(meta.isIgnoreFields());
    assertTrue(meta.isLazyConversionActive());
    assertTrue(meta.isAddResult());
    assertTrue(meta.isRunningInParallel());
    assertTrue(meta.isNewlinePossibleInFields());
    assertEquals("UTF-8", meta.getEncoding());
    assertNotNull(meta.getInputFields());
    assertEquals(2, meta.getInputFields().size());

    CsvInputField f1 = meta.getInputFields().get(0);
    assertEquals("id", f1.getName());
    assertEquals(IValueMeta.TYPE_INTEGER, f1.getType());
    assertEquals(9, f1.getLength());
    assertEquals(0, f1.getPrecision());
    assertEquals(IValueMeta.TRIM_TYPE_BOTH, f1.getTrimType());

    CsvInputField f2 = meta.getInputFields().get(1);
    assertEquals("name", f2.getName());
    assertEquals(IValueMeta.TYPE_STRING, f2.getType());
    assertEquals(100, f2.getLength());
    assertEquals(-1, f2.getPrecision());
    assertEquals(IValueMeta.TRIM_TYPE_RIGHT, f2.getTrimType());
  }

  @Test
  void testMappings() throws Exception {
    Map<String, Set<String>> map = HopMetadataInjector.findInjectionGroupKeys(CsvInputMeta.class);
    assertNotNull(map);
    assertEquals(1, map.size());
    Set<String> fields = map.get("INPUT_FIELDS");
    assertEquals(9, fields.size());
  }
}
