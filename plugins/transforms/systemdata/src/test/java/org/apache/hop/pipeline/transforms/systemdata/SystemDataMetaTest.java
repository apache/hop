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

package org.apache.hop.pipeline.transforms.systemdata;

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
import org.junit.jupiter.api.Test;

/** Unit test for {@link SystemDataMeta} */
class SystemDataMetaTest {
  @Test
  void testLoadSave() throws Exception {
    Path path = Paths.get(Objects.requireNonNull(getClass().getResource("/transform.xml")).toURI());
    String xml = Files.readString(path);
    SystemDataMeta meta = new SystemDataMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        SystemDataMeta.class,
        meta,
        new MemoryMetadataProvider());

    validate(meta);

    // Do a round trip:
    //
    String xmlCopy =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    SystemDataMeta metaCopy = new SystemDataMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xmlCopy, TransformMeta.XML_TAG),
        SystemDataMeta.class,
        metaCopy,
        new MemoryMetadataProvider());
    validate(metaCopy);
  }

  @Test
  void testFieldWithMissingTypeDefaultsToNoneAndDoesNotThrow() throws Exception {
    // Regression: a field whose <type> is missing/empty (legacy or hand-edited pipelines) used to
    // map to NONE. The enum migration made it deserialize to null, causing NPEs in getFields(),
    // check() and the dialog. getFieldType() must never return null.
    String xml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + "<fields><field><name>legacy</name></field></fields>"
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    SystemDataMeta meta = new SystemDataMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        SystemDataMeta.class,
        meta,
        new MemoryMetadataProvider());

    assertEquals(1, meta.getFields().size());
    assertEquals(SystemDataType.NONE, meta.getFields().getFirst().getFieldType());

    // None of the consumers below must throw a NullPointerException.
    RowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "t", null, null, new Variables(), null);
    assertEquals(IValueMeta.TYPE_NONE, rowMeta.getValueMeta(0).getType());

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, null, null, null, null, null, new Variables(), null);
    assertFalse(remarks.isEmpty());
  }

  @Test
  void testGetFieldsDefaultType() throws Exception {
    SystemDataMeta meta = new SystemDataMeta();

    SystemDataMeta.SystemInfoField field = new SystemDataMeta.SystemInfoField();
    field.setFieldName("noneField");
    field.setFieldType(SystemDataType.NONE);

    meta.getFields().add(field);

    RowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "t", null, null, new Variables(), null);

    assertEquals(IValueMeta.TYPE_NONE, rowMeta.getValueMeta(0).getType());
  }

  @Test
  void testCheckErrorWhenTypeNone() {
    SystemDataMeta meta = new SystemDataMeta();

    SystemDataMeta.SystemInfoField field = new SystemDataMeta.SystemInfoField();
    field.setFieldName("f1");
    field.setFieldType(SystemDataType.NONE);

    meta.getFields().add(field);

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, null, null, null, null, null, new Variables(), null);

    assertFalse(remarks.isEmpty());
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.getFirst().getType());
  }

  @Test
  void testCheckOk() {
    SystemDataMeta meta = new SystemDataMeta();

    SystemDataMeta.SystemInfoField field = new SystemDataMeta.SystemInfoField();
    field.setFieldName("f1");
    field.setFieldType(SystemDataType.SYSTEM_DATE);

    meta.getFields().add(field);

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, null, null, null, null, null, new Variables(), null);

    assertEquals(1, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.getFirst().getType());
  }

  @Test
  void testCloneDeepCopy() {
    SystemDataMeta meta = new SystemDataMeta();

    SystemDataMeta.SystemInfoField field = new SystemDataMeta.SystemInfoField();
    field.setFieldName("f1");
    field.setFieldType(SystemDataType.SYSTEM_DATE);

    meta.getFields().add(field);

    SystemDataMeta cloned = (SystemDataMeta) meta.clone();

    assertNotSame(meta, cloned);
    assertEquals(meta.getFields().size(), cloned.getFields().size());

    assertNotSame(meta.getFields().getFirst(), cloned.getFields().getFirst());
  }

  @Test
  void testCopyConstructor() {
    SystemDataMeta meta = new SystemDataMeta();

    SystemDataMeta.SystemInfoField field = new SystemDataMeta.SystemInfoField();
    field.setFieldName("f1");
    field.setFieldType(SystemDataType.SYSTEM_DATE);

    meta.getFields().add(field);

    SystemDataMeta copy = new SystemDataMeta(meta);

    assertEquals(1, copy.getFields().size());
    assertNotSame(meta.getFields().getFirst(), copy.getFields().getFirst());
  }

  private static void validate(SystemDataMeta meta) {
    assertNotNull(meta.getFields());
    assertFalse(meta.getFields().isEmpty());
    assertEquals(8, meta.getFields().size());
    SystemDataMeta.SystemInfoField f1 = meta.getFields().getFirst();
    assertEquals("variable_sysdate", f1.getFieldName());
    assertEquals(SystemDataType.SYSTEM_DATE, f1.getFieldType());

    SystemDataMeta.SystemInfoField f2 = meta.getFields().get(1);
    assertEquals("fixed_sysdate", f2.getFieldName());
    assertEquals(SystemDataType.SYSTEM_START, f2.getFieldType());

    SystemDataMeta.SystemInfoField f3 = meta.getFields().get(2);
    assertEquals("JVM max memory", f3.getFieldName());
    assertEquals(SystemDataType.JVM_MAX_MEMORY, f3.getFieldType());

    SystemDataMeta.SystemInfoField f4 = meta.getFields().get(3);
    assertEquals("IP address", f4.getFieldName());
    assertEquals(SystemDataType.IP_ADDRESS, f4.getFieldType());

    SystemDataMeta.SystemInfoField f5 = meta.getFields().get(4);
    assertEquals("real_Hostname", f5.getFieldName());
    assertEquals(SystemDataType.HOSTNAME_REAL, f5.getFieldType());

    SystemDataMeta.SystemInfoField f6 = meta.getFields().get(5);
    assertEquals("Network_Hostname", f6.getFieldName());
    assertEquals(SystemDataType.HOSTNAME, f6.getFieldType());

    SystemDataMeta.SystemInfoField f7 = meta.getFields().get(6);
    assertEquals("Available Processors", f7.getFieldName());
    assertEquals(SystemDataType.AVAILABLE_PROCESSORS, f7.getFieldType());

    SystemDataMeta.SystemInfoField f8 = meta.getFields().get(7);
    assertEquals("JVM Total Memory", f8.getFieldName());
    assertEquals(SystemDataType.JVM_TOTAL_MEMORY, f8.getFieldType());
  }
}
