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
 *
 */

package org.apache.hop.pipeline.transforms.jsonoutputenhanced;

import static org.apache.hop.core.util.Utils.isEmpty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.apache.hop.core.Const;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

class JsonEOutputMetaTest {

  @Test
  void newMeta_defaultJsonBlocIsEmpty() {
    assertTrue(isEmpty(new JsonEOutputMeta().getJsonBloc()));
  }

  @Test
  void deserializeSelfClosingJsonBloc_overridesConstructor() throws Exception {
    String xml =
        """
        <transform>
          <outputValue>prompt</outputValue>
          <jsonBloc/>
          <operation_type>outputvalue</operation_type>
          <use_arrays_with_single_instance>N</use_arrays_with_single_instance>
          <use_single_item_per_group>N</use_single_item_per_group>
          <json_prittified>N</json_prittified>
          <encoding>UTF-8</encoding>
          <addtoresult>N</addtoresult>
          <file>
            <name/>
            <split_output_after>0</split_output_after>
            <extention>json</extention>
            <append>N</append>
            <split>N</split>
            <haspartno>N</haspartno>
            <add_date>N</add_date>
            <add_time>N</add_time>
            <create_parent_folder>N</create_parent_folder>
            <doNotOpenNewFileInit>N</doNotOpenNewFileInit>
          </file>
          <additional_fields>
            <json_size_field/>
          </additional_fields>
          <key_fields/>
          <fields>
            <field>
              <name>role</name>
              <element>role</element>
              <json_fragment>N</json_fragment>
              <is_without_enclosing>N</is_without_enclosing>
              <remove_if_blank>Y</remove_if_blank>
            </field>
          </fields>
        </transform>
        """;
    JsonEOutputMeta meta = new JsonEOutputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        JsonEOutputMeta.class,
        meta,
        new MemoryMetadataProvider());
    assertTrue(isEmpty(meta.getJsonBloc()));
  }

  @Test
  void testLoadSave() throws Exception {
    Path path =
        Paths.get(
            Objects.requireNonNull(getClass().getResource("/json-enhanced-output.xml")).toURI());
    String xml = Files.readString(path);
    JsonEOutputMeta meta = new JsonEOutputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        JsonEOutputMeta.class,
        meta,
        new MemoryMetadataProvider());

    validate(meta);

    // Do a round trip:
    //
    String xmlCopy =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    JsonEOutputMeta metaCopy = new JsonEOutputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xmlCopy, TransformMeta.XML_TAG),
        JsonEOutputMeta.class,
        metaCopy,
        new MemoryMetadataProvider());
    validate(metaCopy);
  }

  private static void validate(JsonEOutputMeta meta) {
    assertFalse(meta.isAddingToResult());
    assertTrue(meta.getFileSettings().isCreateParentFolder());
    assertFalse(meta.getFileSettings().isDateInFileName());
    assertTrue(meta.getFileSettings().isDoNotOpenNewFileInit());
    assertEquals(Const.UTF_8, meta.getEncoding());
    assertEquals("json", meta.getFileSettings().getExtension());
    assertFalse(meta.getFileSettings().isFileAppended());
    assertEquals("filename", meta.getFileSettings().getFileName());
    assertEquals("blockName", meta.getJsonBloc());
    assertEquals(100, meta.getFileSettings().splitOutputAfter);
    assertEquals(JsonEOutputMeta.OperationType.WRITE_TO_FILE, meta.getOperationType());
    assertEquals("outputValue", meta.getOutputValue());
    assertFalse(meta.getFileSettings().isTimeInFileName());

    assertNotNull(meta.getOutputFields());
    assertEquals(3, meta.getOutputFields().size());
    JsonEOutputField f1 = meta.getOutputFields().getFirst();
    assertEquals("f1", f1.getFieldName());
    assertEquals("element1", f1.getElementName());

    JsonEOutputField f2 = meta.getOutputFields().get(1);
    assertEquals("f2", f2.getFieldName());
    assertEquals("element2", f2.getElementName());

    JsonEOutputField f3 = meta.getOutputFields().get(2);
    assertEquals("f3", f3.getFieldName());
    assertEquals("element3", f3.getElementName());
  }
}
