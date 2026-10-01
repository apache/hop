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

package org.apache.hop.avro.transforms.avrooutput;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

/**
 * Issue #3861: an Avro File Output whose output type was never set took the whole pipeline down
 * with a NullPointerException while the pipeline was being prepared, which on a Hop server left the
 * pipeline stuck in the server's object list.
 */
class AvroOutputMetaOutputTypeTest {

  /**
   * setDefault() is only called by the GUI when a transform is dropped on the canvas, so a file
   * that carries no output_type tag has to fall back to the default on its own.
   */
  @Test
  void outputTypeDefaultsWithoutSetDefault() {
    assertEquals(
        AvroOutputMeta.OUTPUT_TYPES[AvroOutputMeta.OUTPUT_TYPE_BINARY_FILE],
        new AvroOutputMeta().getOutputType());
  }

  @Test
  void outputTypeSurvivesLoadingATransformNodeWithoutTheTag() throws Exception {
    AvroOutputMeta meta = new AvroOutputMeta();
    Node node =
        XmlHandler.getSubNode(
            XmlHandler.loadXmlString(
                "<transform><name>avro</name><type>AvroOutput</type></transform>"),
            "transform");
    meta.loadXml(node, new MemoryMetadataProvider());

    assertEquals(
        AvroOutputMeta.OUTPUT_TYPES[AvroOutputMeta.OUTPUT_TYPE_BINARY_FILE], meta.getOutputType());
    assertDoesNotThrow(() -> getFields(meta));
  }

  /** An unselected combo hands the dialog -1; that must not null the output type out. */
  @Test
  void unknownOutputTypeIdFallsBackToTheDefault() {
    AvroOutputMeta meta = new AvroOutputMeta();
    meta.setOutputTypeById(-1);

    assertEquals(
        AvroOutputMeta.OUTPUT_TYPES[AvroOutputMeta.OUTPUT_TYPE_BINARY_FILE], meta.getOutputType());
    assertEquals(AvroOutputMeta.OUTPUT_TYPE_BINARY_FILE, meta.getOutputTypeId());
    assertDoesNotThrow(() -> getFields(meta));
  }

  /** Even an explicitly nulled output type may not throw a raw NullPointerException. */
  @Test
  void nullOutputTypeDoesNotThrow() {
    AvroOutputMeta meta = new AvroOutputMeta();
    meta.setOutputType(null);

    assertDoesNotThrow(() -> getFields(meta));
  }

  /** The binary-field output type still contributes its field. */
  @Test
  void binaryFieldOutputTypeStillAddsTheField() throws Exception {
    AvroOutputMeta meta = new AvroOutputMeta();
    meta.setOutputTypeById(AvroOutputMeta.OUTPUT_TYPE_FIELD);
    meta.setOutputFieldName("avro_record");

    IRowMeta row = new RowMeta();
    meta.getFields(row, "avro", null, null, new Variables(), new MemoryMetadataProvider());

    assertEquals(1, row.size());
    assertEquals("avro_record", row.getValueMeta(0).getName());
  }

  private static void getFields(AvroOutputMeta meta) throws Exception {
    meta.getFields(
        new RowMeta(), "avro", null, null, new Variables(), new MemoryMetadataProvider());
  }
}
