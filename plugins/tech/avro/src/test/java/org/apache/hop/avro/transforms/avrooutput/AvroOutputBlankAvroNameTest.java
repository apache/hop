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

package org.apache.hop.avro.transforms.avrooutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import org.apache.avro.Schema;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;

/**
 * An output field with a blank Avro path must be written under its stream field name. Up to 2.17 a
 * saved {@code <avroname/>} loaded as null; since 2.18 it loads as an empty string, which must not
 * be used as the (empty) Avro field name.
 */
class AvroOutputBlankAvroNameTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<AvroOutputMeta, AvroOutputData> mockHelper;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>("AvroOutput", AvroOutputMeta.class, AvroOutputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void blankAvroNameFallsBackToStreamFieldName() throws Exception {
    AvroOutputMeta meta = loadMeta("<avroname/>");

    assertEquals("id", meta.getOutputFields().get(0).getAvroName());

    Schema schema = createSchema(meta);
    assertEquals(1, schema.getFields().size());
    assertNotNull(schema.getField("id"), "Generated schema: " + schema);
  }

  @Test
  void specifiedAvroNameIsUsed() throws Exception {
    AvroOutputMeta meta = loadMeta("<avroname>customer_id</avroname>");

    assertEquals("customer_id", meta.getOutputFields().get(0).getAvroName());

    Schema schema = createSchema(meta);
    assertEquals(1, schema.getFields().size());
    assertNotNull(schema.getField("customer_id"), "Generated schema: " + schema);
  }

  private Schema createSchema(AvroOutputMeta meta) throws HopException {
    AvroOutput transform =
        new AvroOutput(
            mockHelper.transformMeta,
            meta,
            new AvroOutputData(),
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    return transform.createAvroSchema(new ArrayList<>(meta.getOutputFields()), "");
  }

  private static AvroOutputMeta loadMeta(String avroNameTag) throws Exception {
    String xml =
        "<transform>"
            + "<recordname>customer</recordname>"
            + "<namespace>org.apache.hop.test</namespace>"
            + "<doc>test</doc>"
            + "<fields><field>"
            + "<name>id</name>"
            + avroNameTag
            + "<avrotype>5</avrotype>"
            + "<nullable>N</nullable>"
            + "</field></fields></transform>";
    Node transformNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "transform");
    AvroOutputMeta meta = new AvroOutputMeta();
    meta.loadXml(transformNode, new MemoryMetadataProvider());
    return meta;
  }
}
