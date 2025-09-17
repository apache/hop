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

package org.apache.hop.pipeline.transforms.eventhubs.write;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.Test;

class AzureWriterMetaTest {
  @Test
  void testSerialization() throws Exception {
    HopClientEnvironment.init(List.of(TwoWayPasswordEncoderPluginType.getInstance()));
    AzureWriterMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/azure-writer-transform.xml", AzureWriterMeta.class);
    assertEquals("namespace", meta.getNamespace());
    assertEquals("instance", meta.getEventHubName());
    assertEquals("key-name", meta.getSasKeyName());
    assertEquals("key-string", meta.getSasKey());
    assertEquals("123", meta.getBatchSize());
    assertNotNull("message", meta.getMessageField());
  }
}
