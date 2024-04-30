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

package org.apache.hop.pipeline.transforms.eventhubs.listen;

import java.util.List;
import junit.framework.TestCase;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.Test;

public class AzureListenerMetaTest extends TestCase {

  @Test
  public void testSerialization() throws Exception {
    HopClientEnvironment.init(List.of(TwoWayPasswordEncoderPluginType.getInstance()));
    AzureListenerMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/azure-listener-transform.xml", AzureListenerMeta.class);

    assertEquals("namespace", meta.getNamespace());
    assertEquals("instance", meta.getEventHubName());
    assertEquals("key-name", meta.getSasKeyName());
    assertEquals("key-value", meta.getSasKey());
    assertEquals("123", meta.getBatchSize());
    assertEquals("234", meta.getPrefetchSize());
    assertNotNull("message", meta.getOutputField());
    assertNotNull("partitionId", meta.getPartitionIdField());
    assertNotNull("offset", meta.getOffsetField());
    assertNotNull("sequenceNumber", meta.getSequenceNumberField());
    assertNotNull("host", meta.getHostField());
    assertNotNull("enqueuedTime", meta.getEnqueuedTimeField());
    assertNotNull("$Default", meta.getConsumerGroupName());
    assertNotNull("connection-string", meta.getStorageConnectionString());
    assertNotNull("container-name", meta.getStorageContainerName());
    assertNotNull("pipeline.hpl", meta.getBatchPipeline());
    assertNotNull("input", meta.getBatchInputTransform());
    assertNotNull("output", meta.getBatchOutputTransform());
    assertNotNull("5000", meta.getBatchMaxWaitTime());
  }
}
