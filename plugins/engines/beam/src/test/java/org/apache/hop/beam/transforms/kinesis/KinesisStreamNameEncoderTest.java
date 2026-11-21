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

package org.apache.hop.beam.transforms.kinesis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

/**
 * The Kinesis stream name is no longer flagged as a password, so the linter stops reporting it as a
 * hardcoded secret, but it is stored exactly as before.
 *
 * @see <a href="https://github.com/apache/hop/issues/9500">#9500</a>
 */
class KinesisStreamNameEncoderTest {

  /** How the stream name was declared before #9500. */
  public static class PasswordStreamName {
    @HopMetadataProperty(key = "stream_name", password = true)
    private String streamName;

    public String getStreamName() {
      return streamName;
    }

    public void setStreamName(String streamName) {
      this.streamName = streamName;
    }
  }

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  private static String streamNameElement(String xml) {
    int start = xml.indexOf("<stream_name>");
    int end = xml.indexOf("</stream_name>") + "</stream_name>".length();
    return xml.substring(start, end);
  }

  private static Node transformNode(String xml) throws Exception {
    return XmlHandler.getSubNode(
        XmlHandler.loadXmlString("<transform>" + xml + "</transform>"), "transform");
  }

  private static String passwordXml(String streamName) throws Exception {
    PasswordStreamName before = new PasswordStreamName();
    before.setStreamName(streamName);
    return XmlMetadataUtil.serializeObjectToXml(before);
  }

  @Test
  void producerStoresTheStreamNameAsBefore() throws Exception {
    for (String streamName : new String[] {"orders", "${KINESIS_STREAM}"}) {
      BeamKinesisProduceMeta meta = new BeamKinesisProduceMeta();
      meta.setStreamName(streamName);
      String xml = XmlMetadataUtil.serializeObjectToXml(meta);

      assertEquals(streamNameElement(passwordXml(streamName)), streamNameElement(xml));

      BeamKinesisProduceMeta read =
          XmlMetadataUtil.deSerializeFromXml(
              transformNode(passwordXml(streamName)),
              BeamKinesisProduceMeta.class,
              new MemoryMetadataProvider());
      assertEquals(streamName, read.getStreamName());
    }
    assertTrue(streamNameElement(passwordXml("orders")).contains("Encrypted "));
  }

  @Test
  void consumerStoresTheStreamNameAsBefore() throws Exception {
    BeamKinesisConsumeMeta meta = new BeamKinesisConsumeMeta();
    meta.setStreamName("orders");
    String xml = XmlMetadataUtil.serializeObjectToXml(meta);

    assertEquals(streamNameElement(passwordXml("orders")), streamNameElement(xml));

    BeamKinesisConsumeMeta read =
        XmlMetadataUtil.deSerializeFromXml(
            transformNode(passwordXml("orders")),
            BeamKinesisConsumeMeta.class,
            new MemoryMetadataProvider());
    assertEquals("orders", read.getStreamName());
  }
}
