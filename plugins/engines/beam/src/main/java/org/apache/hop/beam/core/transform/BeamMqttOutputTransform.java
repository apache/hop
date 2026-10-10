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

package org.apache.hop.beam.core.transform;

import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.io.mqtt.MqttIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.HopToMqttFn;

@RequiredArgsConstructor
public class BeamMqttOutputTransform extends PTransform<PCollection<HopRow>, PDone> {
  private final String transformName;
  private final BeamMqttConnection connection;
  private final String payloadField;
  private final String payloadType;
  private final String rowMetaXml;
  private final boolean retained;

  public MqttIO.Write<byte[]> getWrite() {
    return MqttIO.write()
        .withConnectionConfiguration(connection.createConfiguration())
        .withRetained(retained);
  }

  @Override
  public PDone expand(PCollection<HopRow> input) {
    return input
        .apply(
            "Convert Hop payload",
            ParDo.of(new HopToMqttFn(transformName, payloadField, payloadType, rowMetaXml)))
        .setCoder(ByteArrayCoder.of())
        .apply("Write MQTT", getWrite());
  }
}
