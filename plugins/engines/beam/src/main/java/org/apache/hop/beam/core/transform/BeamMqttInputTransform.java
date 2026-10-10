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
import org.apache.beam.sdk.io.mqtt.MqttIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.beam.core.fn.MqttToHopFn;
import org.joda.time.Duration;

@RequiredArgsConstructor
public class BeamMqttInputTransform extends PTransform<PBegin, PCollection<HopRow>> {
  private final String transformName;
  private final BeamMqttConnection connection;
  private final String payloadType;
  private final long maxNumRecords;
  private final long maxReadTime;

  public MqttIO.Read<byte[]> getRead() {
    var read = MqttIO.read().withConnectionConfiguration(connection.createConfiguration());
    if (maxNumRecords > 0) read = read.withMaxNumRecords(maxNumRecords);
    if (maxReadTime > 0) read = read.withMaxReadTime(Duration.standardSeconds(maxReadTime));
    return read;
  }

  @Override
  public PCollection<HopRow> expand(PBegin input) {
    return input
        .apply("Read MQTT", getRead())
        .apply("Convert MQTT payload", ParDo.of(new MqttToHopFn(transformName, payloadType)))
        .setCoder(new HopRowCoder());
  }
}
