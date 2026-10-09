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

package org.apache.hop.beam.core.fn;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class BeamMqttConversionTest {
  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void utf8PayloadBecomesHopStringWithRealBeamMetrics() {
    Pipeline p = Pipeline.create();
    PAssert.that(
            p.apply(Create.of("héllo 😀".getBytes(StandardCharsets.UTF_8)))
                .apply(ParDo.of(new MqttToHopFn("mqtt-input", "String"))))
        .satisfies(
            rows -> {
              int count = 0;
              for (HopRow row : rows) {
                assertArrayEquals(new Object[] {"héllo 😀"}, row.getRow());
                count++;
              }
              assertEquals(1, count);
              return null;
            });
    var result = p.run();
    result.waitUntilFinish();
    var metrics =
        result
            .metrics()
            .queryMetrics(
                org.apache.beam.sdk.metrics.MetricsFilter.builder()
                    .addNameFilter(
                        org.apache.beam.sdk.metrics.MetricNameFilter.named(
                            org.apache.hop.pipeline.Pipeline.METRIC_NAME_INPUT, "mqtt-input"))
                    .build());
    assertTrue(metrics.getCounters().iterator().hasNext());
    assertEquals(1L, metrics.getCounters().iterator().next().getAttempted());
  }

  @Test
  void arbitraryBinaryPayloadSurvivesCoderRoundTrip() throws Exception {
    Pipeline p = Pipeline.create();
    byte[] bytes = {0, -1, 12, 42};
    var output =
        p.apply(Create.of(bytes))
            .apply(ParDo.of(new MqttToHopFn("binary-input", "Binary")))
            .setCoder(new HopRowCoder());
    PAssert.that(output)
        .satisfies(
            rows -> {
              for (HopRow row : rows) assertArrayEquals(bytes, (byte[]) row.getRow()[0]);
              return null;
            });
    p.run().waitUntilFinish();
  }

  @Test
  void hopStringConversionUsesValueMetadataAndBinaryRemainsExact() throws Exception {
    RowMeta meta = new RowMeta();
    meta.addValueMeta(new ValueMetaInteger("number"));
    meta.addValueMeta(new ValueMetaBinary("bytes"));
    Pipeline p = Pipeline.create();
    var rows =
        p.apply(
            Create.of(new HopRow(new Object[] {42L, new byte[] {0, -1, 9}}))
                .withCoder(new HopRowCoder()));
    PAssert.that(
            rows.apply(
                "string",
                ParDo.of(new HopToMqttFn("write-string", "number", "String", meta.getMetaXml()))))
        .satisfies(
            values -> {
              for (byte[] bytes : values)
                assertArrayEquals("42".getBytes(StandardCharsets.UTF_8), bytes);
              return null;
            });
    PAssert.that(
            rows.apply(
                "binary",
                ParDo.of(new HopToMqttFn("write-binary", "bytes", "Binary", meta.getMetaXml()))))
        .satisfies(
            values -> {
              for (byte[] bytes : values) assertArrayEquals(new byte[] {0, -1, 9}, bytes);
              return null;
            });
    p.run().waitUntilFinish();
  }

  @Test
  void missingFieldFailsDuringSetupWithoutPayloadLogging() throws Exception {
    RowMeta meta = new RowMeta();
    meta.addValueMeta(new ValueMetaInteger("present"));
    HopToMqttFn fn = new HopToMqttFn("write", "missing", "String", meta.getMetaXml());
    var metrics = new org.apache.beam.runners.core.metrics.MetricsContainerImpl("missing-field");
    try (var ignored =
        org.apache.beam.sdk.metrics.MetricsEnvironment.scopedMetricsContainer(metrics)) {
      var error = assertThrows(org.apache.hop.core.exception.HopRuntimeException.class, fn::setup);
      assertTrue(error.getMessage().contains("missing"));
      assertEquals(
          1L,
          metrics
              .getCounter(
                  org.apache.beam.sdk.metrics.MetricName.named(
                      org.apache.hop.pipeline.Pipeline.METRIC_NAME_ERROR, "write"))
              .getCumulative());
    }
  }

  @Test
  void lazyAndIndexedStoragePublishesExactBytes() throws Exception {
    RowMeta meta = storageMeta();
    byte[] latin1 = "caf\u00e9".getBytes(StandardCharsets.ISO_8859_1);
    Pipeline p = Pipeline.create();
    PAssert.that(
            p.apply(Create.of(new HopRow(new Object[] {latin1, 1, 1})).withCoder(new HopRowCoder()))
                .apply(
                    "lazy", ParDo.of(new HopToMqttFn("lazy", "lazy", "String", meta.getMetaXml()))))
        .satisfies(
            values -> {
              for (byte[] bytes : values)
                assertArrayEquals("caf\u00e9".getBytes(StandardCharsets.UTF_8), bytes);
              return null;
            });
    PAssert.that(
            p.apply(Create.of(new HopRow(new Object[] {latin1, 1, 1})).withCoder(new HopRowCoder()))
                .apply(
                    "indexed-string",
                    ParDo.of(
                        new HopToMqttFn("indexed-string", "indexed", "String", meta.getMetaXml()))))
        .satisfies(
            values -> {
              for (byte[] bytes : values)
                assertArrayEquals("bravo".getBytes(StandardCharsets.UTF_8), bytes);
              return null;
            });
    PAssert.that(
            p.apply(Create.of(new HopRow(new Object[] {latin1, 1, 1})).withCoder(new HopRowCoder()))
                .apply(
                    "indexed-binary",
                    ParDo.of(
                        new HopToMqttFn("indexed-binary", "bytes", "Binary", meta.getMetaXml()))))
        .satisfies(
            values -> {
              for (byte[] bytes : values) assertArrayEquals(new byte[] {9, -1}, bytes);
              return null;
            });
    p.run().waitUntilFinish();
  }

  private static RowMeta storageMeta() {
    RowMeta meta = new RowMeta();
    ValueMetaString lazy = new ValueMetaString("lazy");
    lazy.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    ValueMetaString storage = new ValueMetaString("lazy");
    storage.setStringEncoding(StandardCharsets.ISO_8859_1.name());
    lazy.setStorageMetadata(storage);
    meta.addValueMeta(lazy);
    ValueMetaString indexed = new ValueMetaString("indexed");
    indexed.setStorageType(IValueMeta.STORAGE_TYPE_INDEXED);
    indexed.setIndex(new Object[] {"alpha", "bravo"});
    meta.addValueMeta(indexed);
    ValueMetaBinary binary = new ValueMetaBinary("bytes");
    binary.setStorageType(IValueMeta.STORAGE_TYPE_INDEXED);
    binary.setIndex(new Object[] {new byte[] {1}, new byte[] {9, -1}});
    meta.addValueMeta(binary);
    return meta;
  }

  @Test
  void nullPayloadFailsRatherThanBeingDropped() throws Exception {
    RowMeta meta = new RowMeta();
    meta.addValueMeta(new ValueMetaBinary("bytes"));
    Pipeline p = Pipeline.create();
    p.apply(Create.of(new HopRow(new Object[] {null})).withCoder(new HopRowCoder()))
        .apply(ParDo.of(new HopToMqttFn("write", "bytes", "Binary", meta.getMetaXml())));
    assertThrows(RuntimeException.class, () -> p.run().waitUntilFinish());
  }
}
