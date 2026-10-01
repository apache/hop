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

package org.apache.hop.beam.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.apache.beam.sdk.io.Compression;
import org.apache.hop.beam.metadata.FileDefinition;
import org.apache.hop.beam.transforms.io.BeamOutputMeta;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Issue #2337: the Beam file output transform must be able to write compressed files.
 *
 * <p>End to end on the Direct runner: run a real pipeline and check the bytes on disk, rather than
 * only asserting that a property round-trips through the transform XML.
 */
class BeamOutputCompressionPipelineTest extends SingleTransformPipelineTestBase {

  private static final int EXPECTED_ROWS = 100;

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  /** Beam Input -> Dummy -> Beam Output, with the output transform configured for compression. */
  private PipelineMeta pipelineWithCompression(String compression, String suffix) throws Exception {
    FileDefinition fileDefinition =
        org.apache.hop.beam.util.BeamPipelineMetaUtil.createCustomersInputFileDefinition();
    fileDefinition.setName("CustomersCompressed");
    metadataProvider.getSerializer(FileDefinition.class).save(fileDefinition);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("compressed-output");
    pipelineMeta.setMetadataProvider(metadataProvider);

    org.apache.hop.beam.transforms.io.BeamInputMeta inputMeta =
        new org.apache.hop.beam.transforms.io.BeamInputMeta();
    inputMeta.setInputLocation(INPUT_CUSTOMERS_FILE);
    inputMeta.setFileDefinitionName(fileDefinition.getName());
    TransformMeta inputTransformMeta = new TransformMeta("INPUT", inputMeta);
    inputTransformMeta.setTransformPluginId("BeamInput");
    pipelineMeta.addTransform(inputTransformMeta);

    TransformMeta dummyTransformMeta = new TransformMeta("Dummy", new DummyMeta());
    pipelineMeta.addTransform(dummyTransformMeta);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(inputTransformMeta, dummyTransformMeta));

    BeamOutputMeta outputMeta = new BeamOutputMeta();
    outputMeta.setOutputLocation(OUTPUT_FOLDER);
    outputMeta.setFileDefinitionName(null);
    outputMeta.setFilePrefix("compressed");
    outputMeta.setFileSuffix(suffix);
    outputMeta.setWindowed(false);
    outputMeta.setCompression(compression);
    TransformMeta outputTransformMeta = new TransformMeta("OUTPUT", outputMeta);
    outputTransformMeta.setTransformPluginId("BeamOutput");
    pipelineMeta.addTransform(outputTransformMeta);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(dummyTransformMeta, outputTransformMeta));

    return pipelineMeta;
  }

  @Test
  void compressionDefaultsToOff() {
    // A transform saved before #2337 has no compression element, so the default keeps the
    // previous behaviour: plain text.
    assertEquals(null, new BeamOutputMeta().getCompression());
  }

  @Test
  void theSupportedCompressionNamesAreRealBeamConstants() {
    // The dialog offers these, so they have to be genuine Beam Compression values.
    for (String name :
        List.of(
            "AUTO",
            "UNCOMPRESSED",
            "GZIP",
            "BZIP2",
            "ZIP",
            "ZSTD",
            "LZO",
            "LZOP",
            "DEFLATE",
            "SNAPPY")) {
      assertEquals(name, Compression.valueOf(name).name());
    }
  }

  @Test
  void anUnknownCompressionNameFailsWithAClearError() throws Exception {
    PipelineMeta pipelineMeta = pipelineWithCompression("NOT-A-CODEC", ".txt");

    HopException exception =
        assertThrows(HopException.class, () -> runAndGetOutputFiles(pipelineMeta));

    // The engine wraps a failure in several layers, so check the whole cause chain rather than
    // only the outermost message.
    assertTrue(
        causeChainContains(exception, "NOT-A-CODEC"),
        "the error should name the bad value, chain was: " + causeChain(exception));
  }

  private static String causeChain(Throwable throwable) {
    StringBuilder builder = new StringBuilder();
    for (Throwable t = throwable; t != null; t = t.getCause()) {
      builder.append(t.getMessage()).append(" | ");
    }
    return builder.toString();
  }

  private static boolean causeChainContains(Throwable throwable, String needle) {
    for (Throwable t = throwable; t != null; t = t.getCause()) {
      if (t.getMessage() != null && t.getMessage().contains(needle)) {
        return true;
      }
    }
    return false;
  }

  @Test
  void gzipOutputIsActuallyGzippedAndStillReadable() throws Exception {
    PipelineMeta pipelineMeta = pipelineWithCompression("GZIP", ".gz");

    Map<String, byte[]> bytesByFile = runAndGetOutputBytesByFile(pipelineMeta);

    int totalRows = 0;
    for (Map.Entry<String, byte[]> entry : bytesByFile.entrySet()) {
      byte[] bytes = entry.getValue();
      assertTrue(bytes.length > 0, "empty shard " + entry.getKey());

      // The whole point of the feature: the bytes on disk are a gzip member.
      assertEquals((byte) 0x1f, bytes[0], "missing gzip magic in " + entry.getKey());
      assertEquals((byte) 0x8b, bytes[1], "missing gzip magic in " + entry.getKey());

      // And the data still round-trips: every row is recoverable, which is what actually matters.
      String text = gunzip(bytes);
      for (String line : text.split("\n")) {
        if (!line.isBlank()) {
          totalRows++;
          assertEquals(
              10,
              line.split(",", -1).length,
              "every row should still carry all 10 fields after a gzip round trip, got: " + line);
        }
      }
    }

    assertEquals(EXPECTED_ROWS, totalRows, "expected all 100 rows to survive compression");
  }

  @Test
  void uncompressedOutputIsNotGzipped() throws Exception {
    PipelineMeta pipelineMeta = pipelineWithCompression("UNCOMPRESSED", ".txt");

    Map<String, byte[]> bytesByFile = runAndGetOutputBytesByFile(pipelineMeta);

    for (Map.Entry<String, byte[]> entry : bytesByFile.entrySet()) {
      byte[] bytes = entry.getValue();
      assertFalse(
          bytes.length >= 2 && bytes[0] == (byte) 0x1f && bytes[1] == (byte) 0x8b,
          "UNCOMPRESSED output should not be gzipped: " + entry.getKey());
    }
  }

  @Test
  void autoCompressionFollowsTheFileSuffix() throws Exception {
    // AUTO derives the codec from the suffix, so a .gz suffix is enough.
    PipelineMeta pipelineMeta = pipelineWithCompression("AUTO", ".gz");

    Map<String, byte[]> bytesByFile = runAndGetOutputBytesByFile(pipelineMeta);

    for (Map.Entry<String, byte[]> entry : bytesByFile.entrySet()) {
      assertEquals(
          (byte) 0x1f,
          entry.getValue()[0],
          "AUTO with a .gz suffix should produce gzipped output: " + entry.getKey());
    }
  }

  private static String gunzip(byte[] bytes) throws IOException {
    try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
