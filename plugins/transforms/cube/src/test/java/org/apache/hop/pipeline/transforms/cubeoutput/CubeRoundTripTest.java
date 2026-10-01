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
package org.apache.hop.pipeline.transforms.cubeoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.cubeinput.CubeInputMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Runs a real Cube Input → Cube Output pipeline. */
class CubeRoundTripTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
  }

  /**
   * An empty cube still has a layout. Reading it and writing it back (here to another file, but
   * refreshing the same file in place is the case that used to fail) must produce a cube with that
   * same layout and no rows, without the output transform having to re-open the input file at the
   * end of the run to find out what the layout was.
   */
  @Test
  void emptyCubeKeepsItsLayoutThroughAPipeline() throws Exception {
    IRowMeta layout = new RowMeta();
    layout.addValueMeta(new ValueMetaString("name"));
    layout.addValueMeta(new ValueMetaInteger("id"));
    Path source = tempDir.resolve("source.cube");
    writeEmptyCube(source, layout);
    Path target = tempDir.resolve("target.cube");

    CubeInputMeta inputMeta = new CubeInputMeta();
    inputMeta.setDefault();
    inputMeta.getFile().setName(source.toString());
    CubeOutputMeta outputMeta = new CubeOutputMeta();
    outputMeta.setDefault();
    outputMeta.setFilename(target.toString());

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("cube round trip");
    TransformMeta input = new TransformMeta("Cube input", inputMeta);
    TransformMeta output = new TransformMeta("Cube output", outputMeta);
    pipelineMeta.addTransform(input);
    pipelineMeta.addTransform(output);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(input, output));

    // Truncate the source before the output transform gets a chance to ask for its layout again:
    // this is what an in-place refresh (same file for input and output) does in init().
    //
    LocalPipelineEngine pipeline =
        new LocalPipelineEngine(pipelineMeta, new Variables(), new LoggingObject("test"));
    pipeline.prepareExecution();
    Files.write(source, new byte[0]);
    pipeline.startThreads();
    pipeline.waitUntilFinished();

    assertEquals(0, pipeline.getErrors(), "the run reported errors");
    IRowMeta written = readCubeLayout(target);
    assertEquals(layout.getFieldNames().length, written.size());
    assertEquals("name", written.getValueMeta(0).getName());
    assertEquals("id", written.getValueMeta(1).getName());
  }

  private static void writeEmptyCube(Path file, IRowMeta layout) throws Exception {
    try (OutputStream os = Files.newOutputStream(file);
        DataOutputStream dos = new DataOutputStream(new GZIPOutputStream(os))) {
      layout.writeMeta(dos);
    }
  }

  private static IRowMeta readCubeLayout(Path file) throws Exception {
    try (InputStream is = Files.newInputStream(file);
        DataInputStream dis = new DataInputStream(new GZIPInputStream(is))) {
      return new RowMeta(dis);
    }
  }
}
