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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class CubeOutputTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @TempDir Path tempDir;

  private TransformMockHelper<CubeOutputMeta, CubeOutputData> helper;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper = new TransformMockHelper<>("Cube output", CubeOutputMeta.class, CubeOutputData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  /**
   * When no rows arrive, the output layout is derived from the pipeline graph. That walk can fail
   * because an upstream input transform has to re-open its source to answer (a Cube Input whose
   * file is being rewritten in place by this very transform, for example). The run used to end in
   * an error with a cube file that had a GZIP header but no layout; now it writes a valid, empty
   * cube without fields.
   */
  @Test
  void emptyStreamWithUnknownLayoutWritesAnEmptyCube() throws Exception {
    Path cubeFile = tempDir.resolve("empty.cube");
    CubeOutput cubeOutput = createCubeOutput(cubeFile);
    when(helper.pipelineMeta.getPrevTransformFields(any(), any(TransformMeta.class)))
        .thenThrow(
            new HopTransformException("Error opening/reading cube file", new EOFException()));
    doReturn(null).when(cubeOutput).getRow();

    assertTrue(cubeOutput.init());
    assertFalse(cubeOutput.processRow());
    cubeOutput.dispose();

    assertEquals(0, cubeOutput.getErrors());
    assertEquals(0, readCubeLayout(cubeFile).size());
  }

  @Test
  void emptyStreamWithKnownLayoutWritesTheLayout() throws Exception {
    Path cubeFile = tempDir.resolve("empty.cube");
    CubeOutput cubeOutput = createCubeOutput(cubeFile);
    IRowMeta layout = new RowMeta();
    layout.addValueMeta(new org.apache.hop.core.row.value.ValueMetaString("name"));
    when(helper.pipelineMeta.getPrevTransformFields(any(), any(TransformMeta.class)))
        .thenReturn(layout);
    doReturn(null).when(cubeOutput).getRow();

    assertTrue(cubeOutput.init());
    assertFalse(cubeOutput.processRow());
    cubeOutput.dispose();

    assertEquals(0, cubeOutput.getErrors());
    IRowMeta written = readCubeLayout(cubeFile);
    assertEquals(1, written.size());
    assertEquals("name", written.getValueMeta(0).getName());
  }

  private CubeOutput createCubeOutput(Path cubeFile) {
    CubeOutputMeta meta = new CubeOutputMeta();
    meta.setDefault();
    meta.setFilename(cubeFile.toString());
    when(helper.transformMeta.getTransform()).thenReturn(meta);
    CubeOutput cubeOutput =
        new CubeOutput(
            helper.transformMeta,
            meta,
            new CubeOutputData(),
            0,
            helper.pipelineMeta,
            helper.pipeline);
    return spy(cubeOutput);
  }

  /** Reads the file the way Cube Input does: header, then rows until the end-of-file marker. */
  private static IRowMeta readCubeLayout(Path cubeFile) throws Exception {
    try (InputStream is = Files.newInputStream(cubeFile);
        DataInputStream dis = new DataInputStream(new GZIPInputStream(is))) {
      IRowMeta rowMeta = new RowMeta(dis);
      assertThrows(HopEofException.class, () -> rowMeta.readData(dis));
      return rowMeta;
    }
  }
}
