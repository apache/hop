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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.apache.hop.resource.SimpleResourceNaming;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class CubeOutputMetaTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void testRoundTrip() throws Exception {
    CubeOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/serialize-transform.xml", CubeOutputMeta.class);

    assertNotNull(meta.getFilename());
    assertFalse(meta.isIncludeTransformNr());
  }

  @Test
  void includeTransformNrRoundTrips() throws Exception {
    CubeOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/serialize-with-copy-nr.xml", CubeOutputMeta.class);

    assertTrue(meta.isIncludeTransformNr());
    assertTrue(meta.isFilenameCreatingParentFolders());
  }

  @Test
  void exportResourcesKeepsANamePerCopy() throws Exception {
    writeCube(tempDir.resolve("export_0.cube"));
    Variables variables = new Variables();
    variables.setVariable("DIR", tempDir.toString());

    CubeOutputMeta meta = new CubeOutputMeta();
    meta.setDefault();
    meta.setFilename("${DIR}/export_${Internal.Transform.CopyNr}.cube");

    String exported = meta.exportResources(variables, null, new SimpleResourceNaming(), null);

    assertNotNull(exported);
    assertTrue(exported.contains("/export_${Internal.Transform.CopyNr}.cube"), exported);
    assertFalse(exported.contains("export_0"), exported);
    assertTrue(meta.getFilename().contains("Internal.Transform.CopyNr"), meta.getFilename());
    assertFalse(meta.isIncludeTransformNr());

    writeCube(tempDir.resolve("numbered_0.cube"));
    CubeOutputMeta numbered = new CubeOutputMeta();
    numbered.setDefault();
    numbered.setIncludeTransformNr(true);
    numbered.setFilename("${DIR}/numbered.cube");

    String numberedExport =
        numbered.exportResources(variables, null, new SimpleResourceNaming(), null);

    assertNotNull(numberedExport);
    assertTrue(numberedExport.contains("/numbered.cube"), numberedExport);
    assertFalse(numberedExport.contains("numbered_0"), numberedExport);
    assertTrue(numbered.isIncludeTransformNr());

    CubeOutputMeta missing = new CubeOutputMeta();
    missing.setDefault();
    missing.setIncludeTransformNr(true);
    missing.setFilename(tempDir.resolve("missing.cube").toString());

    assertNull(missing.exportResources(new Variables(), null, new SimpleResourceNaming(), null));
    assertTrue(missing.isIncludeTransformNr());
    assertTrue(missing.getFilename().endsWith("missing.cube"), missing.getFilename());
  }

  private static void writeCube(Path file) throws Exception {
    IRowMeta layout = new RowMeta();
    layout.addValueMeta(new ValueMetaString("name"));
    try (OutputStream os = Files.newOutputStream(file);
        DataOutputStream dos = new DataOutputStream(new GZIPOutputStream(os))) {
      layout.writeMeta(dos);
    }
  }
}
