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

package org.apache.hop.pipeline.transforms.cubeinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopTransformException;
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

class CubeInputMetaTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void testRoundTrip() throws Exception {
    CubeInputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/de-serialize-transform.xml", CubeInputMeta.class);
    assertNotNull(meta.getFile());
    assertNotNull(meta.getFile().getName());
    assertFalse(meta.isIncludeTransformNr());
    assertFalse(meta.isFilenameInField());
  }

  @Test
  void filenameFromAFieldRoundTrips() throws Exception {
    CubeInputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/de-serialize-from-field.xml", CubeInputMeta.class);

    assertTrue(meta.isFilenameInField());
    assertFalse(meta.isIncludeTransformNr());
    assertEquals("filename", meta.getFilenameField());
    assertEquals("5", meta.getRowLimit());
    assertTrue(meta.isAddFilenameResult());
    assertTrue(meta.consumesMainInput());
    assertFalse(meta.canStartWithoutInput());
  }

  /**
   * Issue #3861: with no cube file configured, getFields() dereferenced the file name and threw a
   * raw NullPointerException out of prepareExecution instead of reporting the misconfiguration.
   */
  @Test
  void getFieldsWithoutFilenameReportsTheMisconfiguration() {
    CubeInputMeta meta = new CubeInputMeta();

    HopTransformException e =
        assertThrows(
            HopTransformException.class,
            () -> meta.getFields(new RowMeta(), "cube input", null, null, new Variables(), null));
    assertTrue(e.getMessage().contains("cube file name"), e.getMessage());
  }

  /**
   * The layout of a cube lives in the file, so getFields() has to open it. That also happens at
   * runtime, when a downstream transform that received no rows falls back to the design-time
   * layout: if the file is empty or half-written by then (a cube refreshed in place, a concurrent
   * writer), we fail with a message that names the file instead of a bare EOFException.
   */
  @Test
  void getFieldsOnTruncatedCubeFileReportsTheFile() throws Exception {
    Path cubeFile = Files.createFile(tempDir.resolve("truncated.cube"));
    CubeInputMeta meta = new CubeInputMeta();
    meta.setDefault();
    meta.getFile().setName(cubeFile.toString());

    HopTransformException e =
        assertThrows(
            HopTransformException.class,
            () -> meta.getFields(new RowMeta(), "cube input", null, null, new Variables(), null));
    assertTrue(e.getMessage().contains(cubeFile.toString()), e.getMessage());
    assertTrue(e.getCause() instanceof EOFException, String.valueOf(e.getCause()));
  }

  @Test
  void getFieldsWithoutASampleNameStillReportsTheMisconfigurationWhenReadingFromAField() {
    CubeInputMeta meta = new CubeInputMeta();
    meta.setFilenameInField(true);
    meta.setFilenameField("filename");

    HopTransformException e =
        assertThrows(
            HopTransformException.class,
            () -> meta.getFields(new RowMeta(), "cube input", null, null, new Variables(), null));
    assertTrue(e.getMessage().contains("cube file name"), e.getMessage());
  }

  @Test
  void getFieldsOpensCopyZeroWhenTheCopyVariableIsMissingOrStale() throws Exception {
    writeCube(tempDir.resolve("direct_0.cube"));
    writeCube(tempDir.resolve("case_0.cube"));
    writeCube(tempDir.resolve("win_0.cube"));
    writeCube(tempDir.resolve("nested_0.cube"));
    writeCube(tempDir.resolve("stale_0.cube"));
    writeCube(tempDir.resolve("data_0.cube"));

    Variables variables = new Variables();
    variables.setVariable(Const.INTERNAL_VARIABLE_TRANSFORM_COPYNR, "9");
    variables.setVariable(
        "FILE", tempDir.resolve("nested_${Internal.Transform.CopyNr}.cube").toString());

    assertField(
        readFields(
            tempDir.resolve("direct_${Internal.Transform.CopyNr}.cube").toString(), variables));
    assertField(
        readFields(
            tempDir.resolve("case_${internal.transform.copynr}.cube").toString(), variables));
    assertField(
        readFields(tempDir.toString() + "/win_%%Internal.Transform.CopyNr%%.cube", variables));
    assertField(readFields("${FILE}", variables));
    assertField(
        readFields(
            tempDir.resolve("stale_${Internal.Transform.CopyNr}.cube").toString(), variables));

    CubeInputMeta numbered = new CubeInputMeta();
    numbered.setDefault();
    numbered.setIncludeTransformNr(true);
    numbered.getFile().setName(tempDir.resolve("data.cube").toString());
    IRowMeta fields = new RowMeta();
    numbered.getFields(fields, "cube input", null, null, variables, null);
    assertField(fields);
  }

  @Test
  void getFieldsNamesTheResolvedCopyZeroFileWhenItIsMissing() {
    CubeInputMeta meta = new CubeInputMeta();
    meta.setDefault();
    meta.getFile().setName(tempDir.resolve("missing_${Internal.Transform.CopyNr}.cube").toString());

    HopTransformException e =
        assertThrows(
            HopTransformException.class,
            () -> meta.getFields(new RowMeta(), "cube input", null, null, new Variables(), null));
    assertTrue(e.getMessage().contains("missing_0.cube"), e.getMessage());
    assertFalse(e.getMessage().contains("${"), e.getMessage());
  }

  @Test
  void exportResourcesKeepsANamePerCopy() throws Exception {
    writeCube(tempDir.resolve("export_0.cube"));
    Variables variables = new Variables();
    variables.setVariable("DIR", tempDir.toString());
    CubeInputMeta meta = new CubeInputMeta();
    meta.setDefault();
    meta.getFile().setName("${DIR}/export_${Internal.Transform.CopyNr}.cube");

    String exported = meta.exportResources(variables, null, new SimpleResourceNaming(), null);

    assertNotNull(exported);
    assertTrue(exported.contains("/export_${Internal.Transform.CopyNr}.cube"), exported);
    assertFalse(exported.contains("export_0"), exported);
    assertTrue(meta.getFilename().contains("Internal.Transform.CopyNr"), meta.getFilename());
    assertFalse(meta.isIncludeTransformNr());

    writeCube(tempDir.resolve("numbered_0.cube"));
    CubeInputMeta numbered = new CubeInputMeta();
    numbered.setDefault();
    numbered.setIncludeTransformNr(true);
    numbered.getFile().setName("${DIR}/numbered.cube");

    String numberedExport =
        numbered.exportResources(variables, null, new SimpleResourceNaming(), null);

    assertNotNull(numberedExport);
    assertTrue(numberedExport.contains("/numbered.cube"), numberedExport);
    assertFalse(numberedExport.contains("numbered_0"), numberedExport);
    assertTrue(numbered.isIncludeTransformNr());

    writeCube(tempDir.resolve("plain.cube"));
    CubeInputMeta plain = new CubeInputMeta();
    plain.setDefault();
    plain.getFile().setName(tempDir.resolve("plain.cube").toString());

    String plainExport =
        plain.exportResources(new Variables(), null, new SimpleResourceNaming(), null);

    assertNotNull(plainExport);
    assertTrue(plainExport.endsWith("/plain.cube"), plainExport);
  }

  private IRowMeta readFields(String filename, Variables variables) throws Exception {
    CubeInputMeta meta = new CubeInputMeta();
    meta.setDefault();
    meta.getFile().setName(filename);
    IRowMeta fields = new RowMeta();
    meta.getFields(fields, "cube input", null, null, variables, null);
    return fields;
  }

  private static void assertField(IRowMeta fields) {
    assertEquals(1, fields.size());
    assertEquals("name", fields.getValueMeta(0).getName());
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
