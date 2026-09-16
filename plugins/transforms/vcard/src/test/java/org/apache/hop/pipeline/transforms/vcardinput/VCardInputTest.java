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
package org.apache.hop.pipeline.transforms.vcardinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.InputFile;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.pipeline.transforms.vcard.VCardFieldMapping;
import org.apache.hop.pipeline.transforms.vcard.VCardPropertyType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class VCardInputTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<VCardInputMeta, VCardInputData> mockHelper;
  private final List<Object[]> captured = new ArrayList<>();

  @BeforeEach
  void setUp() throws HopException {
    HopEnvironment.init();
    mockHelper =
        new TransformMockHelper<>("VCardInput", VCardInputMeta.class, VCardInputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void readsEachStaticFileOnce(@TempDir Path tempDir) throws Exception {
    writeVcard(tempDir.resolve("a.vcf"), "Contact A");
    writeVcard(tempDir.resolve("b.vcf"), "Contact B");
    writeVcard(tempDir.resolve("c.vcf"), "Contact C");

    VCardInputMeta meta = new VCardInputMeta();
    meta.setDoNotFailIfNoFile(false);
    meta.setIgnoringEmptyFile(true);
    meta.getFieldMappings().add(new VCardFieldMapping(VCardPropertyType.FN, "fn"));
    InputFile inputFile = new InputFile();
    inputFile.setFileName(tempDir.toAbsolutePath().toString());
    inputFile.setFileMask(".*\\.vcf");
    meta.getFileInput().getInputFiles().add(inputFile);

    VCardInputData data = new VCardInputData();
    VCardInput transform =
        new VCardInput(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    transform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
            captured.add(row);
          }
        });

    assertTrue(transform.init());
    int iterations = 0;
    while (transform.processRow()) {
      if (++iterations > 10) {
        break;
      }
    }

    assertEquals(3, captured.size(), "expected one row per vCard file");
    int fnIndex = data.outputRowMeta.indexOfValue("fn");
    Set<String> names =
        captured.stream().map(row -> (String) row[fnIndex]).collect(Collectors.toSet());
    assertEquals(Set.of("Contact A", "Contact B", "Contact C"), names);
    assertEquals(0, transform.getErrors());
  }

  /**
   * The output row metadata is built from the incoming row metadata, which only becomes available
   * after the first getRow(). Preparing it any earlier fails with "This transform is not receiving
   * rows from a previous transform".
   */
  @Test
  void readsFilesNamedByAnIncomingField(@TempDir Path tempDir) throws Exception {
    Path first = tempDir.resolve("first.vcf");
    Path second = tempDir.resolve("second.vcf");
    writeVcard(first, "Contact A");
    writeVcard(second, "Contact B");

    VCardInputMeta meta = new VCardInputMeta();
    meta.setDoNotFailIfNoFile(false);
    meta.setIgnoringEmptyFile(true);
    meta.getFieldMappings().add(new VCardFieldMapping(VCardPropertyType.FN, "fn"));
    meta.getFileInput().setAcceptingFilenames(true);
    meta.getFileInput().setAcceptingTransformName("filenames");
    meta.getFileInput().setAcceptingField("filename");

    VCardInputData data = new VCardInputData();
    VCardInput transform =
        new VCardInput(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    transform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
            captured.add(row);
          }
        });

    // Deliberately no setInputRowMeta() here: the transform has to pick the row metadata up from
    // the first row it reads, exactly like it does in a running pipeline.
    //
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("filename"));
    BlockingRowSet rowSet = new BlockingRowSet(10);
    rowSet.putRow(inputRowMeta, new Object[] {first.toAbsolutePath().toString()});
    rowSet.putRow(inputRowMeta, new Object[] {second.toAbsolutePath().toString()});
    rowSet.setDone();
    transform.addRowSetToInputRowSets(rowSet);

    assertTrue(transform.init());
    int iterations = 0;
    while (transform.processRow()) {
      if (++iterations > 10) {
        break;
      }
    }

    assertEquals(0, transform.getErrors());
    assertEquals(2, captured.size(), "expected one row per filename on the input stream");
    int fnIndex = data.outputRowMeta.indexOfValue("fn");
    Set<String> names =
        captured.stream().map(row -> (String) row[fnIndex]).collect(Collectors.toSet());
    assertEquals(Set.of("Contact A", "Contact B"), names);
  }

  private static void writeVcard(Path path, String fullName) throws Exception {
    String vcard =
        """
      BEGIN:VCARD
      VERSION:3.0
      FN:%s
      END:VCARD
      """
            .formatted(fullName);
    Files.writeString(path, vcard, StandardCharsets.UTF_8);
  }
}
