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
package org.apache.hop.pipeline.transforms.vcardoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.pipeline.transforms.vcard.VCardFieldMapping;
import org.apache.hop.pipeline.transforms.vcard.VCardPropertyType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class VCardOutputTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<VCardOutputMeta, VCardOutputData> mockHelper;

  @BeforeEach
  void setUp() throws HopException {
    HopEnvironment.init();
    mockHelper =
        new TransformMockHelper<>("VCardOutput", VCardOutputMeta.class, VCardOutputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void writeToFileIncrementsOutputLines(@TempDir Path tempDir) throws Exception {
    Path outputBase = tempDir.resolve("vcard-output");
    VCardOutputMeta meta = new VCardOutputMeta();
    meta.setOperationType(VCardOutputMeta.OperationType.WRITE_TO_FILE);
    meta.setPassInputFields(false);
    meta.setAddProdId(false);
    meta.setAddRevision(false);
    meta.setFileNameInField(false);
    meta.getFileSettings().setFileName(outputBase.toAbsolutePath().toString());
    meta.getFieldMappings().add(new VCardFieldMapping(VCardPropertyType.FN, "fn"));

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("fn"));
    Object[] row = {"Hop Test"};

    IRowSet rowSet = mockHelper.getMockInputRowSet(row);
    when(rowSet.getRowMeta()).thenReturn(rowMeta);

    VCardOutputData data = new VCardOutputData();
    VCardOutput transform =
        new VCardOutput(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    transform.addRowSetToInputRowSets(rowSet);

    assertTrue(transform.init());
    assertTrue(transform.processRow());
    assertFalse(transform.processRow());

    assertEquals(1, transform.getLinesOutput());
    assertEquals(1, transform.getLinesWritten());
    Path outputFile = Path.of(outputBase + ".vcf");
    assertTrue(Files.exists(outputFile));
    assertTrue(Files.readString(outputFile).contains("FN:Hop Test"));
  }

  /** With "Append" off every row used to overwrite the file, leaving only the last card. */
  @Test
  void everyCardOfARunEndsUpInTheFile(@TempDir Path tempDir) throws Exception {
    Path outputBase = tempDir.resolve("contacts");
    VCardOutputMeta meta = fileMeta(outputBase);
    Path outputFile = Path.of(outputBase + ".vcf");
    Files.writeString(outputFile, "left over from an earlier run");

    run(meta, nameRows(), new Object[] {"Ann"}, new Object[] {"Bob"}, new Object[] {"Cid"});

    String content = Files.readString(outputFile);
    assertEquals(3, count(content, "BEGIN:VCARD"));
    assertTrue(content.contains("FN:Ann"));
    assertTrue(content.contains("FN:Bob"));
    assertTrue(content.contains("FN:Cid"));
    // not appending: the earlier content is replaced
    assertFalse(content.contains("left over"));
  }

  @Test
  void appendKeepsWhatAnEarlierRunWrote(@TempDir Path tempDir) throws Exception {
    Path outputBase = tempDir.resolve("contacts");
    VCardOutputMeta meta = fileMeta(outputBase);
    meta.getFileSettings().setAppend(true);

    run(meta, nameRows(), new Object[] {"Ann"});
    run(meta, nameRows(), new Object[] {"Bob"}, new Object[] {"Cid"});

    String content = Files.readString(Path.of(outputBase + ".vcf"));
    assertEquals(3, count(content, "BEGIN:VCARD"));
    assertTrue(content.indexOf("FN:Ann") < content.indexOf("FN:Bob"));
  }

  @Test
  void aSecondRunWithoutAppendReplacesTheFile(@TempDir Path tempDir) throws Exception {
    Path outputBase = tempDir.resolve("contacts");
    VCardOutputMeta meta = fileMeta(outputBase);

    run(meta, nameRows(), new Object[] {"Ann"}, new Object[] {"Bob"});
    run(meta, nameRows(), new Object[] {"Cid"});

    String content = Files.readString(Path.of(outputBase + ".vcf"));
    assertEquals(1, count(content, "BEGIN:VCARD"));
    assertTrue(content.contains("FN:Cid"));
  }

  /** A file name that returns later in the run keeps the cards it already has. */
  @Test
  void cardsFollowTheFileNameFromAField(@TempDir Path tempDir) throws Exception {
    String a = tempDir.resolve("a.vcf").toString();
    String b = tempDir.resolve("b.vcf").toString();
    VCardOutputMeta meta = fileMeta(tempDir.resolve("unused"));
    meta.setFileNameInField(true);
    meta.setFileNameField("file");

    RowMeta rowMeta = nameRows();
    rowMeta.addValueMeta(new ValueMetaString("file"));
    VCardOutput transform =
        run(
            meta,
            rowMeta,
            new Object[] {"Ann", a},
            new Object[] {"Bob", b},
            new Object[] {"Cid", a},
            new Object[] {"Dee", a});

    String contentA = Files.readString(Path.of(a));
    assertEquals(3, count(contentA, "BEGIN:VCARD"));
    assertTrue(contentA.contains("FN:Ann"));
    assertTrue(contentA.contains("FN:Cid"));
    assertTrue(contentA.contains("FN:Dee"));
    String contentB = Files.readString(Path.of(b));
    assertEquals(1, count(contentB, "BEGIN:VCARD"));
    assertTrue(contentB.contains("FN:Bob"));
    // each file is in the result once, however many cards it got
    assertEquals(2, transform.getResultFiles().size());
  }

  @Test
  void theDateAndTimeInTheFileNameAreDigits(@TempDir Path tempDir) throws Exception {
    VCardOutputMeta meta = fileMeta(tempDir.resolve("contacts"));
    meta.getFileSettings().setDateInFileName(true);
    meta.getFileSettings().setTimeInFileName(true);

    run(meta, nameRows(), new Object[] {"Ann"}, new Object[] {"Bob"});

    try (Stream<Path> files = Files.list(tempDir)) {
      List<String> names = files.map(p -> p.getFileName().toString()).toList();
      assertEquals(1, names.size(), names.toString());
      assertTrue(names.get(0).matches("contacts_\\d{8}_\\d{6}\\.vcf"), names.toString());
    }
  }

  @Test
  void theFileIsCreatedAtStartOnlyWhenAskedFor(@TempDir Path tempDir) throws Exception {
    VCardOutputMeta notAtStart = fileMeta(tempDir.resolve("later"));
    run(notAtStart, nameRows());
    assertFalse(Files.exists(tempDir.resolve("later.vcf")));

    VCardOutputMeta atStart = fileMeta(tempDir.resolve("empty"));
    atStart.getFileSettings().setDoNotOpenNewFileInit(false);
    run(atStart, nameRows());
    assertTrue(Files.exists(tempDir.resolve("empty.vcf")));
    assertEquals(0, Files.size(tempDir.resolve("empty.vcf")));
  }

  private static VCardOutputMeta fileMeta(Path outputBase) {
    VCardOutputMeta meta = new VCardOutputMeta();
    meta.setOperationType(VCardOutputMeta.OperationType.WRITE_TO_FILE);
    meta.setPassInputFields(false);
    meta.setAddProdId(false);
    meta.setAddRevision(false);
    meta.setFileNameInField(false);
    meta.getFileSettings().setFileName(outputBase.toAbsolutePath().toString());
    meta.getFieldMappings().add(new VCardFieldMapping(VCardPropertyType.FN, "fn"));
    return meta;
  }

  private static RowMeta nameRows() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("fn"));
    return rowMeta;
  }

  /** Runs the transform over the rows, from init to dispose. */
  private VCardOutput run(VCardOutputMeta meta, RowMeta rowMeta, Object[]... rows)
      throws Exception {
    IRowSet rowSet = mockHelper.getMockInputRowSet(rows);
    when(rowSet.getRowMeta()).thenReturn(rowMeta);

    VCardOutput transform =
        new VCardOutput(
            mockHelper.transformMeta,
            meta,
            new VCardOutputData(),
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    transform.addRowSetToInputRowSets(rowSet);

    assertTrue(transform.init());
    for (int i = 0; i < rows.length; i++) {
      assertTrue(transform.processRow());
    }
    assertFalse(transform.processRow());
    transform.dispose();
    return transform;
  }

  private static int count(String text, String part) {
    int count = 0;
    for (int at = text.indexOf(part); at >= 0; at = text.indexOf(part, at + part.length())) {
      count++;
    }
    return count;
  }
}
