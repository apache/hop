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
    Object[] row = new Object[] {"Hop Test"};

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
}
