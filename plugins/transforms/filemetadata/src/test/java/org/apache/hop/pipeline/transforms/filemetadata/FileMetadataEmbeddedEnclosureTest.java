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

package org.apache.hop.pipeline.transforms.filemetadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * #5609: a file with enclosures inside enclosed fields (BOB "ROBERT" SMITH) and amounts with two
 * decimals. The enclosure was dropped once more than 15 rows were scanned, and the amounts were
 * given a precision of 1.
 */
class FileMetadataEmbeddedEnclosureTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static final int ENCLOSURE = 2;
  private static final int FIELD_COUNT = 3;
  private static final int SKIP_FOOTER_LINES = 5;
  private static final int NAME = 7;
  private static final int TYPE = 8;
  private static final int LENGTH = 9;
  private static final int PRECISION = 10;
  private static final int MASK = 11;

  private TransformMockHelper<FileMetadataMeta, FileMetadataData> mockHelper;

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>("FileMetadata", FileMetadataMeta.class, FileMetadataData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @ParameterizedTest
  @ValueSource(strings = {"15", "16", "10000", ""})
  void detectsEnclosureAndDecimals(String limitRows) throws Exception {
    List<Object[]> rows = detect(limitRows);

    assertEquals(11, rows.size(), "one row per column");
    Object[] first = rows.get(0);
    assertEquals("\"", first[ENCLOSURE]);
    assertEquals(11L, first[FIELD_COUNT]);
    assertEquals(0L, first[SKIP_FOOTER_LINES]);

    assertEquals("FY_DC", first[NAME]);
    assertEquals("Integer", first[TYPE]);

    Object[] name = rows.get(7);
    assertEquals("LGL_NM_UP", name[NAME]);
    assertEquals("String", name[TYPE]);
    // BOB "ROBERT" SMITH is read as one value, enclosures inside included
    assertEquals(18L, name[LENGTH]);

    Object[] amount = rows.get(10);
    assertEquals("SUM_PSTNG_AM", amount[NAME]);
    assertEquals("Number", amount[TYPE]);
    assertEquals(2L, amount[PRECISION]);
    assertEquals("#.00", amount[MASK]);
  }

  private List<Object[]> detect(String limitRows) throws Exception {
    FileMetadataMeta meta = new FileMetadataMeta();
    meta.setDefault();
    meta.setLimitRows(limitRows);
    meta.setFileName(
        getClass()
            .getResource(
                '/'
                    + getClass().getPackage().getName().replace('.', '/')
                    + "/delimited/embedded-enclosure.csv")
            .toURI()
            .toString());

    when(mockHelper.transformMeta.getTransform()).thenReturn(meta);
    FileMetadata transform =
        new FileMetadata(
            mockHelper.transformMeta,
            meta,
            new FileMetadataData(),
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    BlockingRowSet output = new BlockingRowSet(100);
    transform.addRowSetToOutputRowSets(output);

    assertFalse(transform.processRow(), "the transform generates its rows and is then done");

    List<Object[]> rows = new ArrayList<>();
    Object[] row;
    while ((row = output.getRowWait(100, TimeUnit.MILLISECONDS)) != null) {
      rows.add(row);
    }
    return rows;
  }
}
