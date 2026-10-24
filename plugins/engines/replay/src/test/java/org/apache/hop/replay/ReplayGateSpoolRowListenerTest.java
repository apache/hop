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

package org.apache.hop.replay;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.replay.manifest.SnapshotManifest;
import org.apache.hop.replay.pipeline.ReplayGateSpoolReader;
import org.apache.hop.replay.pipeline.ReplayGateSpoolRowListener;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplayGateSpoolRowListenerTest {

  @TempDir Path tempFolder;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
  }

  private IRowMeta createTestRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  @Test
  void testSpoolAndReadSnappyRows() throws Exception {
    String spoolDir = tempFolder.resolve("spool-snappy").toUri().toString();
    IVariables variables = new Variables();
    ILogChannel log = mock(ILogChannel.class);
    IEngineComponent component = mock(IEngineComponent.class);
    when(component.getName()).thenReturn("TestTransform");
    when(component.getCopyNr()).thenReturn(0);

    ReplayGate gate = new ReplayGate();
    gate.setEnabled(true);
    gate.setSpoolDirectory(spoolDir);
    gate.setCompression("Snappy");
    gate.setRowLimit(0);

    ReplayGateSpoolRowListener listener =
        new ReplayGateSpoolRowListener(component, gate, "TestPipeline", variables, 1, log);

    IRowMeta rowMeta = createTestRowMeta();
    for (int i = 0; i < 10; i++) {
      listener.rowWrittenEvent(rowMeta, new Object[] {(long) i, "row-" + i});
    }
    listener.close(false);

    assertEquals(10L, listener.getRowCount().get());

    String spoolPath =
        ReplayGateUtil.getTransformSpoolPath(variables, gate, "TestPipeline", "TestTransform");
    FileObject manifestFile = HopVfs.getFileObject(spoolPath + "/SnapshotManifest.json", variables);
    assertTrue(manifestFile.exists());

    FileObject dataFile = HopVfs.getFileObject(spoolPath + "/data.bin.snappy", variables);
    assertTrue(dataFile.exists());

    try (ReplayGateSpoolReader reader = new ReplayGateSpoolReader(spoolPath, variables)) {
      SnapshotManifest manifest = reader.open();
      assertNotNull(manifest);
      assertEquals(SnapshotManifest.STATUS_SEALED, manifest.getStatus());
      assertEquals(10L, manifest.getRowCount());
      assertEquals("Snappy", manifest.getCompression());
      assertNotNull(manifest.getChecksum());

      IRowMeta readMeta = reader.getRowMeta();
      assertEquals(2, readMeta.size());
      assertEquals("id", readMeta.getValueMeta(0).getName());
      assertEquals("name", readMeta.getValueMeta(1).getName());

      for (int i = 0; i < 10; i++) {
        Object[] row = reader.readRow();
        assertNotNull(row);
        assertEquals((long) i, row[0]);
        assertEquals("row-" + i, row[1]);
      }
      assertNull(reader.readRow());
    }
  }

  @Test
  void testSpoolWithRowLimit() throws Exception {
    String spoolDir = tempFolder.resolve("spool-limit").toUri().toString();
    IVariables variables = new Variables();
    ILogChannel log = mock(ILogChannel.class);
    IEngineComponent component = mock(IEngineComponent.class);
    when(component.getName()).thenReturn("LimitedTransform");
    when(component.getCopyNr()).thenReturn(0);

    ReplayGate gate = new ReplayGate();
    gate.setEnabled(true);
    gate.setSpoolDirectory(spoolDir);
    gate.setCompression("Gzip");
    gate.setRowLimit(5);

    ReplayGateSpoolRowListener listener =
        new ReplayGateSpoolRowListener(component, gate, "TestPipeline", variables, 1, log);

    IRowMeta rowMeta = createTestRowMeta();
    for (int i = 0; i < 20; i++) {
      listener.rowWrittenEvent(rowMeta, new Object[] {(long) i, "row-" + i});
    }
    listener.close(false);

    assertEquals(5L, listener.getRowCount().get());

    String spoolPath =
        ReplayGateUtil.getTransformSpoolPath(variables, gate, "TestPipeline", "LimitedTransform");
    try (ReplayGateSpoolReader reader = new ReplayGateSpoolReader(spoolPath, variables)) {
      SnapshotManifest manifest = reader.open();
      assertEquals(5L, manifest.getRowCount());
      assertEquals("Gzip", manifest.getCompression());
      assertEquals(SnapshotManifest.STATUS_SEALED, manifest.getStatus());
    }
  }

  @Test
  void testSpoolInterruptedStatusOnFailure() throws Exception {
    String spoolDir = tempFolder.resolve("spool-failed").toUri().toString();
    IVariables variables = new Variables();
    ILogChannel log = mock(ILogChannel.class);
    IEngineComponent component = mock(IEngineComponent.class);
    when(component.getName()).thenReturn("FailingTransform");
    when(component.getCopyNr()).thenReturn(0);

    ReplayGate gate = new ReplayGate();
    gate.setEnabled(true);
    gate.setSpoolDirectory(spoolDir);

    ReplayGateSpoolRowListener listener =
        new ReplayGateSpoolRowListener(component, gate, "TestPipeline", variables, 1, log);

    IRowMeta rowMeta = createTestRowMeta();
    listener.rowWrittenEvent(rowMeta, new Object[] {1L, "row-1"});
    listener.close(true); // Close with error

    String spoolPath =
        ReplayGateUtil.getTransformSpoolPath(variables, gate, "TestPipeline", "FailingTransform");
    try (ReplayGateSpoolReader reader = new ReplayGateSpoolReader(spoolPath, variables)) {
      SnapshotManifest manifest = reader.open();
      assertEquals(SnapshotManifest.STATUS_INTERRUPTED, manifest.getStatus());
    }
  }
}
