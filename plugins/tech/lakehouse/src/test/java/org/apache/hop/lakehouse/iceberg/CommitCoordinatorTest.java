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

package org.apache.hop.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Test;

/** The run's data files are only deleted when the table can't be referencing them. */
class CommitCoordinatorTest {

  private final FileIO io = mock(FileIO.class);
  private final AppendFiles append = mock(AppendFiles.class);
  private final Table table = mock(Table.class);
  private final DataFile file = mock(DataFile.class);

  private CommitCoordinator coordinator() {
    when(table.io()).thenReturn(io);
    when(table.newAppend()).thenReturn(append);
    when(file.location()).thenReturn("file:///lake/orders/data/a.parquet");
    CommitCoordinator coordinator =
        new CommitCoordinator(table, CommitCoordinator.WriteMode.APPEND, Map.of());
    coordinator.addFiles(List.of(file));
    return coordinator;
  }

  @Test
  void unknownOutcomeKeepsTheFiles() {
    CommitCoordinator coordinator = coordinator();
    doThrow(new CommitStateUnknownException(new RuntimeException("timeout"))).when(append).commit();

    assertThrows(CommitStateUnknownException.class, coordinator::commit);

    assertTrue(coordinator.isOutcomeUnknown());
    assertFalse(coordinator.isPublished());
    assertThrows(IllegalStateException.class, coordinator::abort);
    verify(io, never()).deleteFile(anyString());
  }

  @Test
  void failureAfterPublishingKeepsTheFiles() {
    CommitCoordinator coordinator = coordinator();
    doThrow(new RuntimeException("refresh failed")).when(table).refresh();

    // The snapshot is in the table; only reading it back failed.
    coordinator.commit();

    assertTrue(coordinator.isPublished());
    assertThrows(IllegalStateException.class, coordinator::abort);
    verify(io, never()).deleteFile(anyString());
  }

  @Test
  void definiteFailureDeletesTheFiles() {
    CommitCoordinator coordinator = coordinator();
    doThrow(new CommitFailedException("conflict")).when(append).commit();

    assertThrows(CommitFailedException.class, coordinator::commit);

    assertFalse(coordinator.isPublished());
    coordinator.abort();
    verify(io).deleteFile("file:///lake/orders/data/a.parquet");
  }
}
