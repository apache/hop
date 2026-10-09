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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.expressions.Expressions;

/**
 * Collects the data files written by every copy of an output transform and turns them into a single
 * snapshot, or into nothing at all.
 *
 * <p>Copies call {@link #addFiles(List)} as they finish. Once the whole pipeline is done, exactly
 * one of {@link #commit()} or {@link #abort()} is called: commit when the run ended without errors,
 * abort otherwise. This keeps a run atomic. Readers either see all of its rows or none of them,
 * even when a transform further down the pipeline fails after this one has finished writing.
 */
public class CommitCoordinator {

  public enum WriteMode {
    APPEND,
    OVERWRITE_TABLE,
    OVERWRITE_PARTITIONS
  }

  public static final String SUMMARY_RUN_ID = "hop.run-id";

  private final Table table;
  private final Transaction createTransaction;
  private final WriteMode writeMode;
  private final Map<String, String> summary;
  private final Long startSnapshotId;
  private boolean published;
  private boolean outcomeUnknown;
  private final List<DataFile> files = new ArrayList<>();
  private boolean done;

  /**
   * Commits to an existing table.
   *
   * @param summary extra properties for the snapshot summary, such as the pipeline name and run id
   */
  public CommitCoordinator(Table table, WriteMode writeMode, Map<String, String> summary) {
    this(table, null, writeMode, summary);
  }

  /**
   * Commits to a table that is created by this run. The table only comes into existence together
   * with the run's snapshot, so a failed run leaves no empty table behind.
   */
  public CommitCoordinator(Transaction createTransaction, Map<String, String> summary) {
    this(createTransaction.table(), createTransaction, WriteMode.APPEND, summary);
  }

  private CommitCoordinator(
      Table table,
      Transaction createTransaction,
      WriteMode writeMode,
      Map<String, String> summary) {
    this.table = table;
    this.createTransaction = createTransaction;
    this.writeMode = writeMode;
    this.summary = Map.copyOf(summary);
    Snapshot current = createTransaction == null ? table.currentSnapshot() : null;
    this.startSnapshotId = current == null ? null : current.snapshotId();
  }

  /** The table to write data files for. */
  public Table table() {
    return table;
  }

  public synchronized void addFiles(List<DataFile> dataFiles) {
    if (done) {
      throw new IllegalStateException("The commit for this run has already been decided");
    }
    files.addAll(dataFiles);
  }

  /**
   * Commits all collected files as one snapshot.
   *
   * @return the id of the new snapshot, or null if the commit happened but the table couldn't be
   *     read back afterwards
   * @throws CommitStateUnknownException if the catalog couldn't say whether the commit happened;
   *     the data files must then be kept, see {@link #isPublished()}
   */
  public synchronized Long commit() {
    done = true;
    SnapshotUpdate<?> update =
        switch (writeMode) {
          case APPEND -> {
            AppendFiles append = table.newAppend();
            files.forEach(append::appendFile);
            yield append;
          }
          case OVERWRITE_TABLE -> {
            OverwriteFiles overwrite =
                table.newOverwrite().overwriteByRowFilter(Expressions.alwaysTrue());
            files.forEach(overwrite::addFile);
            // Fail instead of silently replacing data another writer added in the meantime. With
            // no starting snapshot (an empty table), every file now in the table counts as added
            // since the start, so a writer that gave the table its first snapshot is caught too.
            if (startSnapshotId != null) {
              overwrite.validateFromSnapshot(startSnapshotId);
            }
            overwrite.validateNoConflictingData();
            yield overwrite;
          }
          case OVERWRITE_PARTITIONS -> {
            ReplacePartitions replace = table.newReplacePartitions();
            files.forEach(replace::addFile);
            if (startSnapshotId != null) {
              replace.validateFromSnapshot(startSnapshotId);
            }
            replace.validateNoConflictingData().validateNoConflictingDeletes();
            yield replace;
          }
        };
    summary.forEach(update::set);
    try {
      update.commit();
      if (createTransaction != null) {
        createTransaction.commitTransaction();
      }
    } catch (CommitStateUnknownException e) {
      outcomeUnknown = true;
      throw e;
    }
    published = true;

    // The snapshot is in the table from here on: reading it back is only for the log.
    try {
      Table committed = createTransaction != null ? createTransaction.table() : table;
      committed.refresh();
      Snapshot snapshot = committed.currentSnapshot();
      return snapshot == null ? null : snapshot.snapshotId();
    } catch (RuntimeException e) {
      return null;
    }
  }

  /** True once the run's snapshot is in the table; the data files must then never be deleted. */
  public synchronized boolean isPublished() {
    return published;
  }

  /** True if the catalog couldn't say whether the commit happened. */
  public synchronized boolean isOutcomeUnknown() {
    return outcomeUnknown;
  }

  /**
   * Deletes every collected data file, so nothing of this run becomes visible. Refuses to do so
   * once the snapshot is published or when the commit outcome is unknown, since the table may then
   * reference those files.
   */
  public synchronized void abort() {
    if (published || outcomeUnknown) {
      throw new IllegalStateException(
          "The data files of this run may be referenced by the table and are kept");
    }
    done = true;
    for (DataFile file : files) {
      table.io().deleteFile(file.location());
    }
    files.clear();
  }

  public synchronized int fileCount() {
    return files.size();
  }
}
