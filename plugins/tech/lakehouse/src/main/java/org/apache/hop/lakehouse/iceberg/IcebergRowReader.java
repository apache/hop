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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

/**
 * Reads an Iceberg table as Hop rows. Planning is done by Iceberg, so the filter prunes files and
 * row groups using column statistics before anything is read. With several transform copies, the
 * copies split the scan tasks between them without talking to each other, and every split is read
 * exactly once.
 */
public class IcebergRowReader implements Closeable, Iterator<Object[]> {

  private final Table table;
  private final Schema projection;
  private final Expression filter;
  private final IRowMeta rowMeta;
  private final Iterator<FileScanTask> tasks;
  private final List<Integer> plannedFiles = new ArrayList<>();
  private CloseableIterable<Record> currentIterable;
  private CloseableIterator<Record> current;

  /**
   * @param columns columns to read, or null for all
   * @param filter row filter, or null for none
   * @param snapshotId snapshot to read, or null for the current one
   */
  public IcebergRowReader(
      Table table,
      List<String> columns,
      Expression filter,
      Long snapshotId,
      int copyNr,
      int copyCount) {
    this.table = table;
    this.filter = filter == null ? Expressions.alwaysTrue() : filter;
    TableScan scan = table.newScan().filter(this.filter);
    if (columns != null) {
      scan = scan.select(columns);
    }
    if (snapshotId != null) {
      scan = scan.useSnapshot(snapshotId);
    }
    this.projection = scan.schema();

    // Each copy plans the scan on its own, and Iceberg doesn't return tasks in a stable order
    // (manifests are read in parallel). Assigning by position would give copies overlapping
    // slices, so each split is assigned by a hash of its file and offset instead. Every copy
    // computes the same assignment, and each split is read by exactly one copy.
    List<FileScanTask> mine = new ArrayList<>();
    try (CloseableIterable<CombinedScanTask> planned = scan.planTasks()) {
      for (CombinedScanTask combined : planned) {
        for (FileScanTask task : combined.files()) {
          if (copyFor(task, copyCount) == copyNr) {
            mine.add(task);
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    plannedFiles.add(mine.size());
    this.tasks = mine.iterator();

    RowMeta meta = new RowMeta();
    for (Types.NestedField field : projection.columns()) {
      meta.addValueMeta(IcebergTypeMapper.toHopValueMeta(field.name(), field.type()));
    }
    this.rowMeta = meta;
  }

  static int copyFor(FileScanTask task, int copyCount) {
    return Math.floorMod((task.file().location() + "@" + task.start()).hashCode(), copyCount);
  }

  /** The Hop row layout of the rows this reader returns. */
  public IRowMeta getRowMeta() {
    return rowMeta;
  }

  /** Number of file scan tasks this copy was given after pruning. */
  public int getTaskCount() {
    return plannedFiles.get(0);
  }

  @Override
  public boolean hasNext() {
    while (current == null || !current.hasNext()) {
      closeCurrent();
      if (!tasks.hasNext()) {
        return false;
      }
      open(tasks.next());
    }
    return true;
  }

  @Override
  public Object[] next() {
    Record record = current.next();
    List<Types.NestedField> fields = projection.columns();
    Object[] row = new Object[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      Types.NestedField field = fields.get(i);
      row[i] = IcebergTypeMapper.toHopValue(record.getField(field.name()), field.type());
    }
    return row;
  }

  private void open(FileScanTask task) {
    GenericDeleteFilter deletes =
        new GenericDeleteFilter(table.io(), task, table.schema(), projection);
    Schema readSchema = deletes.requiredSchema();
    CloseableIterable<Record> records =
        Parquet.read(table.io().newInputFile(task.file().location()))
            .project(readSchema)
            .split(task.start(), task.length())
            .filter(task.residual())
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(readSchema, fileSchema))
            .build();
    records = deletes.filter(records);

    // Row groups are pruned above; this drops the remaining rows that don't match.
    Evaluator evaluator = new Evaluator(readSchema.asStruct(), task.residual());
    InternalRecordWrapper wrapper = new InternalRecordWrapper(readSchema.asStruct());
    currentIterable = CloseableIterable.filter(records, r -> evaluator.eval(wrapper.wrap(r)));
    current = currentIterable.iterator();
  }

  private void closeCurrent() {
    try {
      if (current != null) {
        current.close();
      }
      if (currentIterable != null) {
        currentIterable.close();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      current = null;
      currentIterable = null;
    }
  }

  @Override
  public void close() {
    closeCurrent();
  }
}
