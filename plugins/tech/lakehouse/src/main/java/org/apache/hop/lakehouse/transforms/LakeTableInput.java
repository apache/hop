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

package org.apache.hop.lakehouse.transforms;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.lakehouse.LakeField;
import org.apache.hop.lakehouse.LakeFormats;
import org.apache.hop.lakehouse.iceberg.IcebergRowReader;
import org.apache.hop.lakehouse.iceberg.IcebergTables;
import org.apache.hop.lakehouse.iceberg.PluginClassLoader;
import org.apache.hop.lakehouse.metadata.LakeCatalog;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.SnapshotUtil;

/**
 * Reads a lake table on the local engine. Iceberg tables are read with the Iceberg Java library,
 * with all files going through Hop VFS. When the transform runs as several copies, each copy reads
 * its own share of the table. On the Spark engine this class isn't used: the Spark plugin reads the
 * table as a Dataset instead.
 */
public class LakeTableInput extends BaseTransform<LakeTableInputMeta, LakeTableInputData> {

  /** Marks a table that had no snapshot yet in the shared current snapshot entry. */
  private static final Long NO_SNAPSHOT = -1L;

  private static final DateTimeFormatter TIMESTAMP_FORMAT =
      new DateTimeFormatterBuilder()
          .appendPattern("yyyy-MM-dd[ ]['T']HH:mm:ss")
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .optionalEnd()
          .toFormatter();

  public LakeTableInput(
      TransformMeta transformMeta,
      LakeTableInputMeta meta,
      LakeTableInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    try (PluginClassLoader ignored = PluginClassLoader.activate()) {
      return readRow();
    }
  }

  private boolean readRow() throws HopException {
    if (first) {
      first = false;
      openReader();
    }

    if (!data.reader.hasNext()) {
      setOutputDone();
      return false;
    }

    Object[] row = data.reader.next();
    putRow(data.outputRowMeta, toOutputRow(row));
    return true;
  }

  private void openReader() throws HopException {
    String format = resolve(meta.getFormat());
    if (!LakeFormats.FORMAT_ICEBERG.equalsIgnoreCase(format)) {
      throw new HopException(
          "Table format '"
              + format
              + "' isn't supported on this engine yet. The local engine reads Apache Iceberg"
              + " tables; Delta Lake tables can be read with the Spark engine.");
    }

    Table table = loadTable();
    int copies = getTransformMeta().getCopies(this);
    Long snapshotId = snapshotId(table);
    if (snapshotId == null && copies > 1) {
      snapshotId = sharedCurrentSnapshotId(table);
    }

    List<String> columns = null;
    if (!meta.getFields().isEmpty()) {
      columns = new ArrayList<>();
      for (LakeField field : meta.getFields()) {
        columns.add(field.getName());
      }
    }

    data.reader = new IcebergRowReader(table, columns, null, snapshotId, getCopy(), copies);
    data.readRowMeta = data.reader.getRowMeta();
    if (columns == null) {
      data.outputRowMeta = data.readRowMeta.clone();
    } else {
      data.outputRowMeta = new RowMeta();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
      // Iceberg returns the selected columns in table order, not in the order they are listed.
      data.readIndexes = new int[data.outputRowMeta.size()];
      for (int i = 0; i < data.readIndexes.length; i++) {
        String name = data.outputRowMeta.getValueMeta(i).getName();
        data.readIndexes[i] = data.readRowMeta.indexOfValue(name);
        if (data.readIndexes[i] < 0) {
          throw new HopException("Column '" + name + "' isn't in table " + table.name());
        }
      }
    }
    logBasic(
        "Reading Iceberg table "
            + table.name()
            + (snapshotId == null ? "" : " at snapshot " + snapshotId)
            + ": "
            + data.reader.getTaskCount()
            + " split(s) for this copy");
  }

  Table loadTable() throws HopException {
    if (LakeTableInputMeta.MODE_TABLE.equalsIgnoreCase(meta.getIdentifierMode())) {
      String catalogName = resolve(meta.getCatalogMetadataName());
      if (StringUtils.isBlank(catalogName)) {
        throw new HopException("Table mode needs a catalog: please select one");
      }
      LakeCatalog catalog = metadataProvider.getSerializer(LakeCatalog.class).load(catalogName);
      if (catalog == null) {
        throw new HopException("Catalog '" + catalogName + "' couldn't be found");
      }
      return IcebergTables.loadFromCatalog(catalog, meta.getTableIdentifier(), this);
    }

    String path = resolve(meta.getTablePath());
    if (StringUtils.isBlank(path)) {
      throw new HopException("Please specify the location of the table");
    }
    return IcebergTables.loadFromPath(path);
  }

  /**
   * The current snapshot, resolved once for all copies of this transform in this pipeline run. Each
   * copy loads the table on its own, so without this a commit landing while the copies start could
   * make them read different snapshots, and rows would be read twice or not at all.
   */
  Long sharedCurrentSnapshotId(Table table) {
    Map<String, Object> shared = getPipeline().getExtensionDataMap();
    String key = LakeTableInput.class.getName() + ".currentSnapshot." + getTransformName();
    synchronized (shared) {
      Object id = shared.get(key);
      if (id == null) {
        Snapshot current = table.currentSnapshot();
        id = current == null ? NO_SNAPSHOT : current.snapshotId();
        shared.put(key, id);
      }
      return NO_SNAPSHOT.equals(id) ? null : (Long) id;
    }
  }

  /** The snapshot to read for time travel, or null for the current snapshot. */
  Long snapshotId(Table table) throws HopException {
    String type = StringUtils.defaultString(meta.getTimeTravelType());
    if (LakeTableInputMeta.TIME_TRAVEL_VERSION.equalsIgnoreCase(type)) {
      String version = resolve(meta.getTimeTravelVersion());
      try {
        long snapshotId = Long.parseLong(StringUtils.trim(version));
        if (table.snapshot(snapshotId) == null) {
          throw new HopException(
              "Table " + table.name() + " has no snapshot with id " + snapshotId);
        }
        return snapshotId;
      } catch (NumberFormatException e) {
        throw new HopException("'" + version + "' isn't a valid Iceberg snapshot id", e);
      }
    }
    if (LakeTableInputMeta.TIME_TRAVEL_TIMESTAMP.equalsIgnoreCase(type)) {
      long millis = parseTimestamp(resolve(meta.getTimeTravelTimestamp()));
      try {
        return SnapshotUtil.snapshotIdAsOfTime(table, millis);
      } catch (IllegalArgumentException e) {
        throw new HopException(
            "Table "
                + table.name()
                + " has no snapshot as of '"
                + meta.getTimeTravelTimestamp()
                + "'",
            e);
      }
    }
    return null;
  }

  /**
   * Parses a time travel timestamp the way the Spark engine accepts it: {@code yyyy-MM-dd
   * HH:mm:ss[.fraction]} in the local time zone, an ISO-8601 timestamp with an offset, or epoch
   * milliseconds.
   */
  static long parseTimestamp(String value) throws HopException {
    String text = StringUtils.trim(value);
    if (StringUtils.isEmpty(text)) {
      throw new HopException("Please specify the timestamp to read the table as of");
    }
    if (StringUtils.isNumeric(text)) {
      return Long.parseLong(text);
    }
    try {
      return OffsetDateTime.parse(text).toInstant().toEpochMilli();
    } catch (DateTimeParseException e) {
      // Not an offset timestamp, try a local one below.
    }
    try {
      return LocalDateTime.parse(text, TIMESTAMP_FORMAT)
          .atZone(ZoneId.systemDefault())
          .toInstant()
          .toEpochMilli();
    } catch (DateTimeParseException e) {
      throw new HopException(
          "'" + value + "' isn't a valid timestamp, use for example 2026-10-01 12:00:00", e);
    }
  }

  private Object[] toOutputRow(Object[] row) throws HopException {
    if (data.readIndexes == null) {
      return RowDataUtil.resizeArray(row, data.outputRowMeta.size());
    }
    Object[] out = RowDataUtil.allocateRowData(data.outputRowMeta.size());
    for (int i = 0; i < data.readIndexes.length; i++) {
      int index = data.readIndexes[i];
      IValueMeta source = data.readRowMeta.getValueMeta(index);
      IValueMeta target = data.outputRowMeta.getValueMeta(i);
      out[i] = target.convertData(source, row[index]);
    }
    return out;
  }

  @Override
  public void dispose() {
    if (data.reader != null) {
      try (PluginClassLoader ignored = PluginClassLoader.activate()) {
        data.reader.close();
      }
      data.reader = null;
    }
    super.dispose();
  }
}
