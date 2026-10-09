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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.lakehouse.LakeFormats;
import org.apache.hop.lakehouse.iceberg.CommitCoordinator;
import org.apache.hop.lakehouse.iceberg.CommitCoordinator.WriteMode;
import org.apache.hop.lakehouse.iceberg.IcebergRowWriter;
import org.apache.hop.lakehouse.iceberg.IcebergTableTarget;
import org.apache.hop.lakehouse.iceberg.IcebergTables;
import org.apache.hop.lakehouse.iceberg.IcebergTypeMapper;
import org.apache.hop.lakehouse.iceberg.PluginClassLoader;
import org.apache.hop.lakehouse.metadata.LakeCatalog;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.CommitStateUnknownException;

/**
 * Writes rows to a lake table on the local engine and passes them on unchanged.
 *
 * <p>Every copy of the transform writes its own Parquet files. Nothing becomes visible in the table
 * until the whole pipeline has finished: then all files of the run are committed as one Iceberg
 * snapshot, or deleted if the pipeline failed or was stopped. A transform further down the pipeline
 * that fails after this one has written its rows still rolls the run back, the way a database
 * transaction would. On the Spark engine this class isn't used.
 */
public class LakeTableOutput extends BaseTransform<LakeTableOutputMeta, LakeTableOutputData> {

  private static final String COORDINATOR_KEY = "lakehouse.iceberg.coordinator.";

  public LakeTableOutput(
      TransformMeta transformMeta,
      LakeTableOutputMeta meta,
      LakeTableOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    try (PluginClassLoader ignored = PluginClassLoader.activate()) {
      return writeRow();
    }
  }

  private boolean writeRow() throws HopException {
    Object[] row = getRow();
    if (first) {
      first = false;
      IRowMeta rowMeta =
          row == null
              ? getPipelineMeta().getPrevTransformFields(this, getTransformMeta())
              : getInputRowMeta();
      data.coordinator = coordinator(rowMeta).orElse(null);
      if (data.coordinator != null) {
        data.writer =
            new IcebergRowWriter(
                data.coordinator.table(),
                rowMeta,
                getCopy(),
                System.currentTimeMillis(),
                IcebergRowWriter.DEFAULT_TARGET_FILE_SIZE);
      }
    }

    if (row == null) {
      if (data.writer != null) {
        data.coordinator.addFiles(data.writer.complete());
        data.writer = null;
      }
      setOutputDone();
      return false;
    }

    if (data.writer != null) {
      data.writer.write(row);
    }
    putRow(getInputRowMeta(), row);
    return true;
  }

  /**
   * The commit coordinator shared by all copies of this transform in this run, created by the first
   * copy that gets here. Empty when the save mode says to leave an existing table alone.
   */
  @SuppressWarnings("unchecked")
  private Optional<CommitCoordinator> coordinator(IRowMeta rowMeta) throws HopException {
    Map<String, Object> shared = getPipeline().getExtensionDataMap();
    String key = COORDINATOR_KEY + getTransformName();
    synchronized (shared) {
      if (!shared.containsKey(key)) {
        Optional<CommitCoordinator> coordinator = Optional.ofNullable(newCoordinator(rowMeta));
        shared.put(key, coordinator);
        coordinator.ifPresent(this::commitWhenPipelineFinishes);
      }
      return (Optional<CommitCoordinator>) shared.get(key);
    }
  }

  /** Coalesce and extra options configure a Spark write; this engine doesn't use them. */
  private void warnAboutSparkOnlyOptions() {
    for (String[] option :
        new String[][] {
          {"Coalesce partitions", meta.getCoalescePartitions()},
          {"Extra options", meta.getExtraOptions()}
        }) {
      if (StringUtils.isNotBlank(resolve(option[1]))) {
        logError(
            "Warning: '"
                + option[0]
                + "' only applies to the Spark engine and is ignored when writing on this engine");
      }
    }
  }

  private CommitCoordinator newCoordinator(IRowMeta rowMeta) throws HopException {
    warnAboutSparkOnlyOptions();
    String format = resolve(meta.getFormat());
    if (!LakeFormats.FORMAT_ICEBERG.equalsIgnoreCase(format)) {
      throw new HopException(
          "Table format '"
              + format
              + "' isn't supported on this engine yet. The local engine writes Apache Iceberg"
              + " tables; Delta Lake tables can be written with the Spark engine.");
    }

    IcebergTableTarget target = target();
    String saveMode =
        StringUtils.defaultIfBlank(resolve(meta.getSaveMode()), LakeTableOutputMeta.MODE_ERROR);
    Map<String, String> summary = new HashMap<>();
    summary.put(CommitCoordinator.SUMMARY_RUN_ID, getPipeline().getLogChannelId());
    summary.put("hop.pipeline", Const.NVL(getPipelineMeta().getName(), ""));

    if (target.exists()) {
      if (LakeTableOutputMeta.MODE_ERROR.equalsIgnoreCase(saveMode)) {
        throw new HopException(
            "Table "
                + target.name()
                + " already exists. Choose the Append or Overwrite save mode to write to it.");
      }
      if (LakeTableOutputMeta.MODE_IGNORE.equalsIgnoreCase(saveMode)) {
        logBasic("Table " + target.name() + " already exists, rows are not written (Ignore)");
        return null;
      }
      WriteMode mode;
      if (LakeTableOutputMeta.MODE_OVERWRITE.equalsIgnoreCase(saveMode)) {
        mode = WriteMode.OVERWRITE_TABLE;
      } else if (LakeTableOutputMeta.MODE_APPEND.equalsIgnoreCase(saveMode)) {
        mode = WriteMode.APPEND;
      } else {
        throw unsupportedSaveMode(saveMode);
      }
      return new CommitCoordinator(target.load(), mode, summary);
    }

    if (!isSupportedSaveMode(saveMode)) {
      throw unsupportedSaveMode(saveMode);
    }
    Schema schema = IcebergTypeMapper.toIcebergSchema(rowMeta);
    PartitionSpec spec = partitionSpec(schema, resolve(meta.getPartitionByColumns()));
    logBasic(
        "Creating table "
            + target.name()
            + (spec.isUnpartitioned() ? "" : ", partitioned by " + meta.getPartitionByColumns()));
    return new CommitCoordinator(target.create(schema, spec, Map.of()), summary);
  }

  private static boolean isSupportedSaveMode(String saveMode) {
    for (String mode :
        new String[] {
          LakeTableOutputMeta.MODE_APPEND,
          LakeTableOutputMeta.MODE_OVERWRITE,
          LakeTableOutputMeta.MODE_ERROR,
          LakeTableOutputMeta.MODE_IGNORE
        }) {
      if (mode.equalsIgnoreCase(saveMode)) {
        return true;
      }
    }
    return false;
  }

  private static HopException unsupportedSaveMode(String saveMode) {
    return new HopException(
        "Save mode '"
            + saveMode
            + "' isn't supported. Use Append, Overwrite, ErrorIfExists or Ignore.");
  }

  private IcebergTableTarget target() throws HopException {
    if (LakeTableInputMeta.MODE_TABLE.equalsIgnoreCase(meta.getIdentifierMode())) {
      String catalogName = resolve(meta.getCatalogMetadataName());
      if (StringUtils.isBlank(catalogName)) {
        throw new HopException("Table mode needs a catalog: please select one");
      }
      LakeCatalog catalog = metadataProvider.getSerializer(LakeCatalog.class).load(catalogName);
      if (catalog == null) {
        throw new HopException("Catalog '" + catalogName + "' couldn't be found");
      }
      return IcebergTables.targetInCatalog(catalog, meta.getTableIdentifier(), this);
    }
    String path = resolve(meta.getTablePath());
    if (StringUtils.isBlank(path)) {
      throw new HopException("Please specify the location of the table");
    }
    return IcebergTableTarget.atPath(path);
  }

  /** Identity partitions on the listed columns, used when the table is created. */
  static PartitionSpec partitionSpec(Schema schema, String columns) throws HopException {
    if (StringUtils.isBlank(columns)) {
      return PartitionSpec.unpartitioned();
    }
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    List<String> missing = new ArrayList<>();
    for (String column : columns.split(",")) {
      String name = column.trim();
      if (name.isEmpty()) {
        continue;
      }
      if (schema.findField(name) == null) {
        missing.add(name);
      } else {
        builder.identity(name);
      }
    }
    if (!missing.isEmpty()) {
      throw new HopException("Partition column(s) not found in the input: " + missing);
    }
    return builder.build();
  }

  private void commitWhenPipelineFinishes(CommitCoordinator coordinator) {
    getPipeline()
        .addExecutionFinishedListener(
            pipeline -> {
              // This runs on the thread of whichever transform finished last.
              try (PluginClassLoader ignored = PluginClassLoader.activate()) {
                finishRun(pipeline.getErrors() > 0 || pipeline.isStopped(), coordinator);
              }
            });
  }

  /**
   * Commits or aborts the run. This runs from an execution-finished listener, after Hop has already
   * marked the pipeline finished, so for the duration of the commit the pipeline reports itself as
   * running again: a status poll (the remote engine, a parent workflow) can't see "Finished" before
   * the snapshot exists. A failed commit is counted as an error of this transform before the
   * listener returns, so it is part of the pipeline result that waiters read.
   */
  private void finishRun(boolean failed, CommitCoordinator coordinator) throws HopException {
    if (failed) {
      coordinator.abort();
      logBasic("The pipeline didn't finish successfully: no rows were committed");
      return;
    }
    // On the local engine, and inside Hop Server for the remote engine, this is a Pipeline.
    Pipeline pipeline = getPipeline() instanceof Pipeline local ? local : null;
    if (pipeline != null) {
      pipeline.setRunning(true);
    }
    try {
      Long snapshotId = coordinator.commit();
      logBasic(
          "Committed "
              + coordinator.fileCount()
              + " data file(s)"
              + (snapshotId == null ? "" : " as snapshot " + snapshotId)
              + " to table "
              + coordinator.table().name());
    } catch (CommitStateUnknownException e) {
      // The catalog may have applied the commit: deleting the files could break the table.
      setErrors(getErrors() + 1);
      throw new HopException(
          "The commit to Iceberg table "
              + coordinator.table().name()
              + " may or may not have been applied. The data files are kept; check the table"
              + " history before running the pipeline again.",
          e);
    } catch (Exception e) {
      setErrors(getErrors() + 1);
      if (!coordinator.isPublished()) {
        coordinator.abort();
      }
      throw new HopException("Unable to commit to Iceberg table", e);
    } finally {
      if (pipeline != null) {
        pipeline.setRunning(false);
      }
    }
  }

  @Override
  public void dispose() {
    if (data.writer != null) {
      try (PluginClassLoader ignored = PluginClassLoader.activate()) {
        data.writer.abort();
      } catch (HopException e) {
        logError("Unable to clean up data files", e);
      }
      data.writer = null;
    }
    super.dispose();
  }
}
