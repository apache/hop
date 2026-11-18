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

package org.apache.hop.datacatalog;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.LineageDefinitions;
import org.apache.hop.lineage.LineageFileIdentity;
import org.apache.hop.lineage.LineageRelationalIdentity;
import org.apache.hop.lineage.LineageSqlTables;
import org.apache.hop.lineage.model.FileIoLineagePayload;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.LineagePayload;
import org.apache.hop.lineage.model.RelationalIoLineagePayload;
import org.apache.hop.lineage.model.RelationalTable;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * The tables and files a pipeline declares it reads and writes, named the way lineage names them,
 * so a catalog finds the datasets the lineage sinks write. Nothing runs and no database is asked
 * for anything. Experimental.
 */
public final class PipelineDatasets {

  /**
   * A dataset a pipeline touches.
   *
   * @param dataset how a catalog finds it
   * @param readBy the transforms that read it
   * @param writtenBy the transforms that write it
   */
  public record TouchedDataset(DatasetRef dataset, Set<String> readBy, Set<String> writtenBy) {}

  private PipelineDatasets() {}

  /**
   * The datasets the transforms of a pipeline declare, each once, in the order of the transforms. A
   * transform whose declaration can't be resolved (a connection that doesn't exist, a variable that
   * isn't set) adds nothing.
   */
  public static List<TouchedDataset> of(PipelineMeta meta, IVariables variables) {
    Map<DatasetRef, TouchedDataset> found = new LinkedHashMap<>();
    for (TransformMeta transform : meta.getTransforms()) {
      List<LineagePayload> declared;
      try {
        declared = LineageDefinitions.declaredIo(meta, transform, variables, false);
      } catch (Exception e) {
        continue;
      }
      for (LineagePayload payload : declared) {
        if (payload instanceof FileIoLineagePayload file) {
          boolean read = file.getOperation() == FileIoOperation.READ;
          LineageFileIdentity.Identity identity =
              LineageFileIdentity.of(read ? file.getSourceUri() : file.getTargetUri());
          if (identity != null) {
            add(found, DatasetRef.of(identity.namespace(), identity.name()), transform, read);
          }
        } else if (payload instanceof RelationalIoLineagePayload relational
            && !Utils.isEmpty(relational.getDatasetNamespace())) {
          List<RelationalTable> inputs = relational.getInputs();
          List<RelationalTable> outputs = relational.getOutputs();
          if (inputs.isEmpty() && outputs.isEmpty() && !Utils.isEmpty(relational.getSqlText())) {
            // Table input: the tables are in its SQL, found as the sinks find them
            LineageSqlTables.Tables parsed =
                LineageSqlTables.parse(relational.getSqlText(), relational.getDatasetNamespace());
            inputs = parsed.inputs();
            outputs = parsed.outputs();
          }
          addTables(found, relational, inputs, transform, true);
          addTables(found, relational, outputs, transform, false);
        }
      }
    }
    return new ArrayList<>(found.values());
  }

  private static void addTables(
      Map<DatasetRef, TouchedDataset> found,
      RelationalIoLineagePayload relational,
      List<RelationalTable> tables,
      TransformMeta transform,
      boolean read) {
    for (RelationalTable table : tables) {
      String name =
          LineageRelationalIdentity.datasetName(
              LineageRelationalIdentity.withDefaults(
                  table, relational.getDefaultCatalog(), relational.getDefaultSchema()));
      if (!Utils.isEmpty(name)) {
        add(found, DatasetRef.of(relational.getDatasetNamespace(), name), transform, read);
      }
    }
  }

  private static void add(
      Map<DatasetRef, TouchedDataset> found,
      DatasetRef dataset,
      TransformMeta transform,
      boolean read) {
    if (!dataset.isComplete()) {
      return;
    }
    TouchedDataset touched =
        found.computeIfAbsent(
            dataset, d -> new TouchedDataset(d, new LinkedHashSet<>(), new LinkedHashSet<>()));
    (read ? touched.readBy() : touched.writtenBy()).add(transform.getName());
  }
}
