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
package org.apache.hop.imp;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.UnaryOperator;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.util.HopMetadataPropertyWalker;
import org.apache.hop.metadata.util.HopMetadataPropertyWalker.StringProperty;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

/**
 * After a Kettle import has written pipelines, workflows and relational connections, collapse
 * case-insensitive connection names (and optionally a naming-scheme mapping) onto one Hop name and
 * rewrite every {@link HopMetadataPropertyType#RDBMS_CONNECTION} field to match.
 */
public final class ImportedConnectionRewriter {

  private ImportedConnectionRewriter() {}

  @Getter
  public static final class Result {
    private int connectionsRenamed;
    private int filesRewritten;
    private int fieldsRewritten;
    private ConnectionNameMap nameMap = ConnectionNameMap.empty();
  }

  /**
   * Collect names, rename {@link DatabaseMeta} objects, and rewrite imported {@code .hpl}/{@code
   * .hwf} files. Failures to load a single file are logged and skipped.
   */
  public static Result rewrite(HopImportBase hopImport) throws HopException {
    Result result = new Result();
    if (hopImport == null) {
      return result;
    }
    IHopMetadataProvider provider = hopImport.getMetadataProvider();
    IVariables variables = hopImport.getVariables();
    ILogChannel log = hopImport.getLog();
    if (provider == null) {
      return result;
    }

    List<String> names = new ArrayList<>();
    if (hopImport.getConnectionsList() != null) {
      for (DatabaseMeta databaseMeta : hopImport.getConnectionsList()) {
        if (databaseMeta != null && !StringUtils.isEmpty(databaseMeta.getName())) {
          names.add(databaseMeta.getName());
        }
      }
    }
    collectReferenceNames(hopImport, names, log);

    UnaryOperator<String> mapper = hopImport.getConnectionNameMapper();
    ConnectionNameMap nameMap =
        ConnectionNameMap.build(names, hopImport.getSharedConnectionNames(), mapper);
    result.nameMap = nameMap;
    if (nameMap.isEmpty() || nameMap.changedCount() == 0) {
      return result;
    }

    result.connectionsRenamed = renameConnections(hopImport, nameMap, log);
    RewriteCounts counts = rewriteWrittenFiles(hopImport, nameMap, provider, variables, log);
    result.filesRewritten = counts.files;
    result.fieldsRewritten = counts.fields;
    return result;
  }

  public static int rewriteObject(Object root, ConnectionNameMap nameMap) {
    if (root == null || nameMap == null) {
      return 0;
    }
    return HopMetadataPropertyWalker.rewriteStrings(
        root, HopMetadataPropertyType.RDBMS_CONNECTION, nameMap::targetFor);
  }

  private static void collectReferenceNames(
      HopImportBase hopImport, List<String> names, ILogChannel log) {
    IHopMetadataProvider provider = hopImport.getMetadataProvider();
    IVariables variables = hopImport.getVariables();
    for (String filename : hopImport.getWrittenHopFileNames()) {
      try {
        Object graph = loadGraph(filename, provider, variables);
        if (graph == null) {
          continue;
        }
        walkGraph(
            graph,
            node -> {
              for (StringProperty property :
                  HopMetadataPropertyWalker.collectStrings(
                      node, HopMetadataPropertyType.RDBMS_CONNECTION)) {
                names.add(property.value());
              }
            });
      } catch (Exception e) {
        if (log != null) {
          log.logError("Unable to scan connection names in imported file " + filename, e);
        }
      }
    }
  }

  private static int renameConnections(
      HopImportBase hopImport, ConnectionNameMap nameMap, ILogChannel log) throws HopException {
    IHopMetadataSerializer<DatabaseMeta> serializer =
        hopImport.getMetadataProvider().getSerializer(DatabaseMeta.class);
    int renamed = 0;
    List<DatabaseMeta> connections = hopImport.getConnectionsList();
    if (connections == null) {
      return 0;
    }
    for (DatabaseMeta databaseMeta : connections) {
      if (databaseMeta == null || StringUtils.isEmpty(databaseMeta.getName())) {
        continue;
      }
      String oldName = databaseMeta.getName();
      String newName = nameMap.targetFor(oldName);
      if (StringUtils.isEmpty(newName) || newName.equals(oldName)) {
        continue;
      }
      boolean deletedOld = false;
      try {
        if (serializer.exists(newName) && !oldName.equalsIgnoreCase(newName)) {
          if (log != null) {
            log.logError(
                "Cannot rename connection '"
                    + oldName
                    + "' to '"
                    + newName
                    + "': that name already exists");
          }
          continue;
        }
        // Delete first so a case-only rename works on case-insensitive filesystems.
        if (serializer.exists(oldName)) {
          serializer.delete(oldName);
          deletedOld = true;
        }
        databaseMeta.setName(newName);
        serializer.save(databaseMeta);
        renamed++;
        if (log != null) {
          log.logBasic("Renamed imported connection '" + oldName + "' to '" + newName + "'");
        }
      } catch (Exception e) {
        databaseMeta.setName(oldName);
        if (deletedOld) {
          try {
            serializer.save(databaseMeta);
          } catch (Exception ignored) {
            // already logging the rename failure
          }
        }
        if (log != null) {
          log.logError(
              "Error renaming imported connection '" + oldName + "' to '" + newName + "'", e);
        }
      }
    }
    return renamed;
  }

  private static RewriteCounts rewriteWrittenFiles(
      HopImportBase hopImport,
      ConnectionNameMap nameMap,
      IHopMetadataProvider provider,
      IVariables variables,
      ILogChannel log) {
    RewriteCounts counts = new RewriteCounts();
    for (String filename : hopImport.getWrittenHopFileNames()) {
      try {
        Object graph = loadGraph(filename, provider, variables);
        if (graph == null) {
          continue;
        }
        int[] changed = new int[1];
        walkGraph(graph, node -> changed[0] += rewriteObject(node, nameMap));
        if (changed[0] == 0) {
          continue;
        }
        saveGraph(filename, graph, variables);
        counts.files++;
        counts.fields += changed[0];
        if (log != null) {
          log.logBasic(
              "Updated " + changed[0] + " relational connection reference(s) in " + filename);
        }
      } catch (Exception e) {
        if (log != null) {
          log.logError("Unable to rewrite connection names in imported file " + filename, e);
        }
      }
    }
    return counts;
  }

  private static Object loadGraph(
      String filename, IHopMetadataProvider provider, IVariables variables) throws HopException {
    if (StringUtils.isEmpty(filename)) {
      return null;
    }
    String lower = filename.toLowerCase(Locale.ROOT);
    if (lower.endsWith(".hpl")) {
      return new PipelineMeta(filename, provider, variables);
    }
    if (lower.endsWith(".hwf")) {
      return new WorkflowMeta(variables, filename, provider);
    }
    return null;
  }

  private static void saveGraph(String filename, Object graph, IVariables variables)
      throws HopException {
    String xml;
    if (graph instanceof PipelineMeta pipelineMeta) {
      xml = pipelineMeta.getXml(variables);
    } else if (graph instanceof WorkflowMeta workflowMeta) {
      xml = workflowMeta.getXml(variables);
    } else {
      return;
    }
    try (OutputStream out = HopVfs.getOutputStream(filename, false)) {
      out.write(xml.getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new HopException("Error writing imported file " + filename, e);
    }
  }

  private static void walkGraph(Object graph, java.util.function.Consumer<Object> consumer) {
    if (graph instanceof PipelineMeta pipelineMeta) {
      for (TransformMeta transformMeta : pipelineMeta.getTransforms()) {
        if (transformMeta != null && transformMeta.getTransform() != null) {
          consumer.accept(transformMeta.getTransform());
        }
      }
      return;
    }
    if (graph instanceof WorkflowMeta workflowMeta) {
      for (ActionMeta actionMeta : workflowMeta.getActions()) {
        if (actionMeta != null && actionMeta.getAction() != null) {
          consumer.accept(actionMeta.getAction());
        }
      }
    }
  }

  private static final class RewriteCounts {
    private int files;
    private int fields;
  }
}
