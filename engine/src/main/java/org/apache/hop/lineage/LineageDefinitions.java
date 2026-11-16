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

package org.apache.hop.lineage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.lineage.api.IDeclaresLineage;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.model.FileIoLineagePayload;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.LineagePayload;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload.Transition;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.apache.hop.lineage.model.RelationalLifecycle;
import org.apache.hop.lineage.model.RelationalTable;
import org.apache.hop.lineage.model.RelationalWriteColumn;
import org.apache.hop.lineage.model.RunLifecycleLineagePayload;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

/**
 * The lineage a pipeline or workflow declares, as the events a run of it would emit, without
 * running it: the hop graph, the tables its transforms declare ({@link
 * org.apache.hop.lineage.api.RelationalLineage}), and the files and SQL they declare ({@link
 * IDeclaresLineage}). A sink that writes a catalog turns them into the same pipelines, tasks and
 * datasets a run would, so publishing a project and running it describe the same entities.
 *
 * <p>Every event carries {@link #ATTR_DEFINITION}: the run in them is made up, a sink must not
 * record it as a run. A transform whose declaration can't be resolved (an unknown connection, a
 * variable that isn't set) is published without that dataset rather than failing the rest.
 */
public final class LineageDefinitions {

  /** Set to {@code true} on the events of a definition: their run is not a real run. */
  public static final String ATTR_DEFINITION = "lineageDefinition";

  private LineageDefinitions() {}

  /** The events a run of this pipeline would emit about what it is, reads and writes. */
  public static List<LineageEvent> ofPipeline(
      PipelineMeta meta, IVariables variables, ILogChannel log) {
    List<LineageEvent> events = new ArrayList<>();
    String runId = UUID.randomUUID().toString();
    String filename = meta.getFilename();

    LineageContext.Builder pipeline =
        definition(LineageSubjectType.PIPELINE, runId).pipelineName(meta.getName());
    LineageRunLifecycleEmitter.fillFilenameFields(pipeline, filename, variables);
    LineageRunLifecycleEmitter.fillDescription(pipeline, meta.getDescription());
    pipeline.putAttribute("errors", "0");
    LineageContext started = pipeline.build();
    events.add(lifecycle(started, RunLifecyclePhase.STARTED));
    events.add(
        LineageEvent.of(
            LineageEventKind.PIPELINE_GRAPH,
            started,
            new PipelineGraphLineagePayload(LineageRunLifecycleEmitter.pipelineTransitions(meta))));

    for (TransformMeta transformMeta : inGraphOrder(meta)) {
      LineageContext.Builder transform =
          definition(LineageSubjectType.TRANSFORM, UUID.randomUUID().toString())
              .pipelineName(meta.getName())
              .transformName(transformMeta.getName())
              .copyNr("0");
      LineageRunLifecycleEmitter.fillFilenameFields(transform, filename, variables);
      LineageRunLifecycleEmitter.fillDescription(transform, transformMeta.getDescription());
      transform.putAttribute("pipelineLogChannelId", runId);
      transform.putAttribute("transformPluginId", transformMeta.getTransformPluginId());
      LineageTransformLogic.put(transform, transformMeta, variables);
      LineageContext context = transform.build();
      events.add(lifecycle(context, RunLifecyclePhase.STARTED));
      try {
        for (LineagePayload io : declaredIo(meta, transformMeta, variables)) {
          events.add(
              LineageEvent.of(
                  io instanceof FileIoLineagePayload
                      ? LineageEventKind.FILE_IO
                      : LineageEventKind.RELATIONAL_IO,
                  context,
                  io));
        }
      } catch (Exception e) {
        if (log != null) {
          log.logError(
              "Unable to read what transform '"
                  + transformMeta.getName()
                  + "' of "
                  + Const.NVL(filename, meta.getName())
                  + " reads and writes; it is published without it: "
                  + e.getMessage());
        }
      }
      events.add(finished(transform, RunLifecyclePhase.FINISHED));
    }

    pipeline.putAttribute(LineageRunLifecycleEmitter.OUTCOME, LineageRunLifecycleEmitter.SUCCESS);
    events.add(lifecycle(pipeline.build(), RunLifecyclePhase.FINISHED));
    return events;
  }

  /** The events a run of this workflow would emit about what it is and its actions. */
  public static List<LineageEvent> ofWorkflow(WorkflowMeta meta, IVariables variables) {
    List<LineageEvent> events = new ArrayList<>();
    String runId = UUID.randomUUID().toString();
    String filename = meta.getFilename();

    LineageContext.Builder workflow =
        definition(LineageSubjectType.WORKFLOW, runId).workflowName(meta.getName());
    LineageRunLifecycleEmitter.fillFilenameFields(workflow, filename, variables);
    LineageRunLifecycleEmitter.fillDescription(workflow, meta.getDescription());
    workflow.putAttribute("errors", "0");
    LineageContext started = workflow.build();
    events.add(lifecycle(started, RunLifecyclePhase.STARTED));
    events.add(
        LineageEvent.of(
            LineageEventKind.WORKFLOW_GRAPH,
            started,
            new PipelineGraphLineagePayload(LineageRunLifecycleEmitter.workflowTransitions(meta))));

    for (ActionMeta actionMeta : meta.getActions()) {
      LineageContext.Builder action =
          definition(LineageSubjectType.ACTION, UUID.randomUUID().toString())
              .workflowName(meta.getName())
              .actionName(actionMeta.getName());
      LineageRunLifecycleEmitter.fillFilenameFields(action, filename, variables);
      LineageRunLifecycleEmitter.fillDescription(action, actionMeta.getDescription());
      action.putAttribute("workflowLogChannelId", runId);
      if (actionMeta.getAction() != null) {
        action.putAttribute("actionPluginId", actionMeta.getAction().getPluginId());
      }
      events.add(lifecycle(action.build(), RunLifecyclePhase.STARTED));
      events.add(finished(action, RunLifecyclePhase.FINISHED));
    }

    workflow.putAttribute(LineageRunLifecycleEmitter.OUTCOME, LineageRunLifecycleEmitter.SUCCESS);
    workflow.putAttribute("result", "true");
    events.add(lifecycle(workflow.build(), RunLifecyclePhase.FINISHED));
    return events;
  }

  /**
   * What a transform declares it reads and writes, as event payloads: its tables, and its files and
   * SQL. Empty when it declares nothing that can be resolved without running.
   */
  public static List<LineagePayload> declaredIo(
      PipelineMeta meta, TransformMeta transformMeta, IVariables variables) throws Exception {
    return declaredIo(meta, transformMeta, variables, true);
  }

  /**
   * What a transform declares it reads and writes, as {@link #declaredIo(PipelineMeta,
   * TransformMeta, IVariables)} does.
   *
   * @param withFields whether to work out the fields that reach the transform, for the columns of
   *     the datasets. Without them only the tables and files are named, and nothing upstream is
   *     asked for its fields, which can mean a database query.
   */
  public static List<LineagePayload> declaredIo(
      PipelineMeta meta, TransformMeta transformMeta, IVariables variables, boolean withFields)
      throws Exception {
    List<LineagePayload> payloads = new ArrayList<>();
    IRowMeta inputRowMeta = withFields ? null : new RowMeta();

    LineageMetadataWalker.Declaration table =
        LineageMetadataWalker.read(transformMeta.getTransform());
    if (table != null && table.isUsable()) {
      DatabaseMeta databaseMeta =
          meta.findDatabase(variables.resolve(table.connectionName()), variables);
      if (databaseMeta != null) {
        if (inputRowMeta == null) {
          inputRowMeta = inputFields(meta, transformMeta, variables);
        }
        payloads.add(declaredTable(databaseMeta, variables, table, inputRowMeta));
      }
    }

    if (transformMeta.getTransform() instanceof IDeclaresLineage declaring) {
      if (inputRowMeta == null) {
        inputRowMeta = inputFields(meta, transformMeta, variables);
      }
      for (IDeclaresLineage.Declaration declaration :
          declaring.declaredLineage(variables, inputRowMeta)) {
        switch (declaration) {
          case IDeclaresLineage.DeclaredFile file -> {
            if (!Utils.isEmpty(file.filename())) {
              String uri =
                  LineageVfsIoSupport.uriString(
                      HopVfs.getFileObject(variables.resolve(file.filename()), variables));
              boolean read = file.operation() == FileIoOperation.READ;
              payloads.add(
                  new FileIoLineagePayload(
                      file.operation(),
                      read ? uri : null,
                      read ? null : uri,
                      null,
                      true,
                      null,
                      file.content()));
            }
          }
          case IDeclaresLineage.DeclaredSql sql -> {
            DatabaseMeta databaseMeta =
                meta.findDatabase(variables.resolve(sql.connectionName()), variables);
            if (databaseMeta != null && !Utils.isEmpty(sql.sql())) {
              payloads.add(
                  LineageRelationalIoEmitter.payload(
                      databaseMeta,
                      variables,
                      sql.operation(),
                      sql.sql(),
                      List.of(),
                      List.of(),
                      null,
                      null,
                      null,
                      true,
                      null));
            }
          }
        }
      }
    }
    return payloads;
  }

  /**
   * The fields that reach a transform, or none when they can't be worked out without running: a
   * Table input upstream asks its database for them, which a publication must not depend on. The
   * datasets are published without their columns then.
   */
  private static IRowMeta inputFields(
      PipelineMeta meta, TransformMeta transformMeta, IVariables variables) {
    try {
      return meta.getPrevTransformFields(variables, transformMeta);
    } catch (Exception e) {
      return new RowMeta();
    }
  }

  /** A table a transform declares with {@link org.apache.hop.lineage.api.RelationalLineage}. */
  private static LineagePayload declaredTable(
      DatabaseMeta databaseMeta,
      IVariables variables,
      LineageMetadataWalker.Declaration declared,
      IRowMeta inputRowMeta) {
    RelationalTable table =
        new RelationalTable(
            variables.resolve(databaseMeta.getDatabaseName()),
            variables.resolve(declared.schemaName()),
            variables.resolve(declared.tableName()));
    if (declared.operation() == RelationalIoOperation.DELETE) {
      return LineageRelationalIoEmitter.payload(
          databaseMeta,
          variables,
          RelationalIoOperation.DELETE,
          null,
          List.of(),
          List.of(table),
          null,
          null,
          null,
          true,
          null);
    }
    List<RelationalWriteColumn> columns = new ArrayList<>();
    for (LineageMetadataWalker.ColumnMapping mapping : declared.columns()) {
      LineageRelationalIoEmitter.addWriteColumn(
          columns,
          inputRowMeta,
          variables.resolve(mapping.targetColumn()),
          variables.resolve(mapping.streamField()));
    }
    if (columns.isEmpty()) {
      columns = LineageRelationalIoEmitter.writeColumnsFromRow(inputRowMeta);
    }
    return LineageRelationalIoEmitter.payload(
        databaseMeta,
        variables,
        RelationalIoOperation.WRITE,
        null,
        List.of(),
        List.of(table),
        LineageRelationalIoEmitter.toContentSchema(inputRowMeta),
        columns,
        declared.overwrite() ? RelationalLifecycle.OVERWRITE : null,
        true,
        null);
  }

  /**
   * The transforms upstream first, as a run finishes them: a sink that carries datasets along the
   * hops needs a transform's inputs before it describes the transform. Transforms in a loop, or not
   * reached from a start, follow in pipeline order.
   */
  static List<TransformMeta> inGraphOrder(PipelineMeta meta) {
    Map<String, TransformMeta> byName = new LinkedHashMap<>();
    for (TransformMeta transform : meta.getTransforms()) {
      byName.put(transform.getName(), transform);
    }
    Map<String, Integer> incoming = new LinkedHashMap<>();
    byName.keySet().forEach(name -> incoming.put(name, 0));
    List<Transition> transitions = LineageRunLifecycleEmitter.pipelineTransitions(meta);
    for (Transition transition : transitions) {
      incoming.computeIfPresent(transition.getTo(), (k, n) -> n + 1);
    }
    List<TransformMeta> ordered = new ArrayList<>();
    List<String> ready = new ArrayList<>();
    incoming.forEach(
        (name, n) -> {
          if (n == 0) {
            ready.add(name);
          }
        });
    while (!ready.isEmpty()) {
      String name = ready.remove(0);
      ordered.add(byName.get(name));
      for (Transition transition : transitions) {
        if (transition.getFrom().equals(name)
            && incoming.computeIfPresent(transition.getTo(), (k, n) -> n - 1) == 0) {
          ready.add(transition.getTo());
        }
      }
    }
    for (TransformMeta transform : byName.values()) {
      if (!ordered.contains(transform)) {
        ordered.add(transform);
      }
    }
    return ordered;
  }

  private static LineageContext.Builder definition(LineageSubjectType type, String runId) {
    return LineageContext.builder()
        .subjectType(type)
        .logChannelId(runId)
        .putAttribute(ATTR_DEFINITION, "true");
  }

  private static LineageEvent lifecycle(LineageContext context, RunLifecyclePhase phase) {
    return LineageEvent.of(
        LineageEventKind.RUN_LIFECYCLE, context, new RunLifecycleLineagePayload(phase, null));
  }

  private static LineageEvent finished(LineageContext.Builder builder, RunLifecyclePhase phase) {
    builder.putAttribute(LineageRunLifecycleEmitter.OUTCOME, LineageRunLifecycleEmitter.SUCCESS);
    return lifecycle(builder.build(), phase);
  }
}
