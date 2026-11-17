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

package org.apache.hop.lineage.datahub;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.lineage.LineageDefinitions;
import org.apache.hop.lineage.hub.LineageSinkTarget;
import org.apache.hop.lineage.metadata.LineageSink;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.spi.LineageSinkHealth;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.workflow.WorkflowMeta;

/**
 * Publishes the pipelines and workflows of a folder to DataHub without running them: each as a flow
 * with its transforms or actions as tasks, the tables and files they declare as their inputs and
 * outputs, and the layouts of those files as dataset schemas. It writes through a DataHub {@link
 * LineageSink}, so the entities are the ones the runs report to.
 *
 * <p>With {@code includingDocumentation} set it also publishes what {@code hop doc} writes for each
 * pipeline and workflow as a DataHub document (DataHub 1.7 and later), related to its flow and
 * tasks, under a document of the project.
 */
public final class DataHubPublisher {

  /** How long to wait for DataHub to accept what was published. */
  static final long DELIVERY_TIMEOUT_MS = 120_000L;

  /** How long to wait for DataHub to take the first document, before the others are sent. */
  static final long FIRST_DOCUMENT_TIMEOUT_MS = 30_000L;

  /** The name of the project, set when a project is used. */
  static final String VAR_PROJECT_NAME = "HOP_PROJECT_NAME";

  /**
   * What a publication did.
   *
   * @param pipelines the pipelines published
   * @param workflows the workflows published
   * @param unreadable the files that could not be read, and were skipped
   * @param documentationNotPublished why the documentation was not published, or null
   * @param health how the delivery to DataHub went
   */
  public record Result(
      int pipelines,
      int workflows,
      List<String> unreadable,
      String documentationNotPublished,
      LineageSinkHealth health) {

    /** Whether everything was read and DataHub accepted all of it. */
    public boolean isSuccess() {
      return unreadable.isEmpty()
          && documentationNotPublished == null
          && health != null
          && health.waiting() == 0
          && health.rejected() == 0
          && health.dropped() == 0;
    }
  }

  private final IVariables variables;
  private final IHopMetadataProvider metadataProvider;
  private final ILogChannel log;

  /**
   * Whether to publish what {@code hop doc} writes for each pipeline and workflow as well, as
   * DataHub documents: DataHub 1.7 or later, an earlier version refuses them.
   */
  @Setter private boolean includingDocumentation;

  /** Whether the documentation lists the parameters, as {@code hop doc -ip} does. */
  @Setter private boolean includingParameters;

  /** Whether the documentation has the text of the notes, as {@code hop doc -in} does. */
  @Setter private boolean includingNotes;

  public DataHubPublisher(
      IVariables variables, IHopMetadataProvider metadataProvider, ILogChannel log) {
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.log = log;
  }

  /**
   * Publishes every pipeline and workflow in {@code folder} and its subfolders.
   *
   * @param sinkName the name of a lineage sink of type DataHub
   * @param folder the folder, variables allowed
   * @throws HopException when the sink can't be used, or the folder doesn't exist
   */
  public Result publish(String sinkName, String folder) throws HopException {
    DataHubLineageSink sink = startSink(sinkName);
    int pipelines = 0;
    int workflows = 0;
    List<String> unreadable = new ArrayList<>();
    List<LineageEvent> documents = new ArrayList<>();
    String documentationNotPublished = null;
    LineageSinkHealth health;
    try {
      FileObject root = folder(folder);
      DocumentationWriter documentation = includingDocumentation ? documentationWriter() : null;
      String project = variables.getVariable(VAR_PROJECT_NAME);
      for (FileObject file : hopFiles(root)) {
        String filename = HopVfs.getFilename(file);
        try {
          String relativeFile = root.getName().getRelativeName(file.getName());
          if (filename.endsWith(".hpl")) {
            PipelineMeta meta = new PipelineMeta(filename, metadataProvider, variables);
            List<LineageEvent> events =
                LineageDefinitions.ofPipeline(meta, variablesOf(meta, variables), log);
            if (documentation != null) {
              documents.add(documentation.pipeline(events.get(0), meta, relativeFile, project));
            }
            sink.accept(events);
            pipelines++;
          } else {
            WorkflowMeta meta = new WorkflowMeta(variables, filename, metadataProvider);
            List<LineageEvent> events =
                LineageDefinitions.ofWorkflow(meta, variablesOf(meta, variables));
            if (documentation != null) {
              documents.add(documentation.workflow(events.get(0), meta, relativeFile, project));
            }
            sink.accept(events);
            workflows++;
          }
          log.logDetailed("Published " + filename);
        } catch (Exception e) {
          unreadable.add(filename);
          log.logError("Unable to publish " + filename + ", it is skipped: " + e.getMessage());
        }
      }
      deliver(sink, DELIVERY_TIMEOUT_MS);
      documentationNotPublished = publishDocuments(sink, documents);
      health = sink.health();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HopException("Publishing to DataHub was interrupted", e);
    } finally {
      sink.shutdown();
    }
    return new Result(pipelines, workflows, unreadable, documentationNotPublished, health);
  }

  /**
   * Sends the documents after the pipelines and workflows are delivered, so a DataHub that refuses
   * documents (before 1.7) refuses only them. The first goes alone: when DataHub doesn't take it,
   * the others are not sent.
   *
   * @return why the documents were not published, or null when they were
   */
  private String publishDocuments(DataHubLineageSink sink, List<LineageEvent> documents)
      throws HopException, InterruptedException {
    if (documents.isEmpty()) {
      return null;
    }
    LineageSinkHealth before = sink.health();
    if (before.waiting() > 0) {
      return notPublished(
          "The documentation is not published: DataHub did not receive all pipelines and workflows"
              + " in time");
    }
    sink.accept(documents.subList(0, 1));
    deliver(sink, FIRST_DOCUMENT_TIMEOUT_MS);
    LineageSinkHealth after = sink.health();
    String error = after.lastError() == null ? "" : ": " + after.lastError();
    if (after.rejected() > before.rejected() || after.dropped() > before.dropped()) {
      return notPublished(
          "DataHub refused the documentation, which needs DataHub 1.7 or later; it is not"
              + " published"
              + error);
    }
    if (after.waiting() > 0) {
      return notPublished(
          "The documentation is not published: DataHub did not take the first document in time"
              + error);
    }
    sink.accept(documents.subList(1, documents.size()));
    deliver(sink, DELIVERY_TIMEOUT_MS);
    return null;
  }

  private String notPublished(String why) {
    log.logError(why);
    return why;
  }

  /**
   * Waits until DataHub took everything sent, or the time is up. The sink stops waiting while it
   * pauses before trying again after a failure; this waits through those pauses.
   */
  private static void deliver(DataHubLineageSink sink, long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (true) {
      long left = deadline - System.currentTimeMillis();
      if (left <= 0) {
        return;
      }
      sink.awaitDelivery(left);
      if (sink.health().waiting() == 0 || System.currentTimeMillis() >= deadline) {
        return;
      }
      Thread.sleep(Math.min(200L, Math.max(1L, deadline - System.currentTimeMillis())));
    }
  }

  /**
   * The variables a run of a pipeline or workflow resolves its names with, as far as they are known
   * without running it: those it is published with, its own internal variables (its folder is
   * {@code Internal.Entry.Current.Folder}) and its parameters, set to their defaults as a run
   * without parameter values sets them.
   */
  static IVariables variablesOf(AbstractMeta meta, IVariables parent) {
    IVariables own = new Variables();
    own.initializeFrom(parent);
    meta.setInternalHopVariables(own);
    for (String parameter : meta.listParameters()) {
      try {
        own.setVariable(parameter, Const.NVL(meta.getParameterDefault(parameter), ""));
      } catch (UnknownParamException e) {
        // Listed, so known
      }
    }
    return own;
  }

  /** A DataHub sink that writes definitions only, from the settings of a lineage sink. */
  private DataHubLineageSink startSink(String sinkName) throws HopException {
    String name = variables.resolve(sinkName);
    if (Utils.isEmpty(name)) {
      throw new HopException("Name the DataHub lineage sink to publish to");
    }
    LineageSink lineageSink = metadataProvider.getSerializer(LineageSink.class).load(name);
    if (lineageSink == null) {
      throw new HopException("Lineage sink '" + name + "' does not exist");
    }
    if (!(lineageSink.getSink() instanceof DataHubSinkConfiguration)) {
      throw new HopException("Lineage sink '" + name + "' is not a DataHub sink");
    }
    LineageSinkTarget target = lineageSink.toTarget(variables, metadataProvider);
    Variables settings = new Variables();
    target.settings().forEach(settings::setVariable);
    // Delivered before this returns, so no spool: and not the folder the runs of this project use
    settings.setVariable(DataHubSinkConfiguration.DATAHUB_PREFIX + "SPOOL_DIR", "");
    DataHubLineageSink sink = new DataHubLineageSink();
    sink.setDefinitionsOnly(true);
    sink.init(settings, log);
    return sink;
  }

  private DocumentationWriter documentationWriter() throws HopException {
    try {
      return new DocumentationWriter(variables, log, includingParameters, includingNotes);
    } catch (LinkageError e) {
      throw new HopException(
          "Publishing documentation needs the documentation plugin (hop doc), which is not"
              + " installed",
          e);
    }
  }

  /** The folder to publish, which must exist. */
  private FileObject folder(String folder) throws HopException {
    String path = variables.resolve(folder);
    try {
      FileObject root = HopVfs.getFileObject(path, variables);
      if (!root.exists() || root.getType() != FileType.FOLDER) {
        throw new HopException("Folder '" + path + "' does not exist");
      }
      return root;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to open folder '" + path + "'", e);
    }
  }

  /** The pipelines and workflows in a folder and its subfolders, in a stable order. */
  private List<FileObject> hopFiles(FileObject root) throws HopException {
    try {
      FileObject[] found =
          root.findFiles(
              new FileSelector() {
                @Override
                public boolean includeFile(FileSelectInfo info) {
                  String extension =
                      info.getFile().getName().getExtension().toLowerCase(Locale.ROOT);
                  return extension.equals("hpl") || extension.equals("hwf");
                }

                @Override
                public boolean traverseDescendents(FileSelectInfo info) {
                  // Not the metadata, nor hidden folders such as .git
                  String base = info.getFile().getName().getBaseName();
                  return info.getDepth() == 0
                      || (!base.startsWith(".") && !base.equals("metadata"));
                }
              });
      List<FileObject> files = new ArrayList<>(found == null ? List.of() : List.of(found));
      files.sort((a, b) -> a.getName().getPath().compareTo(b.getName().getPath()));
      return files;
    } catch (Exception e) {
      throw new HopException(
          "Unable to list the pipelines and workflows in '" + HopVfs.getFilename(root) + "'", e);
    }
  }
}
