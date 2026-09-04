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

package org.apache.hop.lint;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

/**
 * What the files in a project refer to, so that a rule can ask a question no single file can
 * answer.
 *
 * <p>Whether a pipeline is ever called, or whether a connection is used anywhere, is not visible
 * from inside the file itself: it depends on every other file in the project. This index is built
 * once per project lint and answers those questions.
 *
 * <p>References are read through the {@code getResourceDependencies} methods on {@code
 * ITransformMeta} and {@code IAction}, which every transform and action implements, so the index
 * sees third-party plugins without knowing anything about them.
 */
public final class LintProjectIndex {

  private static final ILogChannel log = LogChannel.GENERAL;

  /** Absolute, normalised paths of files some other file in the project refers to. */
  private final Set<String> referencedFiles = new HashSet<>();

  /**
   * File names, without any folder, that some file refers to.
   *
   * <p>References are normally written through variables ({@code ${PROJECT_HOME}/pipelines/x.hpl}),
   * and the variables a lint run has are not always the ones a real run would have. When the full
   * path cannot be resolved, the file name alone still tells us the file is referred to somewhere.
   * That can miss a genuinely dead file which shares a name with a live one, which is the right way
   * round to be wrong: a linter that wrongly calls live code dead is one people switch off.
   */
  private final Set<String> referencedBaseNames = new HashSet<>();

  /** Names of database connections some file in the project refers to. */
  private final Set<String> referencedConnections = new HashSet<>();

  /** The lintable files the index was built from. */
  private final Set<String> indexedFiles = new LinkedHashSet<>();

  private LintProjectIndex() {}

  /** An index that knows nothing, for the paths where no project context is available. */
  public static LintProjectIndex empty() {
    return new LintProjectIndex();
  }

  /**
   * Build the index by reading every pipeline and workflow in the project.
   *
   * <p>A file which fails to parse is skipped rather than failing the build: it will be reported by
   * the ordinary per-file rules, and refusing to index the rest of the project because one file is
   * broken would turn one problem into many.
   *
   * @param files the pipeline and workflow files in the project
   * @param metadataProvider the provider used to load them
   * @param variables the variables used to resolve their references
   * @return the index
   */
  public static LintProjectIndex build(
      List<String> files, IHopMetadataProvider metadataProvider, IVariables variables) {
    LintProjectIndex index = new LintProjectIndex();
    if (files == null) {
      return index;
    }
    for (String path : files) {
      if (Utils.isEmpty(path)) {
        continue;
      }
      String normalised = LintPathUtils.normalizePath(path);
      index.indexedFiles.add(normalised);
      try {
        if (normalised.toLowerCase().endsWith(".hpl")) {
          index.addPipelineReferences(
              new PipelineMeta(path, metadataProvider, variables), path, variables);
        } else if (normalised.toLowerCase().endsWith(".hwf")) {
          index.addWorkflowReferences(
              new WorkflowMeta(variables, path, metadataProvider), path, variables);
        }
      } catch (Exception e) {
        log.logDetailed("Could not index references in " + path + ": " + e.getMessage());
      }
    }
    return index;
  }

  private void addPipelineReferences(PipelineMeta meta, String from, IVariables variables) {
    for (TransformMeta transformMeta : meta.getTransforms()) {
      if (transformMeta.getTransform() == null) {
        continue;
      }
      try {
        record(
            transformMeta.getTransform().getResourceDependencies(variables, transformMeta),
            from,
            variables);
      } catch (Exception e) {
        log.logDetailed(
            "Could not read references from transform '"
                + transformMeta.getName()
                + "' in "
                + from
                + ": "
                + e.getMessage());
      }
    }
  }

  private void addWorkflowReferences(WorkflowMeta meta, String from, IVariables variables) {
    for (ActionMeta actionMeta : meta.getActions()) {
      if (actionMeta.getAction() == null) {
        continue;
      }
      try {
        record(actionMeta.getAction().getResourceDependencies(variables, meta), from, variables);
      } catch (Exception e) {
        log.logDetailed(
            "Could not read references from action '"
                + actionMeta.getName()
                + "' in "
                + from
                + ": "
                + e.getMessage());
      }
    }
  }

  private void recordFile(String from, String resource, IVariables variables) {
    String resolved = variables == null ? resource : variables.resolve(resource);
    referencedFiles.add(resolveAgainst(from, resolved));
    String baseName = new File(resolved).getName();
    if (!Utils.isEmpty(baseName)) {
      referencedBaseNames.add(baseName.toLowerCase());
    }
  }

  private void record(List<ResourceReference> references, String from, IVariables variables) {
    if (references == null) {
      return;
    }
    for (ResourceReference reference : references) {
      if (reference == null || reference.getEntries() == null) {
        continue;
      }
      for (ResourceEntry entry : reference.getEntries()) {
        String resource = entry == null ? null : entry.getResource();
        if (Utils.isEmpty(resource)) {
          continue;
        }
        switch (entry.getResourcetype()) {
          case ACTIONFILE, FILE, URL -> recordFile(from, resource, variables);
          case CONNECTION, DATABASENAME -> referencedConnections.add(resource.trim());
          default -> {
            // SERVER and anything added later carry no meaning for the rules we answer here.
          }
        }
      }
    }
  }

  /**
   * A reference may be written relative to the file that makes it, so resolve it before comparing
   * with the absolute paths the project walk produced.
   */
  private static String resolveAgainst(String from, String resource) {
    String candidate = resource.trim();
    File asGiven = new File(candidate);
    if (!asGiven.isAbsolute()) {
      File parent = new File(from).getParentFile();
      if (parent != null) {
        candidate = new File(parent, candidate).getPath();
      }
    }
    return LintPathUtils.normalizePath(candidate);
  }

  /**
   * Whether any file in the project refers to this one.
   *
   * @param path the file to ask about
   * @return true when something refers to it
   */
  public boolean isFileReferenced(String path) {
    if (Utils.isEmpty(path)) {
      return false;
    }
    String normalised = LintPathUtils.normalizePath(path);
    if (referencedFiles.contains(normalised)) {
      return true;
    }
    if (referencedFiles.stream().anyMatch(known -> LintPathUtils.pathsMatch(known, normalised))) {
      return true;
    }
    // Last resort, the file name on its own: see referencedBaseNames for why.
    String baseName = new File(normalised).getName();
    return !Utils.isEmpty(baseName) && referencedBaseNames.contains(baseName.toLowerCase());
  }

  /**
   * Whether any file in the project refers to this database connection.
   *
   * @param connectionName the connection name to ask about
   * @return true when something refers to it
   */
  public boolean isConnectionReferenced(String connectionName) {
    if (Utils.isEmpty(connectionName)) {
      return false;
    }
    return referencedConnections.stream()
        .anyMatch(name -> name.equalsIgnoreCase(connectionName.trim()));
  }

  /** Whether the index holds anything, so callers can tell a real index from {@link #empty()}. */
  public boolean isPopulated() {
    return !indexedFiles.isEmpty();
  }

  public Set<String> getIndexedFiles() {
    return Collections.unmodifiableSet(indexedFiles);
  }
}
