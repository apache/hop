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

package org.apache.hop.airflow.launcher;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * A workflow of the project that becomes a DAG. Read with the JDK XML parser: listing the DAGs runs
 * every 30 seconds or so and must not start Hop.
 *
 * @param relativePath the path of the .hwf file, relative to the project, with forward slashes
 * @param dagId the DAG id
 * @param description the description of the workflow, or a reason why it could not be read
 * @param tasks one task per action, or an empty list for one task that runs the whole workflow
 * @param singleTaskReason why the workflow runs as one task, or null
 * @param version the version of the file, see {@link HopAirflowBundle#DAG_DOC_VERSION}, or null
 * @param settings the {@value HopAirflowBundle#DAG_METADATA_KEY} object of the DAG, or null for the
 *     DAG a workflow gets without one
 */
public record HopWorkflowFile(
    String relativePath,
    String dagId,
    String description,
    List<ActionTasks.ActionTask> tasks,
    String singleTaskReason,
    String version,
    HopDagSettings settings) {

  /** A workflow that runs as one task. */
  public HopWorkflowFile(String relativePath, String dagId, String description) {
    this(relativePath, dagId, description, List.of(), null, null, null);
  }

  /** A workflow without settings. */
  public HopWorkflowFile(
      String relativePath,
      String dagId,
      String description,
      List<ActionTasks.ActionTask> tasks,
      String singleTaskReason,
      String version) {
    this(relativePath, dagId, description, tasks, singleTaskReason, version, null);
  }

  /** The DAG of these settings: another DAG id, and the parameters and schedule of the object. */
  public HopWorkflowFile withSettings(String newDagId, HopDagSettings newSettings) {
    return new HopWorkflowFile(
        relativePath, newDagId, description, tasks, singleTaskReason, version, newSettings);
  }

  /**
   * @return the task of an action, or null when the task runs the whole workflow
   */
  public ActionTasks.ActionTask task(String taskId) {
    return tasks.stream().filter(task -> task.taskId().equals(taskId)).findFirst().orElse(null);
  }

  /** The tasks by task id. */
  public Map<String, ActionTasks.ActionTask> tasksById() {
    Map<String, ActionTasks.ActionTask> byId = new LinkedHashMap<>();
    tasks.forEach(task -> byId.put(task.taskId(), task));
    return byId;
  }

  /**
   * The workflows of the project that match one of the patterns, or all of them without patterns.
   * Hidden folders such as .git are skipped.
   */
  public static List<HopWorkflowFile> scan(Path project, List<String> patterns, String dagIdPrefix)
      throws IOException {
    return scan(project, patterns, dagIdPrefix, ActionTasks.NOT_EVALUATING);
  }

  /**
   * The workflows of the project that match one of the patterns, or all of them without patterns.
   * Hidden folders such as .git are skipped.
   *
   * @param notEvaluating the plugin ids of the actions that don't evaluate their result
   */
  public static List<HopWorkflowFile> scan(
      Path project, List<String> patterns, String dagIdPrefix, Set<String> notEvaluating)
      throws IOException {
    List<PathMatcher> matchers = new ArrayList<>();
    for (String pattern : patterns) {
      matchers.add(FileSystems.getDefault().getPathMatcher("glob:" + pattern));
    }
    List<Path> files = new ArrayList<>();
    Files.walkFileTree(
        project,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attributes) {
            boolean hidden = !dir.equals(project) && dir.getFileName().toString().startsWith(".");
            return hidden ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
            if (file.getFileName().toString().endsWith(".hwf")) {
              Path relative = project.relativize(file);
              if (matchers.isEmpty() || matchers.stream().anyMatch(m -> m.matches(relative))) {
                files.add(file);
              }
            }
            return FileVisitResult.CONTINUE;
          }
        });
    List<HopWorkflowFile> workflows = new ArrayList<>();
    for (Path file : files.stream().sorted().toList()) {
      String relative = project.relativize(file).toString().replace('\\', '/');
      workflows.add(read(file, relative, dagIdPrefix, notEvaluating));
    }
    return workflows;
  }

  static HopWorkflowFile read(Path file, String relative, String dagIdPrefix) {
    return read(file, relative, dagIdPrefix, ActionTasks.NOT_EVALUATING);
  }

  static HopWorkflowFile read(
      Path file, String relative, String dagIdPrefix, Set<String> notEvaluating) {
    String description;
    String version = null;
    List<ActionTasks.ActionTask> tasks = List.of();
    List<String> reasons = new ArrayList<>();
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
      factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      byte[] content = Files.readAllBytes(file);
      version = version(content);
      Document document = factory.newDocumentBuilder().parse(new ByteArrayInputStream(content));
      Element root = document.getDocumentElement();
      description = text(root, "description");
      Element actions = child(root, "actions");
      tasks = ActionTasks.of(actions, child(root, "hops"), reasons, notEvaluating);
    } catch (Exception e) {
      description = "Hop could not read this workflow: " + e.getMessage();
      reasons.add(description);
    }
    return new HopWorkflowFile(
        relative,
        dagId(dagIdPrefix, relative),
        description,
        tasks,
        reasons.isEmpty() ? null : String.join("; ", reasons),
        version);
  }

  /** See {@link HopAirflowBundle#DAG_DOC_VERSION}. */
  static String version(byte[] content) throws NoSuchAlgorithmException {
    byte[] hash = MessageDigest.getInstance("SHA-256").digest(content);
    return HexFormat.of().formatHex(hash).substring(0, HopAirflowBundle.VERSION_LENGTH);
  }

  /**
   * The DAG id of an Airflow DAG object with a DAG id of its own: the prefix and that id, with the
   * characters Airflow does not allow replaced.
   */
  public static String objectDagId(String prefix, String id) {
    return prefix + id.replaceAll("[^A-Za-z0-9_.-]", "_");
  }

  /** The prefix and the path without .hwf; folders become two underscores. */
  public static String dagId(String prefix, String relativePath) {
    String withoutExtension = relativePath.substring(0, relativePath.length() - ".hwf".length());
    return prefix + withoutExtension.replace("/", "__").replaceAll("[^A-Za-z0-9_.-]", "_");
  }

  private static Element child(Element parent, String tag) {
    for (Node node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
      if (node instanceof Element element && tag.equals(element.getTagName())) {
        return element;
      }
    }
    return null;
  }

  private static String text(Element parent, String tag) {
    Element element = child(parent, tag);
    return element == null ? "" : element.getTextContent().trim();
  }
}
