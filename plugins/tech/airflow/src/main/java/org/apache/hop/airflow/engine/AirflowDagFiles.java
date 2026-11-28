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
package org.apache.hop.airflow.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.airflow.launcher.HopAirflowBundle;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;

/**
 * The {@value HopAirflowBundle#DAG_METADATA_KEY} objects of a published bundle, as the launcher on
 * the workers reads them. Hop never loads the launcher, so this repeats its rules: the metadata
 * folder of the project, the workflow path, the version of the object. A test checks that both read
 * the same.
 */
public final class AirflowDagFiles {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String PROJECT_HOME = "${PROJECT_HOME}";

  private AirflowDagFiles() {}

  /**
   * One published object.
   *
   * @param name the name of the object
   * @param workflow the workflow, relative to the project, or null when the launcher can't use it
   * @param dagId the DAG id after the prefix, or null for the default DAG id of the workflow
   * @param tasks the code of its tasks, or null for the tasks of the bundle
   * @param version the version the launcher writes after {@link HopAirflowBundle#DAG_DOC_SETTINGS}
   * @param parameters the names of its parameters
   */
  public record PublishedDag(
      String name,
      String workflow,
      String dagId,
      String tasks,
      String version,
      Set<String> parameters) {}

  /** The objects of the bundle, sorted by file name as the launcher reads them. */
  public static List<PublishedDag> read(String bundleFolder) throws HopException {
    return read(HopVfs.getFileObject(bundleFolder));
  }

  /** The objects of a project or bundle folder, sorted by file name as the launcher reads them. */
  public static List<PublishedDag> read(FileObject project) throws HopException {
    List<PublishedDag> dags = new ArrayList<>();
    try {
      FileObject metadata = metadataFolder(project);
      if (metadata == null) {
        return dags;
      }
      FileObject folder = metadata.resolveFile(HopAirflowBundle.DAG_METADATA_KEY);
      if (!folder.isFolder()) {
        return dags;
      }
      FileObject[] children = folder.getChildren();
      Arrays.sort(children, Comparator.comparing(child -> child.getName().getBaseName()));
      for (FileObject child : children) {
        String baseName = child.getName().getBaseName();
        if (!child.isFile() || !baseName.endsWith(".json")) {
          continue;
        }
        byte[] content;
        try (InputStream in = HopVfs.getInputStream(child)) {
          content = in.readAllBytes();
        }
        JsonNode node;
        try {
          node = JSON.readTree(content);
        } catch (Exception e) {
          // The launcher fails the DAG of this object
          continue;
        }
        String name = text(node, "name");
        if (name == null) {
          name = baseName.substring(0, baseName.length() - ".json".length());
        }
        String workflow = text(node, "workflow");
        workflow = workflow == null ? null : projectRelative(workflow);
        Set<String> parameters = new LinkedHashSet<>();
        for (JsonNode parameter : node.path("parameters")) {
          String parameterName = text(parameter, "name");
          if (parameterName != null) {
            parameters.add(parameterName);
          }
        }
        String tasks = text(node, "tasks");
        dags.add(
            new PublishedDag(
                name,
                workflow,
                text(node, "dag_id"),
                HopAirflowBundle.TASKS_BUNDLE_DEFAULT.equals(tasks) ? null : tasks,
                version(content),
                parameters));
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          "Unable to read the Airflow DAGs of " + project.getName().getFriendlyURI(), e);
    }
    return dags;
  }

  /**
   * The metadata folder of the project: {@code metadataBaseFolder} of {@code project-config.json}
   * when it is in the project, else {@code metadata}. Null for a folder outside the project.
   */
  static FileObject metadataFolder(FileObject project) throws Exception {
    FileObject configFile = project.resolveFile("project-config.json");
    String folder = PROJECT_HOME + "/metadata";
    if (configFile.isFile()) {
      try (InputStream in = HopVfs.getInputStream(configFile)) {
        folder = JSON.readTree(in).path("metadataBaseFolder").asText(folder);
      }
    }
    String relative = projectRelative(folder);
    if (relative == null) {
      return null;
    }
    return relative.isEmpty() ? project : project.resolveFile(AirflowDagIds.encode(relative));
  }

  /**
   * A path in the project, relative to it with forward slashes: as it is, or after {@value
   * #PROJECT_HOME}. Empty for the project itself; null for any other variable, an absolute path or
   * a path that leaves the project.
   */
  static String projectRelative(String path) {
    String relative = path.trim().replace('\\', '/');
    if (relative.startsWith(PROJECT_HOME)) {
      relative = relative.substring(PROJECT_HOME.length());
      if (!relative.isEmpty() && !relative.startsWith("/")) {
        return null;
      }
      relative = relative.replaceFirst("^/+", "");
    } else if (relative.isBlank()
        || relative.contains("$")
        || relative.contains(":")
        || relative.startsWith("~")
        || relative.startsWith("/")) {
      return null;
    }
    if (relative.contains("${")) {
      return null;
    }
    List<String> parts = new ArrayList<>();
    for (String part : relative.split("/")) {
      if (part.isEmpty() || part.equals(".")) {
        continue;
      }
      if (part.equals("..")) {
        if (parts.isEmpty()) {
          return null;
        }
        parts.remove(parts.size() - 1);
      } else {
        parts.add(part);
      }
    }
    return String.join("/", parts);
  }

  private static String version(byte[] content) throws Exception {
    byte[] hash = MessageDigest.getInstance("SHA-256").digest(content);
    return HexFormat.of().formatHex(hash).substring(0, HopAirflowBundle.VERSION_LENGTH);
  }

  private static String text(JsonNode node, String key) {
    JsonNode value = node.get(key);
    if (value == null || value.isNull()) {
      return null;
    }
    String text = value.asText();
    return text.isBlank() ? null : text.trim();
  }
}
