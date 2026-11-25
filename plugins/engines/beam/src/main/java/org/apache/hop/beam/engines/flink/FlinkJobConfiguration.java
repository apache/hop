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

package org.apache.hop.beam.engines.flink;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.hop.core.exception.HopException;

/**
 * Beam's {@code FlinkRunnerResult} does not expose the Flink job id. Flink applies {@code
 * $internal.pipeline.job-id} from the configuration directory when it builds the job graph, so the
 * id can be fixed before submission.
 *
 * <p>Flink only reads that directory from the local filesystem.
 *
 * <p>For embedded Flink ({@code [local]}) the directory also hands Flink the class path of the Beam
 * plugin. Flink runs the job in this JVM, in a class loader for user code whose parent is the class
 * loader that loaded Flink: without the plugin's jars on its class path it cannot load the classes
 * of the Beam plugin (HopRowCoder, TransformFn, ...). Classes in Hop's own class path are loaded
 * from there first, so a library that is also in a plugin's lib/ is not loaded twice.
 */
final class FlinkJobConfiguration {

  private static final String JOB_ID_KEY = PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID.key();
  private static final String CLASSPATHS_KEY = PipelineOptions.CLASSPATHS.key();
  private static final String RESOLVE_ORDER_KEY = CoreOptions.CLASSLOADER_RESOLVE_ORDER.key();
  static final String PARENT_FIRST = "parent-first";

  private FlinkJobConfiguration() {}

  static Path create(String configuredConfDir, String jobId) throws HopException {
    return create(configuredConfDir, jobId, List.of());
  }

  /**
   * @param classpath for embedded Flink: the class path of the Beam plugin, as file URIs. It is
   *     added to the user's own {@code pipeline.classpaths}, and the resolve order becomes
   *     parent-first unless the user chose one. Empty for any other Flink master.
   */
  static Path create(String configuredConfDir, String jobId, List<String> classpath)
      throws HopException {
    String source = configuredConfDir;
    if (StringUtils.isEmpty(source)) {
      source = System.getenv("FLINK_CONF_DIR");
    }
    Path sourceDir = StringUtils.isEmpty(source) ? null : Path.of(source);
    return createFromSource(sourceDir, jobId, classpath);
  }

  static Path createFromSource(Path sourceDir, String jobId) throws HopException {
    return createFromSource(sourceDir, jobId, List.of());
  }

  static Path createFromSource(Path sourceDir, String jobId, List<String> classpath)
      throws HopException {
    try {
      JobID.fromHexString(jobId);
      Path dir = Files.createTempDirectory("hop-flink-");
      if (sourceDir == null) {
        Path target = dir.resolve(GlobalConfiguration.FLINK_CONF_FILENAME);
        Files.writeString(target, standardEntry(jobId), StandardCharsets.UTF_8);
        addClasspath(target, false, new Configuration(), classpath);
      } else if (!Files.isDirectory(sourceDir)) {
        throw new HopException("Flink configuration directory does not exist: " + sourceDir);
      } else {
        Path legacy = sourceDir.resolve(GlobalConfiguration.LEGACY_FLINK_CONF_FILENAME);
        Path standard = sourceDir.resolve(GlobalConfiguration.FLINK_CONF_FILENAME);
        Path sourceFile;
        String entry;
        if (Files.isRegularFile(legacy)) {
          sourceFile = legacy;
          entry = legacyEntry(jobId);
        } else if (Files.isRegularFile(standard)) {
          sourceFile = standard;
          entry = standardEntry(jobId);
        } else {
          throw new HopException(
              "Flink configuration directory '"
                  + sourceDir
                  + "' does not contain "
                  + GlobalConfiguration.FLINK_CONF_FILENAME
                  + " or "
                  + GlobalConfiguration.LEGACY_FLINK_CONF_FILENAME);
        }
        Path target = dir.resolve(sourceFile.getFileName());
        Files.copy(sourceFile, target);
        appendJobId(target, entry);
        if (!classpath.isEmpty()) {
          addClasspath(
              target,
              sourceFile.equals(legacy),
              GlobalConfiguration.loadConfiguration(sourceDir.toString()),
              classpath);
        }
      }
      return dir;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          "Unable to create a Flink configuration directory for job id " + jobId, e);
    }
  }

  static void delete(Path dir) throws IOException {
    if (dir == null || !Files.exists(dir)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(dir)) {
      for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
        Files.deleteIfExists(path);
      }
    }
  }

  /**
   * Add the class path to the user's own pipeline.classpaths, and make the resolve order
   * parent-first unless the user set one.
   *
   * @param user the user's configuration, read the way Flink reads it
   */
  private static void addClasspath(
      Path file, boolean legacy, Configuration user, List<String> classpath) throws IOException {
    if (classpath.isEmpty()) {
      return;
    }
    Set<String> entries =
        new LinkedHashSet<>(user.getOptional(PipelineOptions.CLASSPATHS).orElse(List.of()));
    entries.addAll(classpath);
    StringBuilder kept = new StringBuilder();
    for (String line : Files.readString(file).split("\\R", -1)) {
      if (!isKey(line, CLASSPATHS_KEY)) {
        kept.append(line).append('\n');
      }
    }
    String text = kept.toString().replaceAll("\n+$", "\n");
    if (legacy) {
      text += CLASSPATHS_KEY + ": " + String.join(";", entries) + "\n";
    } else {
      text +=
          "\""
              + CLASSPATHS_KEY
              + "\": ["
              + entries.stream().map(e -> "\"" + e + "\"").collect(Collectors.joining(", "))
              + "]\n";
    }
    if (!user.contains(CoreOptions.CLASSLOADER_RESOLVE_ORDER)) {
      text +=
          legacy
              ? RESOLVE_ORDER_KEY + ": " + PARENT_FIRST + "\n"
              : "\"" + RESOLVE_ORDER_KEY + "\": \"" + PARENT_FIRST + "\"\n";
    }
    Files.writeString(file, text, StandardCharsets.UTF_8);
  }

  /** A flat entry of the key, in either format: {@code key: ...} or {@code "key": ...}. */
  private static boolean isKey(String line, String key) {
    String trimmed = line.trim();
    return trimmed.startsWith(key + ":") || trimmed.startsWith("\"" + key + "\":");
  }

  private static void appendJobId(Path file, String entry) throws IOException {
    StringBuilder kept = new StringBuilder();
    for (String line : Files.readString(file).split("\\R", -1)) {
      if (!line.contains(JOB_ID_KEY)) {
        kept.append(line).append('\n');
      }
    }
    Files.writeString(file, kept + entry, StandardCharsets.UTF_8);
  }

  private static String standardEntry(String jobId) {
    return "\"" + JOB_ID_KEY + "\": \"" + jobId + "\"\n";
  }

  private static String legacyEntry(String jobId) {
    return JOB_ID_KEY + ": " + jobId + "\n";
  }
}
