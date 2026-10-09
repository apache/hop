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
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.hop.core.exception.HopException;

/**
 * Beam's {@code FlinkRunnerResult} does not expose the Flink job id. Flink applies {@code
 * $internal.pipeline.job-id} from the configuration directory when it builds the job graph, so the
 * id can be fixed before submission.
 *
 * <p>Flink only reads that directory from the local filesystem.
 */
final class FlinkJobConfiguration {

  private static final String JOB_ID_KEY = PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID.key();

  private FlinkJobConfiguration() {}

  static Path create(String configuredConfDir, String jobId) throws HopException {
    String source = configuredConfDir;
    if (StringUtils.isEmpty(source)) {
      source = System.getenv("FLINK_CONF_DIR");
    }
    Path sourceDir = StringUtils.isEmpty(source) ? null : Path.of(source);
    return createFromSource(sourceDir, jobId);
  }

  static Path createFromSource(Path sourceDir, String jobId) throws HopException {
    try {
      JobID.fromHexString(jobId);
      Path dir = Files.createTempDirectory("hop-flink-");
      if (sourceDir == null) {
        Files.writeString(
            dir.resolve(GlobalConfiguration.FLINK_CONF_FILENAME),
            standardEntry(jobId),
            StandardCharsets.UTF_8);
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
