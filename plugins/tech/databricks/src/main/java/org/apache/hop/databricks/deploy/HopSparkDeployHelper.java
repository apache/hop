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

package org.apache.hop.databricks.deploy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.databricks.client.DatabricksJobsClient;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * Uploads fat jar, pipeline, and exported metadata JSON to DBFS for a MainSpark job on Databricks.
 */
public final class HopSparkDeployHelper {

  public record DeployedArtifacts(
      String jarDbfs, String pipelineDbfs, String metadataDbfs, String runConfigName) {}

  private HopSparkDeployHelper() {}

  /**
   * Export metadata, copy pipeline if needed, upload three artifacts under {@code dbfsBaseDir}.
   *
   * @param dbfsBaseDir e.g. {@code dbfs:/FileStore/hop/my-run} (trailing slash optional)
   */
  public static DeployedArtifacts deploy(
      DatabricksJobsClient client,
      IHopMetadataProvider metadataProvider,
      IVariables variables,
      ILogChannel log,
      String localFatJar,
      String localOrVfsPipeline,
      String runConfigName,
      String dbfsBaseDir)
      throws HopException {
    String jar = resolve(variables, localFatJar);
    String pipeline = resolve(variables, localOrVfsPipeline);
    String runConfig = resolve(variables, runConfigName);
    String base = normalizeBase(resolve(variables, dbfsBaseDir));

    if (StringUtils.isBlank(jar)) {
      throw new HopException("Fat jar path is required for deploy");
    }
    if (StringUtils.isBlank(pipeline)) {
      throw new HopException("Pipeline filename is required for deploy");
    }
    if (StringUtils.isBlank(runConfig)) {
      throw new HopException("Pipeline run configuration name is required for deploy");
    }

    Path jarPath = Paths.get(jar);
    if (!Files.isRegularFile(jarPath)) {
      // try VFS
      try {
        jarPath = Paths.get(HopVfs.getFileObject(jar).getURL().toURI());
      } catch (Exception e) {
        throw new HopException("Fat jar not found: " + jar, e);
      }
      if (!Files.isRegularFile(jarPath)) {
        throw new HopException("Fat jar not found: " + jar);
      }
    }

    Path pipelineLocal;
    try {
      if (Files.isRegularFile(Paths.get(pipeline))) {
        pipelineLocal = Paths.get(pipeline);
      } else {
        // Materialize VFS pipeline to temp
        pipelineLocal = Files.createTempFile("hop-dbx-pipeline-", ".hpl");
        pipelineLocal.toFile().deleteOnExit();
        try (var in = HopVfs.getInputStream(pipeline);
            var out = Files.newOutputStream(pipelineLocal)) {
          in.transferTo(out);
        }
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to read pipeline: " + pipeline, e);
    }

    Path metadataLocal;
    try {
      String json = new SerializableMetadataProvider(metadataProvider).toJson();
      metadataLocal = Files.createTempFile("hop-dbx-metadata-", ".json");
      metadataLocal.toFile().deleteOnExit();
      Files.writeString(metadataLocal, json, StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new HopException("Unable to export Hop metadata to JSON", e);
    }

    String jarDbfs = base + "/hop-native.jar";
    String pipelineDbfs = base + "/pipeline.hpl";
    String metadataDbfs = base + "/metadata.json";

    if (log != null && log.isBasic()) {
      log.logBasic("Uploading fat jar to " + jarDbfs);
    }
    client.uploadToDbfs(jarPath, jarDbfs);
    if (log != null && log.isBasic()) {
      log.logBasic("Uploading pipeline to " + pipelineDbfs);
    }
    client.uploadToDbfs(pipelineLocal, pipelineDbfs);
    if (log != null && log.isBasic()) {
      log.logBasic("Uploading metadata to " + metadataDbfs);
    }
    client.uploadToDbfs(metadataLocal, metadataDbfs);

    return new DeployedArtifacts(jarDbfs, pipelineDbfs, metadataDbfs, runConfig);
  }

  static String normalizeBase(String dbfsBaseDir) throws HopException {
    if (StringUtils.isBlank(dbfsBaseDir)) {
      throw new HopException("DBFS base directory is required");
    }
    String b = dbfsBaseDir.trim();
    if (b.startsWith("dbfs:")) {
      b = b.substring("dbfs:".length());
    }
    if (!b.startsWith("/")) {
      b = "/" + b;
    }
    while (b.endsWith("/") && b.length() > 1) {
      b = b.substring(0, b.length() - 1);
    }
    return b;
  }

  private static String resolve(IVariables variables, String value) {
    if (value == null) {
      return null;
    }
    return variables != null ? variables.resolve(value) : value;
  }
}
