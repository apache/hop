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

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.databricks.client.DatabricksJobsClient;
import org.apache.hop.databricks.client.WorkspaceFileMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.engine.PipelineEnginePluginType;

/**
 * Uploads fat jar, pipeline, exported metadata, optional Spark project package, and optional env
 * config for a MainSpark job on Databricks. Fat jar upload is skipped when remote size and SHA-256
 * sidecar match the local file.
 */
public final class HopSparkDeployHelper {

  public static final String FAT_JAR_REMOTE_NAME = "hop-native.jar";
  public static final String PIPELINE_REMOTE_NAME = "pipeline.hpl";
  public static final String METADATA_REMOTE_NAME = "metadata.json";
  public static final String PROJECT_PACKAGE_REMOTE_NAME = "hop-spark-package.zip";
  public static final String ENV_CONFIG_REMOTE_NAME = "env-config.json";
  public static final String SHA256_SIDECAR_SUFFIX = ".sha256";

  /** Plugin id of the native Spark pipeline engine (engines/spark). */
  public static final String SPARK_ENGINE_PLUGIN_ID = "SparkPipelineEngine";

  private static final String SPARK_PROJECT_PACKAGE_CLASS =
      "org.apache.hop.spark.pkg.SparkProjectPackage";

  /**
   * @param jarDbfs fat jar remote path
   * @param pipelineDbfs uploaded pipeline path, or null when package-only relative path is used
   * @param metadataDbfs metadata JSON path, or null when package embeds metadata
   * @param projectPackageDbfs project package zip, or null
   * @param envConfigDbfs environment config JSON, or null
   * @param runConfigName Native Spark run configuration name
   * @param launch ready-to-use MainSpark parameters for the Jobs API
   */
  public record DeployedArtifacts(
      String jarDbfs,
      String pipelineDbfs,
      String metadataDbfs,
      String projectPackageDbfs,
      String envConfigDbfs,
      String runConfigName,
      MainSparkLaunchSpec launch) {

    /** Backward-compatible view of the classic three paths. */
    public DeployedArtifacts {
      if (launch == null) {
        throw new IllegalArgumentException("launch is required");
      }
    }
  }

  /**
   * Deploy options for optional project package and environment file.
   *
   * @param uploadProjectPackage when true, export or upload a Spark project package zip
   * @param projectHome project home for export (default {@code PROJECT_HOME} variable)
   * @param projectPackageFile optional existing zip; when blank and uploadProjectPackage, export
   * @param environmentConfigFile optional local/VFS env JSON for MainSpark {@code --HopConfigFile}
   */
  public record DeployOptions(
      boolean uploadProjectPackage,
      String projectHome,
      String projectPackageFile,
      String environmentConfigFile) {

    public static DeployOptions none() {
      return new DeployOptions(false, null, null, null);
    }
  }

  private HopSparkDeployHelper() {}

  /**
   * Export metadata, copy pipeline if needed, upload classic three artifacts under {@code
   * dbfsBaseDir}.
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
    return deploy(
        client,
        metadataProvider,
        variables,
        log,
        localFatJar,
        localOrVfsPipeline,
        runConfigName,
        dbfsBaseDir,
        DeployOptions.none());
  }

  /**
   * Upload fat jar, pipeline/metadata and optional project package + environment config under
   * {@code dbfsBaseDir}.
   */
  public static DeployedArtifacts deploy(
      DatabricksJobsClient client,
      IHopMetadataProvider metadataProvider,
      IVariables variables,
      ILogChannel log,
      String localFatJar,
      String localOrVfsPipeline,
      String runConfigName,
      String dbfsBaseDir,
      DeployOptions options)
      throws HopException {
    if (options == null) {
      options = DeployOptions.none();
    }
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

    Path jarPath = resolveLocalFile(jar, "Fat jar");

    Path pipelineLocal = materializeToTemp(pipeline, "hop-dbx-pipeline-", ".hpl", "pipeline");

    String jarDbfs = base + "/" + FAT_JAR_REMOTE_NAME;
    String pipelineDbfs = base + "/" + PIPELINE_REMOTE_NAME;
    String metadataDbfs = base + "/" + METADATA_REMOTE_NAME;
    String packageDbfs = null;
    String envDbfs = null;

    uploadFatJarIfNeeded(client, log, jarPath, jarDbfs);

    // --- Project package (optional) ---
    Path packageLocal = null;
    boolean packageMode = options.uploadProjectPackage();
    String relativePipeline = null;
    if (packageMode) {
      String existingPkg = resolve(variables, options.projectPackageFile());
      if (StringUtils.isNotBlank(existingPkg)) {
        packageLocal = resolveLocalFile(existingPkg, "Project package");
      } else {
        String home = resolveProjectHome(variables, options.projectHome());
        try {
          packageLocal = Files.createTempFile("hop-dbx-spark-pkg-", ".zip");
          packageLocal.toFile().deleteOnExit();
        } catch (Exception e) {
          throw new HopException("Unable to create temp file for Spark project package", e);
        }
        if (log != null && log.isBasic()) {
          log.logBasic("Exporting Spark project package from " + home);
        }
        exportSparkProjectPackage(home, packageLocal.toString(), metadataProvider, variables);
      }
      packageDbfs = base + "/" + PROJECT_PACKAGE_REMOTE_NAME;
      if (log != null && log.isBasic()) {
        log.logBasic("Uploading project package to " + packageDbfs);
      }
      client.uploadToDbfs(packageLocal, packageDbfs);

      String homeForRel = resolveProjectHome(variables, options.projectHome());
      relativePipeline = relativePipelinePath(pipeline, homeForRel, variables);
    }

    // --- Pipeline: always upload in simple mode; upload as fallback when not relative ---
    boolean useRelativePipeline = packageMode && relativePipeline != null;
    if (!useRelativePipeline) {
      if (log != null && log.isBasic()) {
        log.logBasic("Uploading pipeline to " + pipelineDbfs);
      }
      client.uploadToDbfs(pipelineLocal, pipelineDbfs);
    } else if (log != null && log.isDetailed()) {
      log.logDetailed(
          "Using package-relative pipeline path '"
              + relativePipeline
              + "' (not uploading separate pipeline.hpl)");
    }

    // --- Metadata: always in simple mode; package embeds metadata when package mode ---
    String metadataRemote = null;
    if (!packageMode) {
      Path metadataLocal;
      try {
        String json = new SerializableMetadataProvider(metadataProvider).toJson();
        metadataLocal = Files.createTempFile("hop-dbx-metadata-", ".json");
        metadataLocal.toFile().deleteOnExit();
        Files.writeString(metadataLocal, json, StandardCharsets.UTF_8);
      } catch (Exception e) {
        throw new HopException("Unable to export Hop metadata to JSON", e);
      }
      if (log != null && log.isBasic()) {
        log.logBasic("Uploading metadata to " + metadataDbfs);
      }
      client.uploadToDbfs(metadataLocal, metadataDbfs);
      metadataRemote = metadataDbfs;
    }

    // --- Environment config (optional) ---
    String envLocalPath = resolve(variables, options.environmentConfigFile());
    if (StringUtils.isNotBlank(envLocalPath)) {
      Path envLocal =
          materializeToTemp(envLocalPath, "hop-dbx-env-", ".json", "environment config");
      envDbfs = base + "/" + ENV_CONFIG_REMOTE_NAME;
      if (log != null && log.isBasic()) {
        log.logBasic("Uploading environment config to " + envDbfs);
      }
      client.uploadToDbfs(envLocal, envDbfs);
    }

    String launchPipeline = useRelativePipeline ? relativePipeline : pipelineDbfs;
    MainSparkLaunchSpec launch =
        new MainSparkLaunchSpec(launchPipeline, metadataRemote, runConfig, packageDbfs, envDbfs);

    return new DeployedArtifacts(
        jarDbfs,
        useRelativePipeline ? null : pipelineDbfs,
        metadataRemote,
        packageDbfs,
        envDbfs,
        runConfig,
        launch);
  }

  /** Resolve project home: explicit field, else {@code PROJECT_HOME} variable. */
  static String resolveProjectHome(IVariables variables, String projectHomeField)
      throws HopException {
    String home = resolve(variables, projectHomeField);
    if (StringUtils.isBlank(home) && variables != null) {
      home = resolve(variables, variables.getVariable("PROJECT_HOME"));
    }
    if (StringUtils.isBlank(home)) {
      throw new HopException(
          "Project home is required to export a Spark project package. Set PROJECT_HOME or the"
              + " Project home field on the Databricks Job Run action.");
    }
    return home;
  }

  /**
   * If {@code pipelinePath} is under {@code projectHome}, return a package-relative path using
   * forward slashes; otherwise null (caller should upload pipeline.hpl).
   */
  static String relativePipelinePath(
      String pipelinePath, String projectHome, IVariables variables) {
    if (StringUtils.isBlank(pipelinePath) || StringUtils.isBlank(projectHome)) {
      return null;
    }
    try {
      String pipe = resolve(variables, pipelinePath);
      String home = resolve(variables, projectHome);
      Path pipePath = Paths.get(pipe).toAbsolutePath().normalize();
      Path homePath = Paths.get(home).toAbsolutePath().normalize();
      if (!pipePath.startsWith(homePath)) {
        // try VFS-resolved local
        try {
          pipePath =
              Paths.get(HopVfs.getFileObject(pipe).getURL().toURI()).toAbsolutePath().normalize();
        } catch (Exception ignored) {
          return null;
        }
        if (!pipePath.startsWith(homePath)) {
          return null;
        }
      }
      Path rel = homePath.relativize(pipePath);
      return rel.toString().replace('\\', '/');
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Call {@code SparkProjectPackage.exportProject} via the Spark engine plugin classloader (no
   * compile dependency on hop-engines-spark).
   */
  static void exportSparkProjectPackage(
      String projectHome,
      String zipFilename,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws HopException {
    try {
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin plugin =
          registry.findPluginWithId(PipelineEnginePluginType.class, SPARK_ENGINE_PLUGIN_ID);
      if (plugin == null) {
        throw new HopException(
            "Native Spark engine plugin '"
                + SPARK_ENGINE_PLUGIN_ID
                + "' is not installed. Install plugins/engines/spark to export a project package,"
                + " or provide an existing package zip path.");
      }
      ClassLoader cl = registry.getClassLoader(plugin);
      Class<?> clazz = Class.forName(SPARK_PROJECT_PACKAGE_CLASS, true, cl);
      Method export =
          clazz.getMethod(
              "exportProject",
              String.class,
              String.class,
              IHopMetadataProvider.class,
              IVariables.class);
      export.invoke(null, projectHome, zipFilename, metadataProvider, variables);
    } catch (HopException e) {
      throw e;
    } catch (ReflectiveOperationException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      if (cause instanceof HopException he) {
        throw he;
      }
      throw new HopException(
          "Unable to export Spark project package via "
              + SPARK_PROJECT_PACKAGE_CLASS
              + ": "
              + cause.getMessage(),
          cause);
    } catch (Exception e) {
      throw new HopException("Unable to export Spark project package: " + e.getMessage(), e);
    }
  }

  static Path materializeToTemp(String pathOrVfs, String prefix, String suffix, String label)
      throws HopException {
    try {
      if (Files.isRegularFile(Paths.get(pathOrVfs))) {
        return Paths.get(pathOrVfs);
      }
      Path temp = Files.createTempFile(prefix, suffix);
      temp.toFile().deleteOnExit();
      try (var in = HopVfs.getInputStream(pathOrVfs);
          var out = Files.newOutputStream(temp)) {
        in.transferTo(out);
      }
      return temp;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to read " + label + ": " + pathOrVfs, e);
    }
  }

  static Path resolveLocalFile(String pathOrVfs, String label) throws HopException {
    Path p = Paths.get(pathOrVfs);
    if (Files.isRegularFile(p)) {
      return p;
    }
    try {
      p = Paths.get(HopVfs.getFileObject(pathOrVfs).getURL().toURI());
    } catch (Exception e) {
      throw new HopException(label + " not found: " + pathOrVfs, e);
    }
    if (!Files.isRegularFile(p)) {
      throw new HopException(label + " not found: " + pathOrVfs);
    }
    return p;
  }

  /**
   * Upload fat jar only when remote is missing, size differs, or SHA-256 sidecar does not match.
   * Sidecar path is {@code remoteJarPath + ".sha256"} (hex digest only, UTF-8).
   */
  static void uploadFatJarIfNeeded(
      DatabricksJobsClient client, ILogChannel log, Path localJar, String remoteJarPath)
      throws HopException {
    long localSize;
    try {
      localSize = Files.size(localJar);
    } catch (Exception e) {
      throw new HopException("Unable to read local fat jar size: " + localJar, e);
    }
    String localSha = sha256Hex(localJar);
    String sidecarPath = remoteJarPath + SHA256_SIDECAR_SUFFIX;

    if (remoteJarMatches(client, remoteJarPath, sidecarPath, localSize, localSha)) {
      if (log != null && log.isBasic()) {
        log.logBasic(
            "Skipping fat jar upload (remote matches local size="
                + localSize
                + " sha256="
                + localSha.substring(0, Math.min(12, localSha.length()))
                + "…): "
                + remoteJarPath);
      }
      return;
    }

    if (log != null && log.isBasic()) {
      log.logBasic("Uploading fat jar to " + remoteJarPath + " (" + localSize + " bytes)");
    }
    client.uploadToDbfs(localJar, remoteJarPath);
    client.uploadText(sidecarPath, localSha + "\n");
    if (log != null && log.isBasic()) {
      log.logBasic("Wrote fat jar checksum sidecar " + sidecarPath);
    }
  }

  static boolean remoteJarMatches(
      DatabricksJobsClient client,
      String remoteJarPath,
      String sidecarPath,
      long localSize,
      String localSha)
      throws HopException {
    WorkspaceFileMetadata remote = client.getFileMetadata(remoteJarPath);
    if (!remote.exists() || remote.sizeBytes() != localSize) {
      return false;
    }
    Optional<String> remoteSidecar = client.downloadTextIfExists(sidecarPath);
    if (remoteSidecar.isEmpty()) {
      return false;
    }
    String remoteSha = normalizeSha256(remoteSidecar.get());
    return localSha.equalsIgnoreCase(remoteSha);
  }

  /** SHA-256 hex of file contents (lowercase). */
  static String sha256Hex(Path file) throws HopException {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      try (InputStream in = Files.newInputStream(file);
          DigestInputStream din = new DigestInputStream(in, digest)) {
        din.transferTo(OutputStream.nullOutputStream());
      }
      return HexFormat.of().formatHex(digest.digest());
    } catch (Exception e) {
      throw new HopException("Unable to compute SHA-256 of " + file, e);
    }
  }

  /** Accept "hex", "hex filename", or whitespace-padded sidecars. */
  static String normalizeSha256(String sidecarContent) {
    if (sidecarContent == null) {
      return "";
    }
    String t = sidecarContent.trim();
    if (t.isEmpty()) {
      return "";
    }
    int space = t.indexOf(' ');
    if (space > 0) {
      t = t.substring(0, space).trim();
    }
    int tab = t.indexOf('\t');
    if (tab > 0) {
      t = t.substring(0, tab).trim();
    }
    return t.toLowerCase();
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
