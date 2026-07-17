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

package org.apache.hop.spark.pkg;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * Native Spark <em>project package</em>: a zip of Hop definition files (pipelines, workflows, small
 * resources) plus {@code metadata.json}, staged for cluster/local Spark runs so nested Simple
 * Mapping / Pipeline Executor paths under {@code ${PROJECT_HOME}} resolve via Hop VFS.
 *
 * <p>Layout written by {@link #exportProject}:
 *
 * <pre>
 * metadata.json
 * hop-spark-package.properties   # projectRoot=project
 * project/…                      # project home tree (relative paths)
 * </pre>
 *
 * <p>Also accepts classic GUI project export zips (single top-level folder). Default
 * materialization extracts into {@code ${java.io.tmpdir}/hop-spark-pkg-&lt;key&gt;/} once per JVM.
 *
 * <p><strong>Not for bulk Dataset data.</strong> {@code PROJECT_HOME} after materialize points at
 * definition files. Use separate variables ({@code HOP_DATA}, {@code OUTPUT_ROOT}, {@code s3a://…})
 * for Spark File / Lake paths.
 */
public final class SparkProjectPackage {

  /** Variable holding the package zip URI/path for driver and executor materialization. */
  public static final String VAR_PACKAGE_URI = "HOP_SPARK_PROJECT_PACKAGE";

  /**
   * Basename of the package after {@link org.apache.spark.SparkContext#addFile(String)} — workers
   * resolve it via {@link org.apache.spark.SparkFiles#get(String)}.
   */
  public static final String VAR_PACKAGE_SPARK_FILE = "HOP_SPARK_PROJECT_PACKAGE_SPARK_FILE";

  public static final String METADATA_ENTRY = "metadata.json";
  public static final String MARKER_ENTRY = "hop-spark-package.properties";
  public static final String DEFAULT_PROJECT_ROOT = "project";
  public static final String PROP_PROJECT_ROOT = "projectRoot";

  private static final String COMPLETE_MARKER = ".hop-spark-package-complete";

  /** packageUri → absolute project home path (extracted). */
  private static final Map<String, String> MATERIALIZED = new ConcurrentHashMap<>();

  /** applicationId|localPath keys already passed to SparkContext.addFile on this driver JVM. */
  private static final Set<String> DISTRIBUTED = ConcurrentHashMap.newKeySet();

  private static final Set<String> SKIP_DIR_NAMES =
      Set.of(".git", "datasets", "target", "node_modules", ".idea", ".settings");

  private SparkProjectPackage() {}

  /** Result of materializing a package zip on this JVM. */
  public record Materialized(String projectHome, String metadataPath, String extractRoot) {}

  /**
   * Ensure the project package (if configured) is extracted and {@code PROJECT_HOME} points at the
   * project root. Safe to call from every mapPartitions init; materializes once per package path
   * per JVM.
   *
   * <p>On cluster workers, prefers the file distributed via {@code SparkContext.addFile} / {@code
   * SparkFiles.get} ({@link #VAR_PACKAGE_SPARK_FILE}).
   */
  public static void ensureMaterializedOnWorker(IVariables variables) throws HopException {
    if (variables == null) {
      return;
    }
    String packagePath = resolvePackagePathForMaterialize(variables);
    if (StringUtils.isEmpty(packagePath)) {
      return;
    }
    Materialized m = materialize(packagePath);
    // Preserve SparkFiles basename for subsequent partition inits
    String sparkFile = variables.getVariable(VAR_PACKAGE_SPARK_FILE);
    applyToVariables(variables, m, packagePath);
    if (StringUtils.isNotEmpty(sparkFile)) {
      variables.setVariable(VAR_PACKAGE_SPARK_FILE, sparkFile);
    }
  }

  /**
   * Resolve the zip path to open on this JVM: SparkFiles download first, then {@link
   * #VAR_PACKAGE_URI}.
   */
  public static String resolvePackagePathForMaterialize(IVariables variables) {
    if (variables == null) {
      return null;
    }
    String sparkFileName = variables.getVariable(VAR_PACKAGE_SPARK_FILE);
    if (StringUtils.isNotEmpty(sparkFileName)) {
      try {
        String downloaded = org.apache.spark.SparkFiles.get(sparkFileName.trim());
        if (StringUtils.isNotEmpty(downloaded) && new File(downloaded).isFile()) {
          return downloaded;
        }
      } catch (Throwable t) {
        // SparkFiles not available yet or file not distributed — fall through
      }
    }
    String raw = variables.getVariable(VAR_PACKAGE_URI);
    if (StringUtils.isEmpty(raw)) {
      return null;
    }
    String resolved = variables.resolve(raw);
    return StringUtils.isEmpty(resolved) ? null : resolved;
  }

  /**
   * Copy the package to a local file if needed and register it with {@code SparkContext.addFile} so
   * every executor receives it. Sets {@link #VAR_PACKAGE_SPARK_FILE} and rewrites {@link
   * #VAR_PACKAGE_URI} to the local path used for distribution.
   *
   * <p>No-op when {@link #VAR_PACKAGE_URI} is unset. Safe to call for {@code local[*]} and
   * spark-submit (including when reusing an active session).
   */
  public static void distributeToCluster(
      org.apache.spark.sql.SparkSession spark, IVariables variables) throws HopException {
    if (spark == null || variables == null) {
      return;
    }
    String raw = variables.getVariable(VAR_PACKAGE_URI);
    if (StringUtils.isEmpty(raw)) {
      return;
    }
    String uri = variables.resolve(raw);
    if (StringUtils.isEmpty(uri)) {
      return;
    }

    String localPath = ensureLocalPackageFile(uri);
    String basename = new File(localPath).getName();
    if (StringUtils.isEmpty(basename)) {
      throw new HopException("Spark project package has empty filename: " + localPath);
    }

    String appId;
    try {
      appId = spark.sparkContext().applicationId();
    } catch (Exception e) {
      appId = "unknown";
    }
    String distKey = appId + "|" + localPath;
    if (DISTRIBUTED.add(distKey)) {
      try {
        spark.sparkContext().addFile(localPath);
      } catch (Exception e) {
        DISTRIBUTED.remove(distKey);
        throw new HopException(
            "Failed to distribute Spark project package via SparkContext.addFile: " + localPath, e);
      }
    }

    variables.setVariable(VAR_PACKAGE_URI, localPath);
    variables.setVariable(VAR_PACKAGE_SPARK_FILE, basename);
  }

  /**
   * Ensure {@code packageUri} is a local filesystem path suitable for {@code addFile}. Remote/VFS
   * URIs are copied into {@code java.io.tmpdir}.
   */
  public static String ensureLocalPackageFile(String packageUri) throws HopException {
    if (StringUtils.isEmpty(packageUri)) {
      throw new HopException("Spark project package URI is empty");
    }
    File asFile = new File(packageUri);
    if (asFile.isFile()) {
      return asFile.getAbsolutePath();
    }
    // file:/ URI without Hop VFS
    if (packageUri.startsWith("file:")) {
      try {
        File f = new File(java.net.URI.create(packageUri));
        if (f.isFile()) {
          return f.getAbsolutePath();
        }
      } catch (Exception ignored) {
        // fall through to HopVfs
      }
    }
    try {
      FileObject fo = HopVfs.getFileObject(packageUri);
      if (!fo.exists() || fo.getType() != FileType.FILE) {
        throw new HopException("Spark project package not found or not a file: " + packageUri);
      }
      File local =
          new File(
              System.getProperty("java.io.tmpdir"),
              "hop-spark-pkg-src-" + cacheKey(packageUri) + ".zip");
      if (!local.isFile() || local.length() == 0) {
        try (InputStream in = HopVfs.getInputStream(fo);
            OutputStream out = Files.newOutputStream(local.toPath())) {
          in.transferTo(out);
        }
      }
      return local.getAbsolutePath();
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          "Unable to stage Spark project package locally for distribution: " + packageUri, e);
    }
  }

  /** Set package URI + PROJECT_HOME (+ optional metadata path helper is caller-side). */
  public static void applyToVariables(IVariables variables, Materialized m, String packageUri) {
    if (variables == null || m == null) {
      return;
    }
    if (StringUtils.isNotEmpty(packageUri)) {
      variables.setVariable(VAR_PACKAGE_URI, packageUri);
    }
    variables.setVariable("PROJECT_HOME", m.projectHome());
  }

  /**
   * Extract {@code packageUri} under the JVM temp directory if not already done.
   *
   * @param packageUri path or VFS URI to the package zip
   */
  public static Materialized materialize(String packageUri) throws HopException {
    if (StringUtils.isEmpty(packageUri)) {
      throw new HopException("Spark project package URI is empty");
    }
    String key = cacheKey(packageUri);
    String cached = MATERIALIZED.get(key);
    if (cached != null) {
      return buildMaterialized(cached, packageUri);
    }
    synchronized (MATERIALIZED) {
      cached = MATERIALIZED.get(key);
      if (cached != null) {
        return buildMaterialized(cached, packageUri);
      }
      try {
        File extractRoot = new File(System.getProperty("java.io.tmpdir"), "hop-spark-pkg-" + key);
        File complete = new File(extractRoot, COMPLETE_MARKER);
        if (!complete.isFile()) {
          if (extractRoot.exists()) {
            deleteRecursively(extractRoot);
          }
          if (!extractRoot.mkdirs() && !extractRoot.isDirectory()) {
            throw new HopException("Unable to create package extract dir: " + extractRoot);
          }
          extractZip(packageUri, extractRoot);
          Files.writeString(complete.toPath(), packageUri, StandardCharsets.UTF_8);
        }
        String projectHome = detectProjectHome(extractRoot).getAbsolutePath();
        MATERIALIZED.put(key, extractRoot.getAbsolutePath());
        return buildMaterialized(extractRoot.getAbsolutePath(), packageUri);
      } catch (HopException e) {
        throw e;
      } catch (Exception e) {
        throw new HopException("Error materializing Spark project package: " + packageUri, e);
      }
    }
  }

  private static Materialized buildMaterialized(String extractRootPath, String packageUri)
      throws HopException {
    File extractRoot = new File(extractRootPath);
    File projectHome = detectProjectHome(extractRoot);
    String metadata = findMetadataPath(extractRoot, projectHome);
    return new Materialized(projectHome.getAbsolutePath(), metadata, extractRootPath);
  }

  /**
   * Export a Hop project home directory into a Spark-oriented package zip.
   *
   * @param projectHome local/VFS path of the project home (definition tree)
   * @param zipFilename destination zip (created/overwritten)
   * @param metadataProvider metadata to embed as root {@code metadata.json} (required for
   *     MainSpark)
   */
  public static void exportProject(
      String projectHome,
      String zipFilename,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws HopException {
    if (StringUtils.isEmpty(projectHome)) {
      throw new HopException("Project home is required for Spark project package export");
    }
    if (StringUtils.isEmpty(zipFilename)) {
      throw new HopException("Zip filename is required for Spark project package export");
    }
    if (metadataProvider == null) {
      throw new HopException("Metadata provider is required for Spark project package export");
    }
    String realHome = variables != null ? variables.resolve(projectHome) : projectHome;
    String realZip = variables != null ? variables.resolve(zipFilename) : zipFilename;

    try {
      FileObject home = HopVfs.getFileObject(realHome);
      if (!home.exists() || home.getType() != FileType.FOLDER) {
        throw new HopException("Project home is not a folder: " + realHome);
      }

      FileObject zipFile = HopVfs.getFileObject(realZip);
      if (zipFile.exists()) {
        zipFile.delete();
      }

      try (OutputStream os = HopVfs.getOutputStream(zipFile, false);
          ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(os))) {

        // Marker
        Properties props = new Properties();
        props.setProperty(PROP_PROJECT_ROOT, DEFAULT_PROJECT_ROOT);
        props.setProperty("format", "hop-spark-project-package");
        props.setProperty("formatVersion", "1");
        zos.putNextEntry(new ZipEntry(MARKER_ENTRY));
        props.store(zos, "Apache Hop Native Spark project package");
        zos.closeEntry();

        // Metadata at zip root
        String metadataJson = new SerializableMetadataProvider(metadataProvider).toJson();
        zos.putNextEntry(new ZipEntry(METADATA_ENTRY));
        zos.write(metadataJson.getBytes(StandardCharsets.UTF_8));
        zos.closeEntry();

        // Project tree under project/
        Set<String> written = new HashSet<>();
        home.findFiles(
            new FileSelector() {
              @Override
              public boolean includeFile(FileSelectInfo fileInfo) throws Exception {
                FileObject fo = fileInfo.getFile();
                if (fo.getType() != FileType.FILE) {
                  return false;
                }
                String rel = relativePath(home, fo);
                if (rel == null || shouldSkipRelative(rel)) {
                  return false;
                }
                // Do not nest the destination zip into itself
                if (zipFile.equals(fo)) {
                  return false;
                }
                String entryName = DEFAULT_PROJECT_ROOT + "/" + rel.replace('\\', '/');
                if (!written.add(entryName)) {
                  return false;
                }
                zos.putNextEntry(new ZipEntry(entryName));
                try (InputStream in = HopVfs.getInputStream(fo)) {
                  in.transferTo(zos);
                }
                zos.closeEntry();
                return false;
              }

              @Override
              public boolean traverseDescendents(FileSelectInfo fileInfo) throws Exception {
                FileObject fo = fileInfo.getFile();
                if (fo.getType() != FileType.FOLDER) {
                  return true;
                }
                String base = fo.getName().getBaseName();
                if (base.startsWith(".") && fileInfo.getDepth() > 0) {
                  return false;
                }
                return !SKIP_DIR_NAMES.contains(base.toLowerCase(Locale.ROOT));
              }
            });
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          "Error exporting Spark project package from '" + realHome + "' to '" + realZip + "'", e);
    }
  }

  /**
   * Resolve a pipeline path against a materialized project home when the path is relative or uses
   * {@code ${PROJECT_HOME}}.
   */
  public static String resolvePipelinePath(
      String pipelinePath, Materialized m, IVariables variables) throws HopException {
    if (StringUtils.isEmpty(pipelinePath)) {
      throw new HopException("Pipeline path is empty");
    }
    String p = variables != null ? variables.resolve(pipelinePath) : pipelinePath;
    p = p.trim();
    if (p.contains("://") || p.startsWith("/") || p.matches("^[A-Za-z]:[\\\\/].*")) {
      return p;
    }
    // Strip leading PROJECT_HOME/ if still present after resolve failed to expand
    if (p.startsWith("${PROJECT_HOME}/")) {
      p = p.substring("${PROJECT_HOME}/".length());
    }
    String home = m.projectHome();
    if (!home.endsWith("/") && !home.endsWith("\\")) {
      home = home + File.separator;
    }
    return new File(home, p).getAbsolutePath();
  }

  /** Warning text when a Spark Dataset path still references PROJECT_HOME. */
  public static String projectHomeDataPathWarning(String rawPath) {
    if (rawPath == null) {
      return null;
    }
    if (!rawPath.contains("PROJECT_HOME") && !rawPath.contains("${PROJECT_HOME}")) {
      return null;
    }
    return "Spark File/Lake path references PROJECT_HOME ('"
        + rawPath
        + "'). With a Spark project package, PROJECT_HOME is the extracted *definition* tree "
        + "(not object-store data). Prefer HOP_DATA / OUTPUT_ROOT or an explicit Spark URI "
        + "(e.g. s3a://…) for Dataset I/O.";
  }

  // --- internals ---

  private static String cacheKey(String packageUri) {
    // Stable short key; include length-ish hash of URI string
    int h = packageUri.trim().hashCode();
    return Integer.toHexString(h);
  }

  private static void extractZip(String packageUri, File extractRoot) throws Exception {
    FileObject zipFo = HopVfs.getFileObject(packageUri);
    if (!zipFo.exists()) {
      throw new HopException("Spark project package not found: " + packageUri);
    }
    try (InputStream raw = HopVfs.getInputStream(zipFo);
        ZipInputStream zis = new ZipInputStream(new BufferedInputStream(raw))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        if (entry.isDirectory()) {
          continue;
        }
        String name = entry.getName();
        if (name.contains("..")) {
          throw new HopException("Refusing zip entry with '..': " + name);
        }
        File out = new File(extractRoot, name);
        File parent = out.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs() && !parent.isDirectory()) {
          throw new HopException("Unable to create directory: " + parent);
        }
        try (OutputStream os = Files.newOutputStream(out.toPath())) {
          zis.transferTo(os);
        }
        zis.closeEntry();
      }
    }
  }

  static File detectProjectHome(File extractRoot) throws HopException {
    File marker = new File(extractRoot, MARKER_ENTRY);
    if (marker.isFile()) {
      try {
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(marker.toPath())) {
          props.load(in);
        }
        String root = props.getProperty(PROP_PROJECT_ROOT, DEFAULT_PROJECT_ROOT);
        File project = new File(extractRoot, root);
        if (project.isDirectory()) {
          return project;
        }
      } catch (IOException e) {
        throw new HopException("Unable to read " + MARKER_ENTRY, e);
      }
    }
    File projectDir = new File(extractRoot, DEFAULT_PROJECT_ROOT);
    if (projectDir.isDirectory()) {
      return projectDir;
    }
    // GUI export style: single top-level folder
    File[] children = extractRoot.listFiles(f -> !f.getName().startsWith("."));
    if (children != null && children.length == 1 && children[0].isDirectory()) {
      return children[0];
    }
    return extractRoot;
  }

  private static String findMetadataPath(File extractRoot, File projectHome) {
    File atRoot = new File(extractRoot, METADATA_ENTRY);
    if (atRoot.isFile()) {
      return atRoot.getAbsolutePath();
    }
    File inProject = new File(projectHome, METADATA_ENTRY);
    if (inProject.isFile()) {
      return inProject.getAbsolutePath();
    }
    return null;
  }

  private static String relativePath(FileObject home, FileObject file) throws Exception {
    String homeUri = home.getName().getURI();
    String fileUri = file.getName().getURI();
    if (!fileUri.startsWith(homeUri)) {
      return null;
    }
    String rel = fileUri.substring(homeUri.length());
    while (rel.startsWith("/")) {
      rel = rel.substring(1);
    }
    return rel;
  }

  private static boolean shouldSkipRelative(String rel) {
    String[] parts = rel.replace('\\', '/').split("/");
    for (String part : parts) {
      if (part.startsWith(".") && part.length() > 1) {
        return true;
      }
      if (SKIP_DIR_NAMES.contains(part.toLowerCase(Locale.ROOT))) {
        return true;
      }
    }
    return false;
  }

  private static void deleteRecursively(File f) throws IOException {
    if (f == null || !f.exists()) {
      return;
    }
    if (f.isDirectory()) {
      File[] kids = f.listFiles();
      if (kids != null) {
        for (File k : kids) {
          deleteRecursively(k);
        }
      }
    }
    Files.deleteIfExists(f.toPath());
  }

  /** Clear JVM caches (tests). */
  public static void clearMaterializationCache() {
    MATERIALIZED.clear();
    DISTRIBUTED.clear();
  }
}
