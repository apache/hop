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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.HexFormat;
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
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fatjar.FatJarCache;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.spark.util.SparkHadoopConfiguration;

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
 * materialization extracts into {@code ${java.io.tmpdir}/hop-spark-pkg-&lt;key&gt;/}. Extracts are
 * reused only while the package zip fingerprint (size, mtime, sha-256) still matches — so
 * overwriting the same path on a shared volume is picked up on the next materialize.
 *
 * <p><strong>Not for bulk Dataset data.</strong> {@code PROJECT_HOME} after materialize points at
 * definition files. Use separate variables ({@code HOP_DATA}, {@code OUTPUT_ROOT}, {@code s3a://…})
 * for Spark File / Lake paths.
 */
public final class SparkProjectPackage {
  private static final String CONST_PROJECT_HOME = "PROJECT_HOME";
  private static final String CONST_FILE = "file:";

  /**
   * Bumped when worker package distribution behavior changes. Printed by MainSpark so Databricks
   * logs prove which fat jar is live (avoid debugging stale Volume jars).
   */
  public static final String DISTRIBUTION_BUILD_ID = "pkg-dist-v3-stage-volumes-for-addfile";

  /** Variable holding the package zip URI/path for driver and executor materialization. */
  public static final String VAR_PACKAGE_URI = "HOP_SPARK_PROJECT_PACKAGE";

  /**
   * Basename of the package after {@link org.apache.spark.SparkContext#addFile(String)} — workers
   * resolve it via {@link org.apache.spark.SparkFiles#get(String)}.
   */
  public static final String VAR_PACKAGE_SPARK_FILE = "HOP_SPARK_PROJECT_PACKAGE_SPARK_FILE";

  /**
   * {@code Y} when the engine exported the package itself for a client-mode run on a cluster. The
   * package is then only a fallback: a JVM that sees the real {@code PROJECT_HOME} (the driver, an
   * executor on the same machine or on a shared file system) keeps using it, so data paths under
   * the project home stay where they are.
   */
  public static final String VAR_PACKAGE_FALLBACK = "HOP_SPARK_PROJECT_PACKAGE_FALLBACK";

  /**
   * The project home of the client that submitted a cluster-mode run. A driver on a host that sees
   * this folder (one machine, a shared file system) uses it as {@code PROJECT_HOME}, as in client
   * mode.
   */
  public static final String VAR_CLIENT_PROJECT_HOME = "HOP_SPARK_CLIENT_PROJECT_HOME";

  public static final String METADATA_ENTRY = "metadata.json";

  /**
   * Variables of the client for a driver submitted in cluster mode: a JSON object name → value,
   * each value encoded with the two-way password encoder like the passwords in {@link
   * #METADATA_ENTRY}.
   */
  public static final String VARIABLES_ENTRY = "variables.json";

  public static final String MARKER_ENTRY = "hop-spark-package.properties";
  public static final String DEFAULT_PROJECT_ROOT = "project";
  public static final String PROP_PROJECT_ROOT = "projectRoot";

  private static final String COMPLETE_MARKER = ".hop-spark-package-complete";

  /**
   * packageUri cache key → extract root + fingerprint that was materialized (in-JVM). Disk marker
   * under the extract root carries the same fingerprint for long-lived worker JVMs.
   */
  private static final Map<String, MaterializedState> MATERIALIZED = new ConcurrentHashMap<>();

  /** applicationId|localPath keys already passed to SparkContext.addFile on this driver JVM. */
  private static final Set<String> DISTRIBUTED = ConcurrentHashMap.newKeySet();

  private record MaterializedState(String extractRoot, String fingerprint) {}

  private SparkProjectPackage() {}

  /** Result of materializing a package zip on this JVM. */
  public record Materialized(String projectHome, String metadataPath, String extractRoot) {}

  /**
   * Ensure the project package (if configured) is extracted and {@code PROJECT_HOME} points at the
   * project root. Safe to call from every mapPartitions init; materializes once per package path
   * per JVM.
   *
   * <p>Tries each candidate from {@link #listPackagePathsForMaterialize} until one materializes.
   * Never keeps a driver-only {@code PROJECT_HOME} under another node's {@code /tmp}. Never stores
   * SparkFiles paths back into {@link #VAR_PACKAGE_URI} (those are per-JVM and not portable).
   */
  public static void ensureMaterializedOnWorker(IVariables variables) throws HopException {
    if (variables == null) {
      return;
    }
    if ("Y".equalsIgnoreCase(variables.getVariable(VAR_PACKAGE_FALLBACK))) {
      String projectHome = variables.getVariable("PROJECT_HOME");
      if (StringUtils.isNotEmpty(projectHome) && new File(projectHome).isDirectory()) {
        return;
      }
    }
    String preservedUri = variables.getVariable(VAR_PACKAGE_URI);
    String sparkFile = variables.getVariable(VAR_PACKAGE_SPARK_FILE);

    // Drop driver-broadcast PROJECT_HOME when it is not a real directory on *this* JVM.
    clearStaleProjectHome(variables);

    java.util.List<String> candidates = listPackagePathsForMaterialize(variables);
    if (candidates.isEmpty()) {
      if (StringUtils.isNotEmpty(preservedUri)) {
        throw new HopException(
            "Could not resolve Spark project package on this host for materialization: "
                + variables.resolve(preservedUri)
                + " (sparkFile="
                + sparkFile
                + "). PROJECT_HOME was "
                + variables.getVariable(CONST_PROJECT_HOME)
                + ". On Databricks Deploy & run, keep the package on a UC Volume (/Volumes/…) and"
                + " ensure the fat jar stages a SparkFiles copy for executors that cannot open"
                + " Volumes via java.io.File / Hop VFS.");
      }
      return;
    }

    HopException lastError = null;
    Materialized m = null;
    String usedPath = null;
    for (String packagePath : candidates) {
      try {
        m = materialize(packagePath);
        usedPath = packagePath;
        break;
      } catch (HopException e) {
        lastError = e;
      } catch (Exception e) {
        lastError =
            new HopException("Error materializing Spark project package: " + packagePath, e);
      }
    }
    if (m == null) {
      throw new HopException(
          "Could not materialize Spark project package from any candidate path: "
              + candidates
              + ". Last error: "
              + (lastError != null ? lastError.getMessage() : "none"),
          lastError);
    }

    // Never persist SparkFiles paths in VAR_PACKAGE_URI (not valid on other JVMs)
    String uriToKeep = preservedUri;
    if (StringUtils.isEmpty(uriToKeep) || isSparkFilesLocalPath(uriToKeep)) {
      uriToKeep = !isSparkFilesLocalPath(usedPath) ? usedPath : preservedUri;
    }
    // Always overwrite PROJECT_HOME with *this* JVM's extract (not the driver's /tmp path).
    applyToVariables(variables, m, uriToKeep);
    if (StringUtils.isNotEmpty(sparkFile)) {
      variables.setVariable(VAR_PACKAGE_SPARK_FILE, sparkFile);
    }

    String home = variables.getVariable(CONST_PROJECT_HOME);
    if (StringUtils.isEmpty(home) || !new File(home).isDirectory()) {
      throw new HopException(
          "Spark project package materialize did not produce a PROJECT_HOME directory on this"
              + " host (got '"
              + home
              + "') from package path "
              + usedPath
              + ". Package URI="
              + uriToKeep
              + ".");
    }
  }

  /**
   * Drop {@code PROJECT_HOME} when it does not exist as a directory on this JVM (typical: driver
   * {@code /local_disk0/tmp/hop-spark-pkg-…} broadcast to executors).
   */
  static void clearStaleProjectHome(IVariables variables) {
    if (variables == null) {
      return;
    }
    String projectHome = variables.getVariable(CONST_PROJECT_HOME);
    if (StringUtils.isEmpty(projectHome)) {
      return;
    }
    if (!new File(projectHome).isDirectory()) {
      variables.setVariable(CONST_PROJECT_HOME, null);
    }
  }

  /**
   * Resolve the preferred zip path to open on this JVM (first candidate from {@link
   * #listPackagePathsForMaterialize}).
   */
  public static String resolvePackagePathForMaterialize(IVariables variables) {
    java.util.List<String> paths = listPackagePathsForMaterialize(variables);
    return paths.isEmpty() ? null : paths.get(0);
  }

  /**
   * Ordered candidate package zip paths for this JVM.
   *
   * <ol>
   *   <li>{@code SparkFiles.get(basename)} when the file exists (reliable after driver {@code
   *       addFile} of a <em>local</em> copy)
   *   <li>{@code $HOP_DATA/packages/<basename>} (shared data plane)
   *   <li>Cluster-shared {@link #VAR_PACKAGE_URI} (UC {@code /Volumes/…}, {@code /dbfs/}, object
   *       store) — returned even when {@link File#isFile()} is false (FUSE lag / executor mount
   *       quirks); open is attempted via Hop VFS in {@link #materialize}
   *   <li>{@link #VAR_PACKAGE_URI} when that path exists locally, or is a remote {@code scheme://}
   *       URI
   * </ol>
   */
  @SuppressWarnings("javabugs:S2259") // guarded by StringUtils.isNotEmpty
  public static java.util.List<String> listPackagePathsForMaterialize(IVariables variables) {
    java.util.LinkedHashSet<String> ordered = new java.util.LinkedHashSet<>();
    if (variables == null) {
      return java.util.List.of();
    }
    String rawUri = variables.getVariable(VAR_PACKAGE_URI);
    String resolvedUri = StringUtils.isNotEmpty(rawUri) ? variables.resolve(rawUri) : null;

    String basename = variables.getVariable(VAR_PACKAGE_SPARK_FILE);
    if (StringUtils.isEmpty(basename) && StringUtils.isNotEmpty(resolvedUri)) {
      File f = resolveLocalPackageFile(resolvedUri);
      if (f != null) {
        basename = f.getName();
      } else {
        int slash = Math.max(resolvedUri.lastIndexOf('/'), resolvedUri.lastIndexOf('\\'));
        if (slash >= 0 && slash < resolvedUri.length() - 1) {
          basename = resolvedUri.substring(slash + 1);
        }
      }
    }

    // 1) SparkFiles first — local on every executor after addFile(localCopy)
    if (StringUtils.isNotEmpty(basename)) {
      String downloaded = findSparkFile(basename.trim());
      if (downloaded != null) {
        ordered.add(downloaded);
      }
    }

    // 2) Shared data plane: $HOP_DATA/packages/<basename>
    if (StringUtils.isNotEmpty(basename)) {
      String hopData = variables.getVariable("HOP_DATA");
      if (StringUtils.isNotEmpty(hopData)) {
        String dataRoot = variables.resolve(hopData).replace("file://", "");
        while (dataRoot.endsWith("/") || dataRoot.endsWith("\\")) {
          dataRoot = dataRoot.substring(0, dataRoot.length() - 1);
        }
        File shared = new File(dataRoot + File.separator + "packages", basename.trim());
        if (shared.isFile()) {
          ordered.add(shared.getAbsolutePath());
        }
      }
    }

    // 3) Cluster-shared URI (Volumes / DBFS / object store) — try even if File.isFile is false
    if (StringUtils.isNotEmpty(resolvedUri) && isClusterSharedPackagePath(resolvedUri)) {
      ordered.add(resolvedUri);
    }

    // 4) Explicit package URI (local file or remote VFS)
    if (StringUtils.isNotEmpty(resolvedUri) && !ordered.contains(resolvedUri)) {
      if (resolveLocalPackageFile(resolvedUri) != null) {
        ordered.add(resolvedUri);
      } else if (resolvedUri.contains("://") && !resolvedUri.startsWith(CONST_FILE)) {
        ordered.add(resolvedUri);
      }
    }
    return new java.util.ArrayList<>(ordered);
  }

  /** True for Spark's ephemeral per-JVM download dirs (not safe to ship in variables). */
  static boolean isSparkFilesLocalPath(String path) {
    if (StringUtils.isEmpty(path)) {
      return false;
    }
    // SparkFiles root looks like …/userFiles-<uuid>/basename — never treat as portable URI
    return path.replace('\\', '/').contains("/userFiles-");
  }

  /**
   * True when every cluster node can open the same package path without SparkFiles (Databricks UC
   * Volumes, classic DBFS mount, or remote object-store URI).
   */
  static boolean isClusterSharedPackagePath(String path) {
    if (StringUtils.isEmpty(path)) {
      return false;
    }
    String p = path.trim().replace('\\', '/');
    if (p.startsWith("/Volumes/") || p.startsWith("/dbfs/")) {
      return true;
    }
    if (p.startsWith("dbfs:")) {
      return true;
    }
    // Object store / remote VFS (not local file:)
    int scheme = p.indexOf("://");
    return scheme > 0 && !p.startsWith(CONST_FILE);
  }

  /**
   * Make the project package available on executors.
   *
   * <p>Always stages a <strong>local filesystem copy</strong> and registers it with {@code
   * SparkContext.addFile} when possible, setting {@link #VAR_PACKAGE_SPARK_FILE}. Do <em>not</em>
   * call {@code addFile} on a UC Volume path directly — Databricks often fails with {@code Stream
   * '/files/…' was not found}; copy to {@code java.io.tmpdir} first via {@link
   * #ensureLocalPackageFile}.
   *
   * <p>When {@link #VAR_PACKAGE_URI} is already on a <strong>cluster-shared</strong> path (UC
   * {@code /Volumes/…}, {@code /dbfs/}, {@code s3a://…}, …), that portable URI is kept so workers
   * can also open the shared path; the SparkFiles copy is the reliable fallback when Volume FUSE is
   * not visible the same way on every executor JVM.
   */
  @SuppressWarnings("java:S106") // runs on the driver, next to MainSpark's progress
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

    boolean clusterShared = isClusterSharedPackagePath(uri);
    if (clusterShared) {
      // Keep the portable shared URI for workers that can open it directly.
      variables.setVariable(VAR_PACKAGE_URI, uri);
    }

    // Stage a *true* local filesystem copy for addFile. Never pass /Volumes or dbfs paths to
    // addFile — Databricks often fails with "Stream '/files/…' was not found".
    String localPath;
    try {
      localPath = ensureLocalPackageFile(uri);
    } catch (HopException e) {
      if (clusterShared) {
        System.err.println(
            ">>>>>> WARNING: could not stage Spark project package for SparkFiles ("
                + e.getMessage()
                + "); workers will try cluster-shared URI only: "
                + uri);
        return;
      }
      throw e;
    }
    if (isClusterSharedPackagePath(localPath) || isSparkFilesLocalPath(localPath)) {
      // ensureLocalPackageFile must not return a Volume path (addFile would fail).
      throw new HopException(
          "Staged package path is not a plain local file (still looks cluster-shared or"
              + " SparkFiles): "
              + localPath
              + " (source "
              + uri
              + ")");
    }
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
    boolean addFileOk = DISTRIBUTED.contains(distKey);
    if (DISTRIBUTED.add(distKey)) {
      try {
        addFileToSession(spark, localPath);
        addFileOk = true;
      } catch (Exception e) {
        DISTRIBUTED.remove(distKey);
        addFileOk = false;
        if (!clusterShared) {
          throw new HopException(
              "Failed to distribute Spark project package via SparkContext.addFile: " + localPath,
              e);
        }
        System.err.println(
            ">>>>>> WARNING: SparkContext.addFile failed for staged package "
                + localPath
                + " ("
                + e.getMessage()
                + "); workers will try cluster-shared URI only: "
                + uri);
      }
    }

    // Non-shared: rewrite URI to the local staged path used for distribution.
    if (!clusterShared && !isSparkFilesLocalPath(localPath)) {
      variables.setVariable(VAR_PACKAGE_URI, localPath);
    }
    // Only advertise SparkFiles basename when addFile succeeded (or was already registered).
    if (addFileOk) {
      variables.setVariable(VAR_PACKAGE_SPARK_FILE, basename);
    }
  }

  /**
   * The executor-local copy of a file added with {@code addFile}. Spark 4 stores the files of an
   * isolated session in a {@code <session uuid>} folder under the SparkFiles root, where {@code
   * SparkFiles.get} does not look. Package names carry their content hash, so a match in any
   * session folder is the right file.
   *
   * @return the local path, or null when the file is not on this JVM
   */
  static String findSparkFile(String basename) {
    try {
      String downloaded = org.apache.spark.SparkFiles.get(basename);
      if (StringUtils.isNotEmpty(downloaded) && resolveLocalPackageFile(downloaded) != null) {
        return downloaded;
      }
      File[] sessionFolders =
          new File(org.apache.spark.SparkFiles.getRootDirectory()).listFiles(File::isDirectory);
      if (sessionFolders != null) {
        for (File folder : sessionFolders) {
          File candidate = new File(folder, basename);
          if (candidate.isFile()) {
            return candidate.getAbsolutePath();
          }
        }
      }
    } catch (Throwable t) {
      // not distributed or not ready
    }
    return null;
  }

  /**
   * {@code SparkContext.addFile} under the session's artifact state.
   *
   * <p>Spark 4 classic sessions isolate artifacts: a job started by a Dataset action only ships the
   * files added under that session's state to the executors. A file added outside it lands in the
   * global set and never reaches a task of a Dataset write, so executors on a cluster could not
   * find the package. Files added under the session state are part of {@code allAddedFiles} too, so
   * plain RDD jobs still get them.
   */
  static void addFileToSession(org.apache.spark.sql.SparkSession spark, String localPath) {
    if (spark instanceof org.apache.spark.sql.classic.SparkSession classic) {
      classic
          .artifactManager()
          .withResources(
              () -> {
                spark.sparkContext().addFile(localPath);
                return scala.runtime.BoxedUnit.UNIT;
              });
    } else {
      spark.sparkContext().addFile(localPath);
    }
  }

  /**
   * Ensure {@code packageUri} is a plain local filesystem path suitable for {@code
   * SparkContext.addFile}.
   *
   * <p><strong>UC Volumes / DBFS / object-store URIs are always copied</strong> into {@code
   * java.io.tmpdir}. Returning {@code /Volumes/…} directly looks like a local file on the driver
   * ({@link File#isFile()} is true) but {@code addFile} of that path fails on Databricks executors.
   */
  @SuppressWarnings("javabugs:S2259") // the URI is resolved and non-null at this point
  public static String ensureLocalPackageFile(String packageUri) throws HopException {
    if (StringUtils.isEmpty(packageUri)) {
      throw new HopException("Spark project package URI is empty");
    }
    String uri = packageUri.trim();

    // Always stage cluster-shared paths to a real local temp zip for addFile.
    if (isClusterSharedPackagePath(uri)) {
      return stagePackageToLocalTemp(uri);
    }

    File asFile = new File(uri);
    if (asFile.isFile()) {
      return asFile.getAbsolutePath();
    }
    // file:/ URI without Hop VFS
    if (uri.startsWith(CONST_FILE)) {
      try {
        File f = new File(java.net.URI.create(uri));
        if (f.isFile()) {
          return f.getAbsolutePath();
        }
      } catch (Exception ignored) {
        // fall through to HopVfs
      }
    }
    return stagePackageToLocalTemp(uri);
  }

  /**
   * Copy {@code packageUri} into {@code java.io.tmpdir}/hop-spark-pkg-src-&lt;key&gt;.zip when
   * missing or stale (size / mtime vs local source when visible).
   */
  static String stagePackageToLocalTemp(String packageUri) throws HopException {
    if (StringUtils.isEmpty(packageUri)) {
      throw new HopException("Spark project package URI is empty");
    }
    File local =
        new File(
            System.getProperty("java.io.tmpdir"),
            "hop-spark-pkg-src-" + cacheKey(packageUri) + ".zip");
    boolean needCopy = !local.isFile() || local.length() == 0;
    File sourceFile = resolveLocalPackageFile(packageUri);
    if (!needCopy && sourceFile != null) {
      if (sourceFile.length() != local.length()
          || sourceFile.lastModified() > local.lastModified()) {
        needCopy = true;
      }
    }
    if (needCopy) {
      try {
        if (local.exists() && !local.delete() && local.isFile()) {
          // overwrite via FileOutputStream truncate below
        }
        try (InputStream in = openPackageStream(packageUri);
            OutputStream out = Files.newOutputStream(local.toPath())) {
          in.transferTo(out);
        }
      } catch (HopException e) {
        throw e;
      } catch (Exception e) {
        throw new HopException(
            "Unable to stage Spark project package locally for distribution: " + packageUri, e);
      }
    }
    if (!local.isFile() || local.length() == 0) {
      throw new HopException(
          "Staged Spark project package is empty or missing after copy: "
              + local.getAbsolutePath()
              + " (source "
              + packageUri
              + ")");
    }
    return local.getAbsolutePath();
  }

  /** Set package URI + PROJECT_HOME (+ optional metadata path helper is caller-side). */
  public static void applyToVariables(IVariables variables, Materialized m, String packageUri) {
    if (variables == null || m == null) {
      return;
    }
    if (StringUtils.isNotEmpty(packageUri)) {
      variables.setVariable(VAR_PACKAGE_URI, packageUri);
    }
    variables.setVariable(CONST_PROJECT_HOME, m.projectHome());
  }

  /**
   * Extract {@code packageUri} under the JVM temp directory if not already done for the current
   * package fingerprint (size + mtime + sha-256).
   *
   * @param packageUri path or VFS URI to the package zip
   */
  public static Materialized materialize(String packageUri) throws HopException {
    if (StringUtils.isEmpty(packageUri)) {
      throw new HopException("Spark project package URI is empty");
    }
    String key = cacheKey(packageUri);
    String fingerprint;
    try {
      fingerprint = fingerprintPackage(packageUri);
    } catch (Exception e) {
      throw new HopException("Unable to fingerprint Spark project package: " + packageUri, e);
    }

    MaterializedState state = MATERIALIZED.get(key);
    if (state != null
        && fingerprint.equals(state.fingerprint())
        && isUsableExtractRoot(state.extractRoot())) {
      return buildMaterialized(state.extractRoot(), packageUri);
    }
    synchronized (MATERIALIZED) {
      state = MATERIALIZED.get(key);
      if (state != null
          && fingerprint.equals(state.fingerprint())
          && isUsableExtractRoot(state.extractRoot())) {
        return buildMaterialized(state.extractRoot(), packageUri);
      }
      try {
        File extractRoot = new File(System.getProperty("java.io.tmpdir"), "hop-spark-pkg-" + key);
        File complete = new File(extractRoot, COMPLETE_MARKER);
        String stored = complete.isFile() ? Files.readString(complete.toPath()).trim() : null;
        boolean fingerprintMatches =
            fingerprint.equals(stored) || isLegacyCompleteMarker(stored, packageUri, fingerprint);

        if (!fingerprintMatches || !isUsableExtractRoot(extractRoot.getAbsolutePath())) {
          if (extractRoot.exists()) {
            deleteRecursively(extractRoot);
          }
          if (!extractRoot.mkdirs() && !extractRoot.isDirectory()) {
            throw new HopException("Unable to create package extract dir: " + extractRoot);
          }
          extractZip(packageUri, extractRoot);
          Files.writeString(complete.toPath(), fingerprint, StandardCharsets.UTF_8);
        }
        MATERIALIZED.put(key, new MaterializedState(extractRoot.getAbsolutePath(), fingerprint));
        return buildMaterialized(extractRoot.getAbsolutePath(), packageUri);
      } catch (HopException e) {
        throw e;
      } catch (Exception e) {
        throw new HopException("Error materializing Spark project package: " + packageUri, e);
      }
    }
  }

  /** True when extract root exists and {@link #detectProjectHome} yields a directory. */
  static boolean isUsableExtractRoot(String extractRootPath) {
    if (StringUtils.isEmpty(extractRootPath)) {
      return false;
    }
    File extractRoot = new File(extractRootPath);
    if (!extractRoot.isDirectory()) {
      return false;
    }
    try {
      return detectProjectHome(extractRoot).isDirectory();
    } catch (HopException e) {
      return false;
    }
  }

  /**
   * Older markers stored only the package URI. Accept them only when size/mtime still match the
   * current zip so first run after upgrade does not always re-extract unnecessarily when content is
   * unchanged; if fingerprint fields cannot be reconciled, force re-extract by returning false.
   */
  static boolean isLegacyCompleteMarker(String stored, String packageUri, String fingerprint) {
    if (StringUtils.isEmpty(stored) || stored.contains("sha256=")) {
      return false;
    }
    // Legacy: marker was just the URI string
    if (!stored.equals(packageUri.trim()) && !stored.equals(packageUri)) {
      return false;
    }
    // URI-only markers cannot prove content identity — always re-extract for safety
    return false;
  }

  /**
   * Fingerprint a package zip for cache invalidation: size, last-modified (local files), and
   * SHA-256 of contents.
   */
  static String fingerprintPackage(String packageUri) throws Exception {
    File local = resolveLocalPackageFile(packageUri);
    long size = -1L;
    long mtime = -1L;
    if (local != null) {
      size = local.length();
      mtime = local.lastModified();
    } else {
      try {
        FileObject fo = HopVfs.getFileObject(packageUri);
        if (fo.exists() && fo.getType() == FileType.FILE) {
          size = fo.getContent().getSize();
          mtime = fo.getContent().getLastModifiedTime();
        }
      } catch (Exception ignored) {
        // size/mtime optional for remote; sha still required
      }
    }

    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (InputStream raw = openPackageStream(packageUri);
        DigestInputStream din = new DigestInputStream(new BufferedInputStream(raw), digest)) {
      byte[] buf = new byte[8192];
      while (din.read(buf) >= 0) {
        // drain
      }
    }
    String sha = HexFormat.of().formatHex(digest.digest());
    return "uri="
        + packageUri.trim()
        + "\nsize="
        + size
        + "\nmtime="
        + mtime
        + "\nsha256="
        + sha
        + "\n";
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
   * <p>Loads {@link PackageExportFilter#CONFIG_FILENAME} from the project home when present and
   * merges it with built-in default exclude dirs.
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
    exportProject(projectHome, zipFilename, metadataProvider, variables, null);
  }

  /**
   * Export with an explicit filter layer (CLI/GUI). Merge order: built-in defaults (via empty
   * filter) → {@code spark-package.json} in project home → {@code explicitFilter}.
   */
  public static void exportProject(
      String projectHome,
      String zipFilename,
      IHopMetadataProvider metadataProvider,
      IVariables variables,
      PackageExportFilter explicitFilter)
      throws HopException {
    exportProject(projectHome, zipFilename, metadataProvider, variables, explicitFilter, null);
  }

  /**
   * Export with variables for a driver that does not run in this process: they go into {@link
   * #VARIABLES_ENTRY} and {@code MainSpark} applies them.
   *
   * @param packagedVariables variables to ship, or null for none
   */
  public static void exportProject(
      String projectHome,
      String zipFilename,
      IHopMetadataProvider metadataProvider,
      IVariables variables,
      PackageExportFilter explicitFilter,
      Map<String, String> packagedVariables)
      throws HopException {
    exportProject(
        projectHome,
        zipFilename,
        metadataProvider,
        variables,
        explicitFilter,
        packagedVariables,
        Map.of());
  }

  /**
   * @param extraProjectFiles files to add to the project tree, by their path relative to the
   *     project home, such as an unsaved pipeline that a cluster-mode run submits; they win over a
   *     project file with the same path
   */
  public static void exportProject(
      String projectHome,
      String zipFilename,
      IHopMetadataProvider metadataProvider,
      IVariables variables,
      PackageExportFilter explicitFilter,
      Map<String, String> packagedVariables,
      Map<String, byte[]> extraProjectFiles)
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

    PackageExportFilter filter =
        PackageExportFilter.empty()
            .merge(PackageExportFilter.loadFromProjectHome(realHome))
            .merge(explicitFilter);

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

        // Marker. Written by hand: Properties.store adds the current date, and the package must
        // be the same bytes for the same project to keep its content-addressed name.
        zos.putNextEntry(entry(MARKER_ENTRY));
        zos.write(
            ("# Apache Hop Native Spark project package\n"
                    + PROP_PROJECT_ROOT
                    + "="
                    + DEFAULT_PROJECT_ROOT
                    + "\nformat=hop-spark-project-package\nformatVersion=1\n")
                .getBytes(StandardCharsets.ISO_8859_1));
        zos.closeEntry();

        // Metadata at zip root
        String metadataJson = new SerializableMetadataProvider(metadataProvider).toJson();
        zos.putNextEntry(entry(METADATA_ENTRY));
        zos.write(metadataJson.getBytes(StandardCharsets.UTF_8));
        zos.closeEntry();

        if (packagedVariables != null) {
          // Values can be passwords: encode them like the passwords in metadata.json next to it
          Map<String, String> encoded = new java.util.TreeMap<>();
          packagedVariables.forEach(
              (name, value) ->
                  encoded.put(name, value == null ? null : Encr.getEncoder().encode(value, true)));
          zos.putNextEntry(entry(VARIABLES_ENTRY));
          zos.write(
              new com.fasterxml.jackson.databind.ObjectMapper()
                  .writerWithDefaultPrettyPrinter()
                  .writeValueAsBytes(encoded));
          zos.closeEntry();
        }

        // Project tree under project/
        Set<String> written = new HashSet<>();
        if (extraProjectFiles != null) {
          for (Map.Entry<String, byte[]> extra :
              new java.util.TreeMap<>(extraProjectFiles).entrySet()) {
            String entryName = DEFAULT_PROJECT_ROOT + "/" + extra.getKey().replace('\\', '/');
            written.add(entryName);
            zos.putNextEntry(entry(entryName));
            zos.write(extra.getValue());
            zos.closeEntry();
          }
        }
        home.findFiles(
            new FileSelector() {
              @Override
              public boolean includeFile(FileSelectInfo fileInfo) throws Exception {
                FileObject fo = fileInfo.getFile();
                if (fo.getType() != FileType.FILE) {
                  return false;
                }
                String rel = relativePath(home, fo);
                if (rel == null || filter.shouldSkipRelative(rel)) {
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
                zos.putNextEntry(entry(entryName));
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
                return !filter.shouldSkipDirectoryBasename(base, fileInfo.getDepth());
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
   * The modification time of every package entry. A zip stamps entries with the time they were
   * written; a fixed time makes the same project the same bytes, so a cluster run stages its
   * package only when the project changed.
   */
  static final java.time.LocalDateTime ENTRY_TIME = java.time.LocalDateTime.of(2000, 1, 1, 0, 0);

  private static ZipEntry entry(String name) {
    ZipEntry entry = new ZipEntry(name);
    entry.setTimeLocal(ENTRY_TIME);
    return entry;
  }

  /**
   * Export the project for executors that do not share this client's disk, named after the content
   * hash ({@code hop-spark-package-<sha12>.zip} in the temporary folder). An unchanged project
   * gives the same file name, so a reused Spark session does not register it with {@code addFile}
   * twice.
   *
   * @return the local path of the package zip
   */
  public static String exportForCluster(
      String projectHome,
      IHopMetadataProvider metadataProvider,
      IVariables variables,
      ILogChannel log)
      throws HopException {
    return exportForCluster(projectHome, metadataProvider, variables, log, null);
  }

  /**
   * @param packagedVariables variables for a driver that runs elsewhere (cluster mode), or null
   */
  public static String exportForCluster(
      String projectHome,
      IHopMetadataProvider metadataProvider,
      IVariables variables,
      ILogChannel log,
      Map<String, String> packagedVariables)
      throws HopException {
    return exportForCluster(
        projectHome, metadataProvider, variables, log, packagedVariables, Map.of());
  }

  /** Days an exported package is kept after the last run that exported it. */
  static final Duration PACKAGE_RETENTION = Duration.ofDays(7);

  /**
   * @param extraProjectFiles files to add to the project tree, see {@link #exportProject(String,
   *     String, IHopMetadataProvider, IVariables, PackageExportFilter, Map, Map)}
   */
  public static String exportForCluster(
      String projectHome,
      IHopMetadataProvider metadataProvider,
      IVariables variables,
      ILogChannel log,
      Map<String, String> packagedVariables,
      Map<String, byte[]> extraProjectFiles)
      throws HopException {
    Path folder =
        packageFolder(System.getProperty("java.io.tmpdir"), System.getProperty("user.name"));
    Path temp;
    try {
      createPrivateFolder(folder);
      temp = Files.createTempFile(folder, "export-", ".zip");
    } catch (IOException e) {
      throw new HopException("Unable to create a Spark project package in " + folder, e);
    }
    try {
      exportProject(
          projectHome,
          temp.toString(),
          metadataProvider,
          variables,
          null,
          packagedVariables,
          extraProjectFiles);
      String sha = FatJarCache.sha256Hex(temp);
      Path target = folder.resolve("hop-spark-package-" + sha.substring(0, 12) + ".zip");
      if (Files.isRegularFile(target)) {
        // Used again: the pruning goes by the modification time
        Files.setLastModifiedTime(target, FileTime.from(Instant.now()));
      } else {
        Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE);
      }
      prunePackages(folder, target, Instant.now());
      if (log != null) {
        log.logBasic(
            "Exported project "
                + variables.resolve(projectHome)
                + " for the Spark executors: "
                + target
                + " ("
                + Files.size(target) / 1024
                + " kB). Exclude folders with spark-package.json in the project home.");
      }
      return target.toString();
    } catch (IOException e) {
      throw new HopException("Unable to store the Spark project package in " + folder, e);
    } finally {
      try {
        Files.deleteIfExists(temp);
      } catch (IOException e) {
        // best effort
      }
    }
  }

  /**
   * The folder exported packages go to: one per OS user in the temporary folder. On a shared host
   * such as an edge node another user's folder is not writable, and a package holds the run's
   * variables, passwords included: only its user reads it.
   */
  static Path packageFolder(String temporaryFolder, String userName) {
    String user = StringUtils.defaultIfBlank(userName, "hop").replaceAll("[^A-Za-z0-9._-]", "_");
    return Paths.get(temporaryFolder, "hop-spark-packages-" + user);
  }

  private static void createPrivateFolder(Path folder) throws IOException {
    if (Files.isDirectory(folder)) {
      return;
    }
    Files.createDirectories(folder);
    try {
      Files.setPosixFilePermissions(folder, PosixFilePermissions.fromString("rwx------"));
    } catch (UnsupportedOperationException e) {
      // Not a POSIX file system: the folder is in the user's own temporary folder there
    }
  }

  /**
   * Remove the packages no run exported for {@link #PACKAGE_RETENTION}, and temporary exports left
   * by a run that did not finish. A package a session still serves is exported again by the next
   * run that uses it.
   */
  static void prunePackages(Path folder, Path keep, Instant now) {
    Instant cutoff = now.minus(PACKAGE_RETENTION);
    try (DirectoryStream<Path> files =
        Files.newDirectoryStream(folder, "{hop-spark-package-*.zip,export-*.zip}")) {
      for (Path file : files) {
        try {
          if (!file.equals(keep) && Files.getLastModifiedTime(file).toInstant().isBefore(cutoff)) {
            Files.deleteIfExists(file);
          }
        } catch (IOException e) {
          // Another run removed it, or it is in use: try again next time
        }
      }
    } catch (IOException e) {
      // Pruning never fails a run
    }
  }

  /**
   * Whether a folder holds the project of a package: each project file of the package is in it,
   * with the same content. A driver uses the client's project folder only then; a folder at that
   * path on another host can be an old copy of the project, or another one. Files the package left
   * out (excluded folders) don't count.
   */
  public static boolean holdsTheProjectOf(Materialized materialized, String folder) {
    Path packaged = Paths.get(materialized.projectHome());
    Path candidate = Paths.get(folder);
    try (java.util.stream.Stream<Path> files = Files.walk(packaged)) {
      for (Path file : (Iterable<Path>) files.filter(Files::isRegularFile)::iterator) {
        Path other = candidate.resolve(packaged.relativize(file).toString());
        if (!Files.isRegularFile(other)
            || Files.size(other) != Files.size(file)
            || Files.mismatch(file, other) != -1L) {
          return false;
        }
      }
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * The variables a cluster-mode client put in the package, or an empty map.
   *
   * @param materialized the extracted package
   */
  public static Map<String, String> readPackagedVariables(Materialized materialized)
      throws HopException {
    File file = new File(materialized.extractRoot(), VARIABLES_ENTRY);
    if (!file.isFile()) {
      return Map.of();
    }
    try {
      Map<String, String> encoded =
          new com.fasterxml.jackson.databind.ObjectMapper()
              .readValue(
                  file,
                  new com.fasterxml.jackson.core.type.TypeReference<Map<String, String>>() {});
      Map<String, String> variables = new java.util.LinkedHashMap<>();
      encoded.forEach(
          (name, value) ->
              variables.put(name, value == null ? null : Encr.getEncoder().decode(value, true)));
      return variables;
    } catch (IOException e) {
      throw new HopException("Unable to read " + file, e);
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
    if (!rawPath.contains(CONST_PROJECT_HOME) && !rawPath.contains("${PROJECT_HOME}")) {
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
    // Prefer java.io for local paths (incl. SparkFiles.get downloads). HopVfs often fails to
    // resolve bare absolute paths like /tmp/spark-…/userFiles-…/pkg.zip without a file: scheme.
    try (InputStream raw = openPackageStream(packageUri);
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

  /**
   * Open the package zip for reading. Local filesystem first (SparkFiles / prepare-dist paths),
   * then Hop VFS for remote URIs.
   */
  /**
   * Open a URL the way Spark itself would: through the Hadoop file systems of the cluster ({@code
   * hdfs://}, {@code s3a://}, {@code http(s)://}, ...). A driver submitted in cluster mode reads
   * its package from the staging location this way, where Hop VFS has no connection for it.
   *
   * @return the stream, or null when the URL has no scheme or Hadoop cannot open it
   */
  static InputStream openWithHadoopFileSystem(String uri) {
    if (!uri.contains("://") || uri.startsWith("file:")) {
      return null;
    }
    try {
      org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(uri);
      org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(sparkHadoopConfiguration());
      return fs.open(path);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * The Hadoop configuration Spark gives this JVM: the spark.hadoop.* settings of the run, where
   * the Spark settings table puts the s3a endpoint and keys, on top of the defaults. spark-submit
   * hands a driver its settings as system properties, before there is a Spark session; an executor
   * has them in its SparkEnv.
   */
  static org.apache.hadoop.conf.Configuration sparkHadoopConfiguration() {
    Map<String, String> settings = new java.util.LinkedHashMap<>();
    try {
      for (scala.Tuple2<String, String> setting : new org.apache.spark.SparkConf(true).getAll()) {
        settings.put(setting._1(), setting._2());
      }
      org.apache.spark.SparkEnv env = org.apache.spark.SparkEnv.get();
      if (env != null) {
        for (scala.Tuple2<String, String> setting : env.conf().getAll()) {
          settings.put(setting._1(), setting._2());
        }
      }
    } catch (Throwable e) {
      // No Spark settings to add: Hadoop's defaults
    }
    return SparkHadoopConfiguration.hadoopConfiguration(settings);
  }

  static InputStream openPackageStream(String packageUri) throws Exception {
    if (StringUtils.isEmpty(packageUri)) {
      throw new HopException("Spark project package URI is empty");
    }
    String uri = packageUri.trim();

    File local = resolveLocalPackageFile(uri);
    if (local != null) {
      return new FileInputStream(local);
    }

    try {
      FileObject zipFo = HopVfs.getFileObject(uri);
      if (zipFo.exists()) {
        return HopVfs.getInputStream(zipFo);
      }
    } catch (Exception e) {
      InputStream cluster = openWithHadoopFileSystem(uri);
      if (cluster != null) {
        return cluster;
      }
      throw new HopException(
          "Spark project package not found: "
              + uri
              + ". On workers prefer a shared path ($HOP_DATA/packages/<zip>) or SparkFiles "
              + "distribution (spark-submit --files / SparkContext.addFile). Local check failed; "
              + "HopVfs error: "
              + e.getMessage(),
          e);
    }
    InputStream cluster = openWithHadoopFileSystem(uri);
    if (cluster != null) {
      return cluster;
    }
    throw new HopException(
        "Spark project package not found: "
            + uri
            + ". On workers copy the zip to $HOP_DATA/packages/ (shared volume) and/or pass "
            + "spark-submit --files. SparkFiles paths under /tmp/spark-*/userFiles-* are "
            + "per-JVM and must not be shared via variables.");
  }

  /**
   * If {@code uri} denotes an existing local file (plain path or {@code file:} URI), return it;
   * otherwise {@code null}.
   */
  static File resolveLocalPackageFile(String uri) {
    if (StringUtils.isEmpty(uri)) {
      return null;
    }
    try {
      if (uri.startsWith(CONST_FILE)) {
        Path p = Path.of(java.net.URI.create(uri));
        File f = p.toFile();
        return f.isFile() ? f : null;
      }
    } catch (Exception ignored) {
      // fall through to plain path
    }
    File plain = new File(uri);
    if (plain.isFile()) {
      return plain;
    }
    // SparkFiles sometimes returns a path that is not yet a regular file but is readable
    if (plain.exists() && plain.canRead() && plain.length() > 0) {
      return plain;
    }
    return null;
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

  /** Visible for tests — current in-JVM materialize fingerprint for a package URI, or null. */
  static String cachedFingerprintForTest(String packageUri) {
    MaterializedState state = MATERIALIZED.get(cacheKey(packageUri));
    return state == null ? null : state.fingerprint();
  }
}
