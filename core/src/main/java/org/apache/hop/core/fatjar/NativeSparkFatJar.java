/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.fatjar;

import java.io.File;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;

/**
 * Which installed jars go into a fat jar for the native Spark engine (Spark 4, Scala 2.13).
 *
 * <p>Neither flavor contains the Beam engine: its plugin folder, and every jar only the Beam engine
 * installs according to the plugin manifests ({@link PluginOwnedFiles}). Jars Beam shares with
 * other plugins stay.
 *
 * <p>Two flavors:
 *
 * <ul>
 *   <li><b>native</b>: leaves out Beam's Spark 3 / Scala 2.12 client classpath and keeps the Spark
 *       4 runtime of {@code plugins/engines/spark/lib}.
 *   <li><b>native-provided</b>: also leaves out every Spark, Scala and Hadoop client runtime jar,
 *       because the cluster provides them. This is the flavor for a real cluster: standalone, YARN
 *       (where {@code spark.yarn.jars} ships the engine's Spark runtime), Kubernetes and
 *       Databricks.
 * </ul>
 *
 * <p>Shared by the Spark engine (automatic fat jar), the Databricks deployment and {@code hop-conf
 * --generate-fat-jar --fat-jar-engine=spark}.
 */
public final class NativeSparkFatJar {

  /**
   * Written by the engine plugin at build time: its Maven dependency tree, which tells what a Spark
   * distribution of the engine's Spark version ships.
   */
  public static final String SPARK_DEPENDENCIES_FILE =
      "plugins/engines/spark/spark-dependencies.txt";

  /** Cache flavor of a native-provided fat jar: {@code hop-native-provided-<fingerprint>.jar}. */
  public static final String FLAVOR_PROVIDED = "hop-native-provided";

  /** Cache flavor of a native fat jar that embeds the Spark runtime. */
  public static final String FLAVOR_NATIVE = "hop-native";

  private NativeSparkFatJar() {}

  /**
   * @param provided true to leave out the Spark/Scala/Hadoop runtime the cluster provides
   * @return the jar filenames of the installation in the working directory
   */
  public static List<String> findJarFilenames(boolean provided) {
    return findJarFilenames(new File("."), provided);
  }

  /**
   * @param installation the Hop installation folder
   * @param provided true to leave out what the Spark cluster provides
   */
  public static List<String> findJarFilenames(File installation, boolean provided) {
    return findJarFilenames(installation, provided, beamEngineFiles(installation));
  }

  /**
   * @param beamEngine the files only the Beam engine installs, from {@link #beamEngineFiles(File)}
   */
  static List<String> findJarFilenames(
      File installation, boolean provided, PluginOwnedFiles beamEngine) {
    Set<String> sparkArtifacts = provided ? sparkDistributionArtifacts(installation) : Set.of();
    Predicate<File> beam = beamEngine.asExclusion();
    return InstalledJars.find(
        installation,
        file ->
            isExcluded(file, provided)
                || beam.test(file)
                || (!file.isDirectory() && isSparkDistributionJar(file, sparkArtifacts)));
  }

  /** The Beam engine plugin folder and the jars only it installs: none go in a native Spark jar. */
  public static PluginOwnedFiles beamEngineFiles(File installation) {
    return PluginOwnedFiles.onlyInstalledBy(installation, PluginOwnedFiles.BEAM_ENGINE_FOLDER);
  }

  /**
   * The artifact ids a Spark distribution of the engine's Spark version ships, from the dependency
   * tree the engine plugin writes at build time ({@value #SPARK_DEPENDENCIES_FILE}): everything
   * under its {@code org.apache.spark} and {@code org.apache.hadoop} dependencies. Empty when the
   * file is missing, for an engine plugin built before it existed.
   */
  static Set<String> sparkDistributionArtifacts(File installation) {
    File tree = new File(installation, SPARK_DEPENDENCIES_FILE);
    if (!tree.isFile()) {
      return Set.of();
    }
    try {
      return parseDistributionArtifacts(
          java.nio.file.Files.readAllLines(tree.toPath(), java.nio.charset.StandardCharsets.UTF_8));
    } catch (java.io.IOException e) {
      return Set.of();
    }
  }

  /**
   * Parse {@code mvn dependency:tree} text output: the artifact ids of the top-level {@code
   * org.apache.spark} and {@code org.apache.hadoop} dependencies and everything below them.
   */
  static Set<String> parseDistributionArtifacts(List<String> lines) {
    Set<String> artifacts = new HashSet<>();
    boolean inDistribution = false;
    for (String line : lines) {
      int start = 0;
      while (start < line.length() && !Character.isLetterOrDigit(line.charAt(start))) {
        start++;
      }
      if (start == 0 || start >= line.length()) {
        continue; // the root (the engine plugin itself) or an empty line
      }
      String[] coordinates = line.substring(start).trim().split(":");
      if (coordinates.length < 4) {
        continue;
      }
      if (start == 3) {
        // a direct dependency of the engine plugin: does a Spark distribution ship it?
        inDistribution =
            "org.apache.spark".equals(coordinates[0]) || "org.apache.hadoop".equals(coordinates[0]);
      }
      if (inDistribution) {
        artifacts.add(coordinates[1]);
      }
    }
    return artifacts;
  }

  /**
   * A jar of the engine's Spark runtime ({@code plugins/engines/spark/lib}) named after one of
   * these artifacts: {@code <artifactId>-<version>[-classifier].jar}.
   *
   * <p>Only that folder holds copies of what the cluster provides. A jar elsewhere stays even when
   * its artifact id matches: GraalVM's shadowed {@code xz} and {@code icu4j} of the Script
   * transform relocate their classes, and Spark shades the {@code protobuf-java} it uses, so the
   * cluster provides none of their classes. The lakehouse connectors in the engine's {@code
   * lib/delta} and {@code lib/iceberg} stay too: no Spark distribution ships them.
   */
  static boolean isSparkDistributionJar(File jar, Set<String> sparkArtifacts) {
    if (sparkArtifacts.isEmpty() || !isInSparkEngineLib(jar)) {
      return false;
    }
    String name = jar.getName();
    for (int i = name.indexOf('-'); i > 0 && i < name.length() - 1; i = name.indexOf('-', i + 1)) {
      if (Character.isDigit(name.charAt(i + 1))) {
        return sparkArtifacts.contains(name.substring(0, i));
      }
    }
    return false;
  }

  /** Whether a jar sits directly in {@code plugins/engines/spark/lib}. */
  static boolean isInSparkEngineLib(File jar) {
    File lib = jar.getParentFile();
    File spark = lib == null ? null : lib.getParentFile();
    File engines = spark == null ? null : spark.getParentFile();
    File plugins = engines == null ? null : engines.getParentFile();
    return plugins != null
        && "lib".equals(lib.getName())
        && "spark".equals(spark.getName())
        && "engines".equals(engines.getName())
        && "plugins".equals(plugins.getName());
  }

  /**
   * The cached native Spark fat jar of this installation, built when the installation changed since
   * the last build.
   */
  public static Path getOrBuild(ILogChannel log, IVariables variables, boolean provided)
      throws HopException {
    return getOrBuild(log, variables, provided, null);
  }

  /**
   * @param progressMonitor optional: follows a build and can cancel it
   * @see #getOrBuild(ILogChannel, IVariables, boolean)
   */
  public static Path getOrBuild(
      ILogChannel log, IVariables variables, boolean provided, IProgressMonitor progressMonitor)
      throws HopException {
    File installation = new File(".");
    PluginOwnedFiles beamEngine = beamEngineFiles(installation);
    if (!beamEngine.getFoldersWithoutManifest().isEmpty()) {
      log.logBasic(
          "The Beam engine plugin has no plugin manifest: only its folder is left out of the"
              + " native Spark fat jar, its jars in lib/core go in. Reinstall the plugin to leave"
              + " them out.");
    } else if (beamEngine.getJarCount() > 0) {
      log.logBasic(
          "Leaving out the "
              + beamEngine.getJarCount()
              + " jar files only the Beam engine installs.");
    }
    List<String> jars = findJarFilenames(installation, provided, beamEngine);
    if (jars.isEmpty()) {
      throw new HopException(
          "No Hop jar files found under lib/ and plugins/ of the working directory "
              + new File(".").getAbsolutePath()
              + ". A native Spark fat jar is built from the Hop installation: start Hop from its"
              + " installation folder, or set the fat jar of the Spark run configuration.");
    }
    return FatJarCache.getOrBuild(
        log, variables, provided ? FLAVOR_PROVIDED : FLAVOR_NATIVE, jars, progressMonitor);
  }

  static boolean isExcluded(File file, boolean provided) {
    if (file.isDirectory()) {
      return isSparkClientPackDir(file) || isSwtDir(file);
    }
    return provided && isExcludedForNativeProvidedSparkFatJar(file);
  }

  /**
   * {@code lib/spark-client} or {@code lib/spark-clients}: the Spark 3 client packs of the Beam
   * Spark runner. The Beam manifest claims the default pack, but the versioned packs under {@code
   * lib/spark-clients} are made by {@code tools/spark-client-pack/materialize-pack.sh}, so no
   * manifest claims them.
   */
  static boolean isSparkClientPackDir(File dir) {
    String name = dir.getName();
    File parent = dir.getParentFile();
    return parent != null
        && "lib".equals(parent.getName())
        && ("spark-client".equals(name) || "spark-clients".equals(name));
  }

  /**
   * {@code lib/swt}: the SWT jars of five platforms. Plugin and metadata classes reference SWT
   * types, so a pipeline on Spark needs SWT's classes to load them, but not its native libraries:
   * the one jar of {@code lib/swt/linux/x86_64}, which {@link InstalledJars} adds by itself, is
   * enough. Leaving out the other four saves about 10 MB.
   */
  static boolean isSwtDir(File dir) {
    File parent = dir.getParentFile();
    return "swt".equals(dir.getName()) && parent != null && "lib".equals(parent.getName());
  }

  /**
   * Extra exclusions for the native-provided flavor: the Spark/Scala runtime comes from the
   * cluster, not the fat jar.
   */
  public static boolean isExcludedForNativeProvidedSparkFatJar(File jar) {
    if (jar == null) {
      return true;
    }
    String name = jar.getName();
    if (!name.endsWith(".jar")) {
      return false;
    }

    // All Spark distribution modules and the Scala runtime
    if (name.startsWith("spark-") || name.startsWith("scala-")) {
      return true;
    }

    // Common Spark-adjacent Scala libraries from plugins/engines/spark/lib
    if (name.startsWith("chill_")
        || name.startsWith("chill-java-")
        || name.startsWith("json4s-")
        || name.startsWith("jackson-module-scala_")
        || name.startsWith("janino-")
        || name.startsWith("commons-compiler-")
        || name.startsWith("paranamer-")) {
      return true;
    }

    // Hadoop client pieces shipped next to Spark in the plugin lib (cluster provides these)
    return name.startsWith("hadoop-client-api-") || name.startsWith("hadoop-client-runtime-");
  }
}
