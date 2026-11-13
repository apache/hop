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

package org.apache.hop.beam.pipeline.fatjar;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.fatjar.InstalledJars;
import org.apache.hop.core.fatjar.NativeSparkFatJar;

/**
 * The jars of this installation that go into a fat jar for the Beam engines (Direct, Flink,
 * Dataflow, Spark runner), with the Spark client pack of the Beam Spark runner.
 *
 * <p>Called by the fat jar generator of the engine through the plugin registry: a plain class, no
 * GUI dependencies.
 */
public final class BeamFatJar {

  /**
   * Special {@code --spark-client-version} token for a native Spark 4 fat jar: exclude Beam's Spark
   * 3 / Scala 2.12 client classpath and keep {@code plugins/engines/spark/lib} (Spark 4 embedded).
   * Suitable for hop-run client mode; for spark-submit use {@link
   * #SPARK_CLIENT_VERSION_NATIVE_PROVIDED}.
   */
  public static final String SPARK_CLIENT_VERSION_NATIVE = "native";

  /**
   * Like {@link #SPARK_CLIENT_VERSION_NATIVE} but also excludes all Spark / Scala runtime jars so
   * the cluster's Spark distribution provides them (correct for {@code spark-submit}).
   */
  public static final String SPARK_CLIENT_VERSION_NATIVE_PROVIDED = "native-provided";

  /**
   * Collect jar files from the current Hop installation for fat-jar generation.
   *
   * <p>Uses the default Spark client pack ({@code lib/spark-client}), or the versioned pack
   * selected by system property / env {@code HOP_SPARK_CLIENT_VERSION} under {@code
   * lib/spark-clients/<version>}. Versioned packs under {@code lib/spark-clients/} are never all
   * included at once. Special tokens {@link #SPARK_CLIENT_VERSION_NATIVE} and {@link
   * #SPARK_CLIENT_VERSION_NATIVE_PROVIDED} build fat jars for the native Spark 4 engine.
   */
  public static final List<String> findInstalledJarFilenames() {
    return findInstalledJarFilenames(resolveSparkClientVersion(null));
  }

  /**
   * @param sparkClientVersion Spark client pack version (e.g. {@code 3.5.8}), {@link
   *     #SPARK_CLIENT_VERSION_NATIVE}, {@link #SPARK_CLIENT_VERSION_NATIVE_PROVIDED}, or null/blank
   *     for the default pack at {@code lib/spark-client}
   */
  public static final List<String> findInstalledJarFilenames(String sparkClientVersion) {
    if (isNativeSparkClientVersion(sparkClientVersion)) {
      // The native Spark engine's jar selection is shared with the engine and Databricks
      return NativeSparkFatJar.findJarFilenames(
          isNativeProvidedSparkClientVersion(sparkClientVersion));
    }
    boolean versionedPack = StringUtils.isNotBlank(sparkClientVersion);

    // lib/ and plugins/ except the Spark client pack directories (added separately below).
    // Versioned packs own all spark-* jars: drop Beam's spark fragments from its SDK folders.
    Set<String> jarFilenames =
        new TreeSet<>(
            InstalledJars.find(
                file ->
                    file.isDirectory()
                        ? isSparkClientPackDir(file)
                        : versionedPack
                            && file.getName().startsWith("spark-")
                            && isBeamSdkJarFolder(file.getParentFile())));

    // Selected Spark client pack only (never all versioned packs at once)
    File sparkClientDir = resolveSparkClientPackDir(sparkClientVersion);
    if (sparkClientDir.isDirectory()) {
      FileUtils.listFiles(sparkClientDir, new String[] {"jar"}, false)
          .forEach(file -> jarFilenames.add(file.toString()));
    }

    // Final guard: when using a versioned pack, drop any spark-* jar outside that pack
    // (avoids Beam fragments under lib/beam mixing serialVersionUID / version-info).
    // Note: this would also strip plugins/engines/spark Spark 4 jars — intentional for Beam
    // packs; use spark-client-version=native for the native engine fat jar.
    if (versionedPack) {
      String packPrefix = canonicalPath(sparkClientDir);
      jarFilenames.removeIf(
          filename -> {
            File file = new File(filename);
            // Spark distribution jars are named spark-*.jar; Beam's runner is beam-runners-spark-*
            return file.getName().startsWith("spark-")
                && !canonicalPath(file).startsWith(packPrefix);
          });
    }

    // Stable order improves FatJarBuilder "first wins" collision behaviour
    return new ArrayList<>(jarFilenames);
  }

  private static String canonicalPath(File file) {
    try {
      return file.getCanonicalPath();
    } catch (Exception e) {
      return file.getAbsolutePath();
    }
  }

  /** {@code lib/spark-client} or {@code lib/spark-clients}: skipped while scanning lib/. */
  static boolean isSparkClientPackDir(File dir) {
    File parent = dir.getParentFile();
    return parent != null
        && "lib".equals(parent.getName())
        && ("spark-client".equals(dir.getName()) || "spark-clients".equals(dir.getName()));
  }

  /**
   * True when {@code version} is a native Spark fat-jar token ({@code native} or {@code
   * native-provided}).
   */
  public static boolean isNativeSparkClientVersion(String version) {
    if (StringUtils.isBlank(version)) {
      return false;
    }
    String v = version.trim();
    return SPARK_CLIENT_VERSION_NATIVE.equalsIgnoreCase(v)
        || SPARK_CLIENT_VERSION_NATIVE_PROVIDED.equalsIgnoreCase(v);
  }

  /** True when {@code version} is {@link #SPARK_CLIENT_VERSION_NATIVE_PROVIDED}. */
  public static boolean isNativeProvidedSparkClientVersion(String version) {
    return StringUtils.isNotBlank(version)
        && SPARK_CLIENT_VERSION_NATIVE_PROVIDED.equalsIgnoreCase(version.trim());
  }

  /**
   * Resolve which Spark client pack version to use. Explicit argument wins, then system property
   * {@code HOP_SPARK_CLIENT_VERSION}, then env of the same name.
   */
  @SuppressWarnings("javabugs:S2259") // guarded by StringUtils.isNotBlank
  public static String resolveSparkClientVersion(String explicitVersion) {
    if (StringUtils.isNotBlank(explicitVersion)) {
      return explicitVersion.trim();
    }
    String prop = System.getProperty("HOP_SPARK_CLIENT_VERSION");
    if (StringUtils.isNotBlank(prop)) {
      return prop.trim();
    }
    String env = System.getenv("HOP_SPARK_CLIENT_VERSION");
    if (StringUtils.isNotBlank(env)) {
      return env.trim();
    }
    return null;
  }

  /**
   * Directory for the default pack or {@code lib/spark-clients/<version>}. Not used for {@link
   * #SPARK_CLIENT_VERSION_NATIVE}.
   */
  public static File resolveSparkClientPackDir(String sparkClientVersion) {
    if (StringUtils.isBlank(sparkClientVersion) || isNativeSparkClientVersion(sparkClientVersion)) {
      return new File("lib/spark-client");
    }
    return new File("lib/spark-clients/" + sparkClientVersion.trim());
  }

  /**
   * True for the Beam SDK folder of older layouts: the top-level {@code lib/beam} before Beam
   * became a marketplace plugin, and {@code plugins/engines/beam/lib-beam} in between. Current
   * installs put the Beam SDKs in {@code lib/core}, where the generic spark-* guard below already
   * covers them.
   */
  static boolean isBeamSdkJarFolder(File folder) {
    if (folder == null) {
      return false;
    }
    String name = folder.getName();
    if ("lib-beam".equals(name)) {
      return true;
    }
    File parent = folder.getParentFile();
    return "beam".equals(name) && parent != null && "lib".equals(parent.getName());
  }

  private BeamFatJar() {}

  /** Jar filenames for the generator; the argument is a Spark client pack version or null. */
  public static List<String> findJarFilenames(String sparkClientVersion) {
    return findInstalledJarFilenames(resolveSparkClientVersion(sparkClientVersion));
  }
}
