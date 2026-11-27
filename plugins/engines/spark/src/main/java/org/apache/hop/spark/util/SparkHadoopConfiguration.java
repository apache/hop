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

package org.apache.hop.spark.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fatjar.NativeSparkFatJar;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Hadoop client configuration for the Spark sessions Hop starts itself (Hop GUI, hop-run, Hop
 * Server). {@code spark-submit} puts {@code HADOOP_CONF_DIR} on the classpath; a Hop JVM does not,
 * so the {@code *-site.xml} files are read here and passed to Spark as {@code spark.hadoop.*}.
 */
public final class SparkHadoopConfiguration {

  public static final String ENV_HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
  public static final String ENV_YARN_CONF_DIR = "YARN_CONF_DIR";

  /** Read in this order: later files win for keys they both set. */
  public static final List<String> SITE_FILES =
      List.of("core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml");

  public static final String SPARK_HADOOP_PREFIX = "spark.hadoop.";

  private SparkHadoopConfiguration() {}

  /** Is this a YARN master URL ({@code yarn}, {@code yarn-client}, ...)? */
  public static boolean isYarnMaster(String master) {
    return master != null && master.trim().toLowerCase().startsWith("yarn");
  }

  /**
   * The folders to read Hadoop configuration from. An explicit folder (run configuration) is used
   * for every master. The {@code HADOOP_CONF_DIR} / {@code YARN_CONF_DIR} environment variables are
   * only used for YARN: applied to {@code local[*]} runs they could point plain paths at HDFS.
   *
   * @param configuredFolder resolved folder from the run configuration, may be empty
   * @param yarnMaster whether the session runs on YARN
   * @param env environment lookup, {@code System::getenv} outside tests
   * @return distinct folders, in reading order
   */
  public static List<String> resolveFolders(
      String configuredFolder, boolean yarnMaster, UnaryOperator<String> env) {
    List<String> folders = new ArrayList<>();
    if (StringUtils.isNotBlank(configuredFolder)) {
      folders.add(configuredFolder.trim());
      return folders;
    }
    if (yarnMaster) {
      for (String name : List.of(ENV_HADOOP_CONF_DIR, ENV_YARN_CONF_DIR)) {
        String value = env.apply(name);
        if (StringUtils.isNotBlank(value) && !folders.contains(value.trim())) {
          folders.add(value.trim());
        }
      }
    }
    return folders;
  }

  /**
   * A Hadoop configuration with the {@code spark.hadoop.<key>} entries of Spark settings as {@code
   * <key>}, as Spark builds one for its own file systems.
   */
  public static Configuration hadoopConfiguration(Map<String, String> sparkSettings) {
    Configuration configuration = new Configuration();
    sparkSettings.forEach(
        (key, value) -> {
          if (key.startsWith(SPARK_HADOOP_PREFIX) && value != null) {
            configuration.set(key.substring(SPARK_HADOOP_PREFIX.length()), value);
          }
        });
    return configuration;
  }

  /**
   * Reads the site files found in the folders and returns their properties as {@code
   * spark.hadoop.<key>} entries, with Hadoop variable expansion applied.
   */
  public static Map<String, String> load(List<String> folders, ILogChannel log)
      throws HopException {
    Configuration conf = new Configuration(false);
    List<String> filesRead = new ArrayList<>();
    for (String folder : folders) {
      try {
        FileObject dir = HopVfs.getFileObject(folder);
        if (!dir.exists() || !dir.isFolder()) {
          log.logError("Hadoop configuration folder '" + folder + "' does not exist; skipped");
          continue;
        }
        for (String site : SITE_FILES) {
          FileObject file = dir.resolveFile(site);
          if (!file.exists()) {
            continue;
          }
          byte[] bytes;
          try (InputStream in = HopVfs.getInputStream(file)) {
            bytes = IOUtils.toByteArray(in);
          }
          conf.addResource(new ByteArrayInputStream(bytes), file.getName().getURI());
          filesRead.add(file.getName().getURI());
        }
      } catch (Exception e) {
        throw new HopException("Error reading Hadoop configuration from '" + folder + "'", e);
      }
    }
    Map<String, String> result = new LinkedHashMap<>();
    if (filesRead.isEmpty()) {
      return result;
    }
    for (Map.Entry<String, String> entry : conf) {
      String value = conf.get(entry.getKey());
      if (value != null) {
        result.put(SPARK_HADOOP_PREFIX + entry.getKey(), value);
      }
    }
    log.logBasic(
        "Read "
            + result.size()
            + " Hadoop configuration properties from "
            + String.join(", ", filesRead));
    return result;
  }

  /**
   * Classes Spark needs at runtime that the engine plugin's {@code lib} folder leaves to Hop's
   * {@code lib/core} (see the plugin assembly): SLF4J and Jackson.
   */
  /**
   * Classes Hop loads from its shared library folder ({@code lib/core}). Spark's dependencies that
   * Hop also uses (SLF4J, log4j, Jackson, Netty, commons-*, ...) live there and not in the engine
   * plugin's {@code lib}: the YARN containers need that folder as well.
   */
  private static final List<String> SHARED_FOLDER_MARKER_CLASSES =
      List.of("org.slf4j.LoggerFactory", "io.netty.channel.EventLoopGroup");

  /**
   * A jar of Hop's shared folder that belongs on the YARN class path: not Hop itself (the fat jar),
   * not Spark, Scala or the Hadoop client (Spark 4 comes from the engine plugin's lib), and not
   * Hadoop (the cluster has it).
   */
  static boolean isSharedRuntimeJar(File jar) {
    return isSharedRuntimeJar(jar, file -> false);
  }

  /**
   * @param beamOnly the jars only the Beam engine installs (Beam, Spark 3 / Scala 2.12): Beam keeps
   *     those in the same shared folder, and they stay off the YARN class path as they stay out of
   *     the native Spark fat jar
   * @see #isSharedRuntimeJar(File)
   */
  static boolean isSharedRuntimeJar(File jar, Predicate<File> beamOnly) {
    String name = jar.getName();
    return !name.startsWith("hop-")
        && !name.startsWith("hadoop-")
        && !beamOnly.test(jar)
        && !NativeSparkFatJar.isExcludedForNativeProvidedSparkFatJar(jar);
  }

  /**
   * The jars of Hop's shared folder ({@code <installation>/lib/core}) that, by the plugin
   * manifests, only the Beam engine installs. Nothing for a folder outside an installation.
   */
  static Predicate<File> beamOnlyJars(File sharedFolder) {
    File lib = sharedFolder.getParentFile();
    File installation = lib == null ? null : lib.getParentFile();
    if (installation == null || !"lib".equals(lib.getName())) {
      return file -> false;
    }
    return NativeSparkFatJar.beamEngineFiles(installation).asExclusion();
  }

  /**
   * The Spark runtime for YARN containers ({@link #defaultYarnJars}) as one archive for {@code
   * spark.yarn.archive}, built once per installation in the fat jar cache. YARN localizes one
   * archive much faster than hundreds of loose jars, and a cluster-mode run stages it only once.
   */
  public static java.nio.file.Path yarnRuntimeArchive(
      ILogChannel log, org.apache.hop.core.variables.IVariables variables, Class<?> engineClass)
      throws HopException {
    String jars = defaultYarnJars(engineClass);
    if (StringUtils.isEmpty(jars)) {
      throw new HopException(
          "Unable to find the Spark runtime of the native Spark engine (plugins/engines/spark/lib)"
              + " for YARN. Set spark.yarn.jars or spark.yarn.archive in the Spark config of the"
              + " run configuration.");
    }
    List<String> jarFiles = List.of(jars.split(","));
    return org.apache.hop.core.fatjar.FatJarCache.getOrCreate(
        log,
        variables,
        "hop-spark-yarn-runtime",
        ".zip",
        "Spark runtime archive",
        jarFiles,
        target -> writeArchive(target, jarFiles));
  }

  /** The jars at the root of a zip, uncompressed: jars are compressed already. */
  static void writeArchive(java.nio.file.Path target, List<String> jarFiles) throws HopException {
    try (java.util.zip.ZipOutputStream zip =
        new java.util.zip.ZipOutputStream(
            new java.io.BufferedOutputStream(java.nio.file.Files.newOutputStream(target)))) {
      zip.setLevel(java.util.zip.Deflater.NO_COMPRESSION);
      for (String jarFile : jarFiles) {
        java.nio.file.Path jar = java.nio.file.Paths.get(jarFile);
        zip.putNextEntry(new java.util.zip.ZipEntry(jar.getFileName().toString()));
        java.nio.file.Files.copy(jar, zip);
        zip.closeEntry();
      }
    } catch (java.io.IOException e) {
      throw new HopException("Unable to write the Spark runtime archive " + target, e);
    }
  }

  /**
   * The {@code spark.yarn.jars} value that makes YARN containers run the Spark version Hop runs:
   * the jars in the engine plugin's {@code lib} folder plus Hop's shared library folder, the class
   * path Spark runs on inside Hop.
   *
   * @param engineClass a class loaded from the engine plugin jar
   * @return a comma-separated list of jar paths, or null when the plugin's {@code lib} folder
   *     cannot be found (for example in unit tests)
   */
  public static String defaultYarnJars(Class<?> engineClass) {
    File lib;
    try {
      File pluginFolder = jarOf(engineClass).getParentFile();
      if (pluginFolder == null) {
        return null;
      }
      lib = new File(pluginFolder, "lib");
      if (!lib.isDirectory()) {
        return null;
      }
    } catch (Exception e) {
      return null;
    }
    List<String> jars = new ArrayList<>();
    File[] libJars = lib.listFiles((dir, name) -> name.endsWith(".jar"));
    if (libJars == null) {
      return null;
    }
    Arrays.sort(libJars);
    for (File jar : libJars) {
      // Hop classes reach the containers in the fat jar, on Spark's user class loader. A Hop jar
      // here (lib/ also holds the lakehouse metadata jar) would sit on the containers' system
      // class path, where it cannot see the Hop classes it extends.
      if (!jar.getName().startsWith("hop-")) {
        jars.add(jar.toPath().toAbsolutePath().normalize().toString());
      }
    }
    ClassLoader classLoader = engineClass.getClassLoader();
    // Spark runs in the Hop JVM on lib/core plus this plugin's lib: give the containers the same
    // class path. Hop's own jars stay out, they arrive in the fat jar on the user class path.
    java.util.Set<String> names = new java.util.HashSet<>();
    for (String jar : jars) {
      names.add(new File(jar).getName());
    }
    java.util.Set<File> folders = new java.util.LinkedHashSet<>();
    for (String className : SHARED_FOLDER_MARKER_CLASSES) {
      try {
        File folder = jarOf(Class.forName(className, false, classLoader)).getParentFile();
        if (folder != null && folder.isDirectory() && !folder.equals(lib)) {
          folders.add(folder);
        }
      } catch (Exception e) {
        // Not on the classpath: Spark on YARN reports what is missing
      }
    }
    for (File folder : folders) {
      File[] shared = folder.listFiles((dir, name) -> name.endsWith(".jar"));
      if (shared == null) {
        continue;
      }
      Arrays.sort(shared);
      Predicate<File> beamOnly = beamOnlyJars(folder);
      for (File jar : shared) {
        if (isSharedRuntimeJar(jar, beamOnly) && names.add(jar.getName())) {
          jars.add(jar.toPath().toAbsolutePath().normalize().toString());
        }
      }
    }
    return String.join(",", jars);
  }

  private static File jarOf(Class<?> clazz) throws Exception {
    URI location = clazz.getProtectionDomain().getCodeSource().getLocation().toURI();
    return new File(location);
  }
}
