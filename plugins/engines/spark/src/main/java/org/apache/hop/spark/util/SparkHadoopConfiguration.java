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
import java.util.function.UnaryOperator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hop.core.exception.HopException;
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
  private static final List<String> RUNTIME_CLASSES_FROM_HOP =
      List.of(
          "org.slf4j.LoggerFactory",
          "com.fasterxml.jackson.core.JsonFactory",
          "com.fasterxml.jackson.databind.ObjectMapper",
          "com.fasterxml.jackson.annotation.JsonProperty");

  /**
   * The {@code spark.yarn.jars} value that makes YARN containers run the Spark version Hop runs:
   * the jars in the engine plugin's {@code lib} folder, plus the SLF4J and Jackson jars Hop itself
   * uses.
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
    for (String className : RUNTIME_CLASSES_FROM_HOP) {
      try {
        File jar = jarOf(Class.forName(className, false, classLoader));
        String path = jar.toPath().toAbsolutePath().normalize().toString();
        if (jar.isFile() && path.endsWith(".jar") && !jars.contains(path)) {
          jars.add(path);
        }
      } catch (Exception e) {
        // Not on the classpath: Spark on YARN reports what is missing
      }
    }
    return String.join(",", jars);
  }

  private static File jarOf(Class<?> clazz) throws Exception {
    URI location = clazz.getProtectionDomain().getCodeSource().getLocation().toURI();
    return new File(location);
  }
}
