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

package org.apache.hop.airflow.launcher;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * The Hop installation of the worker, loaded in its own class loader. Its parent is the platform
 * class loader: Hop does not see the Airflow SDK's Kotlin and Jackson, and the SDK does not see
 * Hop's. Sets what the hop script sets before it starts Java.
 */
final class HopRuntime {

  private static final String HOP_CLASS = "org.apache.hop.hop.Hop";

  /** Passed on from the environment as system properties, as the hop script does. */
  private static final List<String> PASSED_ON =
      List.of(
          "HOP_CONFIG_FOLDER",
          "HOP_SHARED_JDBC_FOLDERS",
          "HOP_PLUGIN_BASE_FOLDERS",
          "HOP_AUDIT_FOLDER",
          "HOP_PASSWORD_ENCODER_PLUGIN",
          "HOP_AES_ENCODER_KEY",
          "HOP_AES_ENCODER_KEY_FILE");

  private final Path hopHome;
  private final ClassLoader classLoader;

  HopRuntime(Path hopHome) throws IOException {
    this.hopHome = hopHome;
    List<URL> urls = new ArrayList<>();
    addJars(urls, hopHome.resolve("lib/core"));
    String sparkClient = System.getenv("HOP_SPARK_CLIENT_VERSION");
    Path versionedSparkClient =
        sparkClient == null ? null : hopHome.resolve("lib/spark-clients").resolve(sparkClient);
    if (versionedSparkClient != null && Files.isDirectory(versionedSparkClient)) {
      addJars(urls, versionedSparkClient);
    } else {
      addJars(urls, hopHome.resolve("lib/spark-client"));
    }
    if (urls.isEmpty()) {
      throw new IOException("No Hop installation in " + hopHome + ": lib/core holds no jars");
    }
    classLoader =
        new URLClassLoader("hop", urls.toArray(new URL[0]), ClassLoader.getPlatformClassLoader());
  }

  /**
   * The Hop installation: HOP_HOME, or the installation the launcher jar is part of ({@value
   * HopAirflowBundle#LAUNCHER_FOLDER}).
   */
  static Path findHopHome() throws IOException {
    String hopHome = System.getenv("HOP_HOME");
    if (hopHome != null && !hopHome.isBlank()) {
      return Path.of(hopHome);
    }
    Path launcherJar;
    try {
      launcherJar =
          Path.of(HopRuntime.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    } catch (Exception e) {
      throw new IOException("Unable to find the launcher jar; set HOP_HOME", e);
    }
    Path folder = launcherJar.getParent();
    for (int i = 0; i < Path.of(HopAirflowBundle.LAUNCHER_FOLDER).getNameCount(); i++) {
      folder = folder == null ? null : folder.getParent();
    }
    if (folder == null || !Files.isDirectory(folder.resolve("lib/core"))) {
      throw new IOException(
          "The launcher " + launcherJar + " is not part of a Hop installation; set HOP_HOME");
    }
    return folder;
  }

  /** The system properties the hop script would set, for this installation. */
  void applySystemProperties(Map<String, String> environment) {
    for (String name : PASSED_ON) {
      String value = environment.get(name);
      if (value != null && !value.isBlank() && System.getProperty(name) == null) {
        System.setProperty(name, value);
      }
    }
    setDefault("HOP_PLUGIN_BASE_FOLDERS", hopHome.resolve("plugins").toString());
    setDefault("HOP_SHARED_JDBC_FOLDERS", hopHome.resolve("lib/jdbc").toString());
    setDefault("HOP_CONFIG_FOLDER", hopHome.resolve("config").toString());
    setDefault(
        "HOP_AUDIT_FOLDER", Path.of(System.getProperty("java.io.tmpdir"), "hop-audit").toString());
    setDefault("HOP_PLATFORM_OS", System.getProperty("os.name"));
    System.setProperty("HOP_PLATFORM_RUNTIME", "Run");
    System.setProperty("HOP_AUTO_CREATE_CONFIG", "Y");
    System.setProperty("java.awt.headless", "true");
    // Airflow schedules the DAG on the Start action's schedule: run the workflow once.
    System.setProperty("HOP_IGNORE_START_REPEAT", "Y");
  }

  private static void setDefault(String name, String value) {
    if (System.getProperty(name) == null) {
      System.setProperty(name, value);
    }
  }

  /** Runs a hop command line in this JVM and returns its exit code. */
  int execute(List<String> args) throws Exception {
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    thread.setContextClassLoader(classLoader);
    try {
      Method execute =
          Class.forName(HOP_CLASS, true, classLoader).getMethod("execute", String[].class);
      return (Integer) execute.invoke(null, (Object) args.toArray(new String[0]));
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof Exception cause) {
        throw cause;
      }
      throw e;
    } finally {
      thread.setContextClassLoader(previous);
    }
  }

  private static void addJars(List<URL> urls, Path folder) throws IOException {
    if (!Files.isDirectory(folder)) {
      return;
    }
    try (Stream<Path> jars = Files.list(folder)) {
      for (Path jar : jars.filter(p -> p.toString().endsWith(".jar")).sorted().toList()) {
        urls.add(jar.toUri().toURL());
      }
    }
  }
}
