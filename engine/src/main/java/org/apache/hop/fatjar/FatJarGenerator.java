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

package org.apache.hop.fatjar;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fatjar.FatJarBuilder;
import org.apache.hop.core.fatjar.NativeSparkFatJar;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.engine.PipelineEnginePluginType;

/**
 * Writes a Hop fat jar for one engine to a file: {@code hop-conf --generate-fat-jar} and Tools,
 * Generate a Hop fat jar in Hop GUI both use it.
 *
 * <p>A native Spark jar is the one the Spark engine and the Databricks deployment use themselves:
 * it comes from the fat jar cache and is copied, so it is built at most once per installation
 * change. The Beam engine plugin selects the jars of a Beam jar ({@code BeamFatJar}); it is called
 * through the plugin registry because this module does not depend on the plugin.
 */
public final class FatJarGenerator {

  static final String BEAM_FAT_JAR_CLASS = "org.apache.hop.beam.pipeline.fatjar.BeamFatJar";

  private static final Class<?> PKG = FatJarGenerator.class;

  private FatJarGenerator() {}

  /** The engines with their engine plugin installed, in the order to offer them. */
  public static List<FatJarEngine> availableEngines() {
    List<FatJarEngine> engines = new ArrayList<>();
    for (FatJarEngine engine : FatJarEngine.values()) {
      if (enginePlugin(engine) != null) {
        engines.add(engine);
      }
    }
    return engines;
  }

  /**
   * @param engine the engine the jar is for
   * @param targetFile the jar to write
   * @param sparkClientVersion Beam only: the Spark client pack of the Beam Spark runner, or null
   * @param includeSparkRuntime Spark only: also embed Spark 4 and Scala instead of leaving them to
   *     the cluster; only for a JVM without Spark that starts the session itself
   */
  public static void generate(
      ILogChannel log,
      IVariables variables,
      FatJarEngine engine,
      String targetFile,
      String sparkClientVersion,
      boolean includeSparkRuntime)
      throws HopException {
    generate(log, variables, engine, targetFile, sparkClientVersion, includeSparkRuntime, null);
  }

  /**
   * @param progressMonitor optional: follows the build and can cancel it, see {@link
   *     FatJarBuilder#CANCELED_MESSAGE}. Not marked done: that is up to the caller.
   * @see #generate(ILogChannel, IVariables, FatJarEngine, String, String, boolean)
   */
  public static void generate(
      ILogChannel log,
      IVariables variables,
      FatJarEngine engine,
      String targetFile,
      String sparkClientVersion,
      boolean includeSparkRuntime,
      IProgressMonitor progressMonitor)
      throws HopException {
    IPlugin plugin = enginePlugin(engine);
    if (plugin == null) {
      throw new HopException(
          "Cannot generate a fat jar for "
              + engine.getDescription()
              + ": its engine plugin ("
              + engine.getEnginePluginId()
              + ") is not installed. Install it with the Hop marketplace.");
    }
    String target = variables.resolve(targetFile);
    switch (engine) {
      case SPARK -> {
        Path cached =
            NativeSparkFatJar.getOrBuild(log, variables, !includeSparkRuntime, progressMonitor);
        if (progressMonitor != null) {
          progressMonitor.subTask(
              BaseMessages.getString(PKG, "FatJarGenerator.Progress.Copying", target));
        }
        try {
          Path targetPath = Paths.get(target).toAbsolutePath();
          if (targetPath.getParent() != null) {
            Files.createDirectories(targetPath.getParent());
          }
          Files.copy(cached, targetPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
          throw new HopException("Unable to copy the fat jar " + cached + " to " + target, e);
        }
        log.logBasic("Wrote native Spark fat jar " + target + " (from " + cached + ")");
      }
      case BEAM -> {
        List<String> jars = beamJarFilenames(plugin, sparkClientVersion);
        log.logBasic("Found " + jars.size() + " jar files to combine into one fat jar file.");
        FatJarBuilder builder = new FatJarBuilder(log, variables, target, jars);
        builder.setProgressMonitor(progressMonitor);
        builder.buildTargetJar();
        log.logBasic("Wrote Beam fat jar " + target);
      }
    }
  }

  /**
   * {@code HOP_SPARK_CLIENT_VERSION} as a system property or environment variable: the Spark client
   * pack of a Beam jar, or the former {@code native} / {@code native-provided} tokens.
   */
  public static String sparkClientVersionFromEnvironment() {
    String value = System.getProperty("HOP_SPARK_CLIENT_VERSION");
    if (value == null || value.isBlank()) {
      value = System.getenv("HOP_SPARK_CLIENT_VERSION");
    }
    return value == null || value.isBlank() ? null : value.trim();
  }

  @SuppressWarnings("unchecked")
  private static List<String> beamJarFilenames(IPlugin beamPlugin, String sparkClientVersion)
      throws HopException {
    try {
      ClassLoader classLoader = PluginRegistry.getInstance().getClassLoader(beamPlugin);
      Class<?> beamFatJar = Class.forName(BEAM_FAT_JAR_CLASS, true, classLoader);
      Method find = beamFatJar.getMethod("findJarFilenames", String.class);
      return (List<String>) find.invoke(null, sparkClientVersion);
    } catch (java.lang.reflect.InvocationTargetException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new HopException("Unable to select the jars of a Beam fat jar", cause);
    } catch (Exception e) {
      throw new HopException(
          "The installed Beam engine plugin cannot select the jars of a fat jar ("
              + BEAM_FAT_JAR_CLASS
              + ")",
          e);
    }
  }

  private static IPlugin enginePlugin(FatJarEngine engine) {
    return PluginRegistry.getInstance()
        .findPluginWithId(PipelineEnginePluginType.class, engine.getEnginePluginId());
  }
}
