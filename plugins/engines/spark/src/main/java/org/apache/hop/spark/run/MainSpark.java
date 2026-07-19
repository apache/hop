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

package org.apache.hop.spark.run;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.pipeline.engine.PipelineEnginePluginType;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.hop.spark.pkg.SparkProjectPackage;
import org.apache.hop.spark.util.SparkConst;

/**
 * Driver entry point for native Spark pipeline execution via {@code spark-submit}, analogous to
 * Beam's {@code org.apache.hop.beam.run.MainBeam}.
 *
 * <p>Typical usage:
 *
 * <pre>
 * spark-submit --master spark://host:7077 \
 *   --class org.apache.hop.spark.run.MainSpark \
 *   hop-native-spark4.jar \
 *   pipeline.hpl metadata.json runConfigName
 * </pre>
 *
 * <p>Project package mode (nested Simple Mapping / Pipeline Executor definitions):
 *
 * <pre>
 * spark-submit ... MainSpark \
 *   --HopProjectPackage=/path/project-spark.zip \
 *   --HopPipelinePath=pipelines/run-on-spark.hpl \
 *   --HopRunConfigurationName=spark-cluster
 * </pre>
 */
public class MainSpark {

  public static void main(String[] args) {
    try {
      runPipeline(args);
      // Success. Never call System.exit(0) when Databricks TrapExitSecurityManager is installed:
      // it throws ExitSecurityException ("Program attempted to exit with code 0"), which older
      // MainSpark caught as a failure and re-printed as "Error running native Spark pipeline".
      // Plain spark-submit still uses System.exit(0) so non-daemon Spark threads do not hang.
      finishSuccess();
    } catch (SecurityException e) {
      if (isTrappedExitZero(e)) {
        System.out.println(
            ">>>>>> System.exit(0) blocked by security manager — treating run as successful");
        return;
      }
      failAndExit(e);
    } catch (Exception e) {
      failAndExit(e);
    }
  }

  private static void runPipeline(String[] args) throws Exception {
    System.out.println(">>>>>> Initializing Hop");
    System.out.println(
        ">>>>>> Spark project package distribution build: "
            + SparkProjectPackage.DISTRIBUTION_BUILD_ID);
    try {
      java.net.URL loc =
          SparkProjectPackage.class.getProtectionDomain().getCodeSource().getLocation();
      if (loc != null) {
        System.out.println(">>>>>> SparkProjectPackage loaded from: " + loc);
      }
    } catch (Exception ignored) {
      // best-effort diagnostics only
    }
    HopEnvironment.init();

    MainSparkArgs parsed = MainSparkArgs.parse(args);
    System.out.println(
        "Argument : Pipeline path (.hpl)               : " + parsed.getPipelinePath());
    if (parsed.hasProjectPackage()) {
      System.out.println(
          "Argument : Project package (zip)             : " + parsed.getProjectPackage());
    }
    System.out.println(
        "Argument : Metadata export (.json)            : "
            + Const.NVL(parsed.getMetadataPath(), "(from package)"));
    System.out.println(
        "Argument : Pipeline run configuration         : " + parsed.getRunConfigName());
    if (StringUtils.isNotEmpty(parsed.getEnvironmentFile())) {
      System.out.println(
          "Argument : Environment configuration file    : " + parsed.getEnvironmentFile());
    }

    IVariables variables = Variables.getADefaultVariableSpace();

    if (StringUtils.isNotEmpty(parsed.getEnvironmentFile())) {
      DescribedVariablesConfigFile configFile =
          new DescribedVariablesConfigFile(variables.resolve(parsed.getEnvironmentFile()));
      configFile.readFromFile();
      for (DescribedVariable variable : configFile.getDescribedVariables()) {
        variables.setVariable(variable.getName(), variable.getValue());
      }
      System.out.println(
          ">>>>>> Applied number of variables: " + configFile.getDescribedVariables().size());
    }

    String pipelinePath = parsed.getPipelinePath();
    String metadataPath = parsed.getMetadataPath();

    if (parsed.hasProjectPackage()) {
      String packageUri = variables.resolve(parsed.getProjectPackage());
      System.out.println(">>>>>> Materializing Spark project package: " + packageUri);
      SparkProjectPackage.Materialized materialized = SparkProjectPackage.materialize(packageUri);
      SparkProjectPackage.applyToVariables(variables, materialized, packageUri);
      System.out.println(">>>>>> PROJECT_HOME=" + materialized.projectHome());
      System.out.println(
          ">>>>>> Note: PROJECT_HOME is the definition package root — use separate variables"
              + " (HOP_DATA / s3a://…) for Spark Dataset data paths.");
      pipelinePath = SparkProjectPackage.resolvePipelinePath(pipelinePath, materialized, variables);
      if (StringUtils.isEmpty(metadataPath)) {
        if (StringUtils.isEmpty(materialized.metadataPath())) {
          throw new HopException(
              "Project package has no metadata.json and --HopMetadataPath was not set: "
                  + packageUri);
        }
        metadataPath = materialized.metadataPath();
      } else {
        metadataPath = variables.resolve(metadataPath);
      }
      System.out.println(">>>>>> Resolved pipeline path: " + pipelinePath);
      System.out.println(">>>>>> Resolved metadata path: " + metadataPath);
    } else {
      pipelinePath = variables.resolve(pipelinePath);
      metadataPath = variables.resolve(metadataPath);
    }

    String pipelineMetaXml = readFileIntoString(pipelinePath, Const.UTF_8);
    String metadataJson = readFileIntoString(metadataPath, Const.UTF_8);
    String runConfigName = parsed.getRunConfigName();

    SerializableMetadataProvider metadataProvider = new SerializableMetadataProvider(metadataJson);

    IHopMetadataSerializer<PipelineRunConfiguration> serializer =
        metadataProvider.getSerializer(PipelineRunConfiguration.class);
    if (!serializer.exists(runConfigName)) {
      throw new HopException(
          "The specified pipeline run configuration '"
              + runConfigName
              + "' doesn't exist in the metadata export");
    }

    PipelineRunConfiguration pipelineRunConfiguration = serializer.load(runConfigName);
    IPipelineEngineRunConfiguration engineRunConfiguration =
        pipelineRunConfiguration.getEngineRunConfiguration();
    if (!(engineRunConfiguration instanceof ISparkPipelineEngineRunConfiguration)) {
      throw new HopException(
          "Run configuration '"
              + runConfigName
              + "' is not a native Spark pipeline engine configuration (found "
              + (engineRunConfiguration == null
                  ? "null"
                  : engineRunConfiguration.getClass().getName())
              + "). Use a run configuration whose engine is '"
              + SparkConst.PLUGIN_NAME
              + "'.");
    }

    System.out.println(">>>>>> Loading pipeline metadata");
    PipelineMeta pipelineMeta =
        new PipelineMeta(
            XmlHandler.loadXmlString(pipelineMetaXml, PipelineMeta.XML_TAG), metadataProvider);
    // Match hop-run: when name is synchronized with the filename, use the provided path so
    // getName() returns the basename (e.g. spark-transforms) instead of the XML <name>.
    pipelineMeta.setFilename(pipelinePath);

    System.out.println(">>>>>> Validating native Spark engine plugin in fat jar...");
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin sparkEnginePlugin =
        registry.findPluginWithId(PipelineEnginePluginType.class, SparkConst.PLUGIN_ID);
    if (sparkEnginePlugin == null) {
      throw new HopException(
          "ERROR: Unable to find native Spark pipeline engine plugin '"
              + SparkConst.PLUGIN_ID
              + "'. Is plugins/engines/spark included in the fat jar? "
              + "Generate with: hop-conf.sh --generate-fat-jar=... --spark-client-version=native");
    }

    // Diagnose SPARK_HOME mismatches: if SPARK_HOME points at an older install
    // (e.g. /opt/spark -> 3.3.0), even "./bin/spark-submit" from a 4.1.x tree re-executes
    // ${SPARK_HOME}/bin/spark-class and loads that older Spark.
    String sparkHome = System.getenv("SPARK_HOME");
    if (StringUtils.isNotEmpty(sparkHome)) {
      System.out.println(">>>>>> SPARK_HOME=" + sparkHome);
    }

    IPipelineEngine<PipelineMeta> pipelineEngine =
        PipelineEngineFactory.createPipelineEngine(
            variables, runConfigName, metadataProvider, pipelineMeta);
    System.out.println(">>>>>> Pipeline execution starting (native Spark)...");
    pipelineEngine.execute();
    pipelineEngine.waitUntilFinished();
    System.out.println(">>>>>> Execution finished...");
    if (pipelineEngine.getErrors() > 0) {
      throw new HopException("Pipeline finished with " + pipelineEngine.getErrors() + " error(s)");
    }
  }

  /**
   * Complete a successful run without tripping Databricks {@code TrapExitSecurityManager}.
   *
   * <p>Databricks installs a security manager that rejects {@code System.exit} (including code 0)
   * with {@code ExitSecurityException}. Returning from {@code main} is the supported success path
   * there. Classic spark-submit still needs {@code System.exit(0)} so non-daemon Spark threads do
   * not hang the JVM.
   */
  static void finishSuccess() {
    if (shouldAvoidSystemExitOnSuccess()) {
      System.out.println(">>>>>> Skipping System.exit(0) (Databricks / TrapExit security manager)");
      return;
    }
    try {
      System.exit(0);
    } catch (SecurityException e) {
      if (isTrappedExitZero(e)) {
        System.out.println(
            ">>>>>> System.exit(0) blocked by security manager — treating run as successful");
        return;
      }
      throw e;
    }
  }

  private static void failAndExit(Exception e) {
    System.err.println("Error running native Spark pipeline: " + e.getMessage());
    e.printStackTrace();
    if (shouldAvoidSystemExitOnSuccess()) {
      // System.exit(1) is also trapped on Databricks; rethrow so the driver reports failure.
      if (e instanceof RuntimeException re) {
        throw re;
      }
      throw new RuntimeException("Error running native Spark pipeline: " + e.getMessage(), e);
    }
    try {
      System.exit(1);
    } catch (SecurityException se) {
      throw new RuntimeException("Error running native Spark pipeline: " + e.getMessage(), e);
    }
  }

  /**
   * True when running under Databricks Runtime or any JVM that installs a TrapExit-style security
   * manager (detected without Spark API imports at class load).
   */
  static boolean isDatabricksEnvironment() {
    if (StringUtils.isNotEmpty(System.getenv("DATABRICKS_RUNTIME_VERSION"))) {
      return true;
    }
    String sparkHome = System.getenv("SPARK_HOME");
    if (StringUtils.isNotEmpty(sparkHome) && sparkHome.contains("/databricks")) {
      return true;
    }
    return hasTrapExitSecurityManager();
  }

  /** Prefer returning from main over System.exit(0) when exit is trapped or forbidden. */
  static boolean shouldAvoidSystemExitOnSuccess() {
    return isDatabricksEnvironment();
  }

  static boolean hasTrapExitSecurityManager() {
    try {
      SecurityManager sm = System.getSecurityManager();
      if (sm == null) {
        return false;
      }
      String name = sm.getClass().getName();
      return name.contains("TrapExit") || name.contains("databricks");
    } catch (Throwable t) {
      return false;
    }
  }

  /** Databricks {@code ExitSecurityException}: "Program attempted to exit with code 0". */
  static boolean isTrappedExitZero(Throwable t) {
    if (!(t instanceof SecurityException)) {
      return false;
    }
    String msg = t.getMessage();
    if (msg == null) {
      return false;
    }
    String lower = msg.toLowerCase();
    return lower.contains("exit with code 0") || lower.contains("attempted to exit with code 0");
  }

  private static String readFileIntoString(String filename, String encoding) throws IOException {
    try (InputStream inputStream = HopVfs.getInputStream(filename)) {
      return IOUtils.toString(inputStream, encoding);
    } catch (Exception e) {
      throw new IOException("Error reading from file " + filename, e);
    }
  }
}
