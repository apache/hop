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
 */
public class MainSpark {

  public static void main(String[] args) {
    try {
      System.out.println(">>>>>> Initializing Hop");
      HopEnvironment.init();

      MainSparkArgs parsed = MainSparkArgs.parse(args);
      System.out.println(
          "Argument 1 : Pipeline filename (.hpl)        : " + parsed.getPipelinePath());
      System.out.println(
          "Argument 2 : Metadata export filename (.json) : " + parsed.getMetadataPath());
      System.out.println(
          "Argument 3 : Pipeline run configuration       : " + parsed.getRunConfigName());
      if (StringUtils.isNotEmpty(parsed.getEnvironmentFile())) {
        System.out.println(
            "Argument 4 : Environment configuration file  : " + parsed.getEnvironmentFile());
      }

      String pipelineMetaXml = readFileIntoString(parsed.getPipelinePath(), Const.UTF_8);
      String metadataJson = readFileIntoString(parsed.getMetadataPath(), Const.UTF_8);
      String runConfigName = parsed.getRunConfigName();

      SerializableMetadataProvider metadataProvider =
          new SerializableMetadataProvider(metadataJson);

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
      pipelineMeta.setFilename(parsed.getPipelinePath());

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
        System.err.println("Pipeline finished with " + pipelineEngine.getErrors() + " error(s)");
        System.exit(1);
      }
      System.exit(0);
    } catch (Exception e) {
      System.err.println("Error running native Spark pipeline: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static String readFileIntoString(String filename, String encoding) throws IOException {
    try (InputStream inputStream = HopVfs.getInputStream(filename)) {
      return IOUtils.toString(inputStream, encoding);
    } catch (Exception e) {
      throw new IOException("Error reading from file " + filename, e);
    }
  }
}
