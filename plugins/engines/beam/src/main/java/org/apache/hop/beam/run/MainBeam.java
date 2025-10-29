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
 *
 */

package org.apache.hop.beam.run;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;

public class MainBeam {

  public static final String CONST_UTF_8 = "UTF-8";

  public static void main(String[] args) {
    try {
      System.out.println(">>>>>> Initializing Hop");
      HopEnvironment.init();

      // Read the pipeline XML and metadata JSON (optionally from Hadoop FS)
      //
      String pipelineMetaXml = "";
      String metadataJson = "";
      String runConfigName = "";
      String environmentFile = "";

      if (args[0].startsWith("--")) {
        for (String arg : args) {
          String[] split = arg.split("=", 2);
          String key = split.length > 0 ? split[0] : null;
          String value = split.length > 1 ? split[1] : null;
          if (key != null) {
            switch (key) {
              case ("--HopPipelinePath"):
                System.out.println("Pipeline filename (.hpl): " + value);
                pipelineMetaXml = readFileIntoString(value, CONST_UTF_8);
                break;
              case ("--HopMetadataPath"):
                System.out.println("Metadata filename (.json): " + value);
                metadataJson = readFileIntoString(value, CONST_UTF_8);
                break;
              case ("--HopRunConfigurationName"):
                System.out.println("Pipeline run configuration name: " + value);
                runConfigName = value;
                break;
              case ("--HopConfigFile"):
                System.out.println("Environnment configuration file: " + value);
                environmentFile = value;
                break;
              default:
                break;
            }
          }
        }
      } else {
        System.out.println("Argument 1 : Pipeline filename (.hpl)   : " + args[0]);
        pipelineMetaXml = readFileIntoString(args[0], CONST_UTF_8);
        System.out.println("Argument 2 : Environment state filename: (.json)  : " + args[1]);
        metadataJson = readFileIntoString(args[1], CONST_UTF_8);
        System.out.println("Argument 3 : Pipeline run configuration : " + args[2]);
        runConfigName = args[2];
        if (args.length > 3) {
          System.out.println("Argument 4 : Environment configuration file: " + args[3]);
          environmentFile = args[3];
        }
      }

      // Inflate the metadata:
      //
      SerializableMetadataProvider metadataProvider =
          new SerializableMetadataProvider(metadataJson);

      // Load the pipeline run configuration from this metadata provider:
      //
      IHopMetadataSerializer<PipelineRunConfiguration> serializer =
          metadataProvider.getSerializer(PipelineRunConfiguration.class);
      if (!serializer.exists(runConfigName)) {
        throw new HopException(
            "The specified pipeline run configuration '" + runConfigName + "' doesn't exist");
      }

      System.out.println(">>>>>> Loading pipeline metadata");
      PipelineMeta pipelineMeta =
          new PipelineMeta(
              XmlHandler.loadXmlString(pipelineMetaXml, PipelineMeta.XML_TAG), metadataProvider);

      System.out.println(">>>>>> Building Apache Beam Pipeline...");
      PluginRegistry registry = PluginRegistry.getInstance();

      // Validate that the fat jar was found and built correctly.
      // If it doesn't contain the Beam plugin we should just call it quits here.
      //
      IPlugin beamInputPlugin =
          registry.getPlugin(TransformPluginType.class, BeamConst.STRING_BEAM_INPUT_PLUGIN_ID);
      if (beamInputPlugin == null) {
        throw new HopException(
            "ERROR: Unable to find Beam Input transform plugin. Is it in the fat jar? ");
      }

      IVariables variables = Variables.getADefaultVariableSpace();

      // Apply the variables in the configuration file, if any is specified
      //
      if (StringUtils.isNotEmpty(environmentFile)) {
        DescribedVariablesConfigFile configFile =
            new DescribedVariablesConfigFile(variables.resolve(environmentFile));
        configFile.readFromFile();
        for (DescribedVariable variable : configFile.getDescribedVariables()) {
          variables.setVariable(variable.getName(), variable.getValue());
        }
        System.out.println(
            ">>>>>> Applied number of variables: " + configFile.getDescribedVariables().size());
      }

      // Execute it...
      //
      IPipelineEngine<PipelineMeta> pipelineEngine =
          PipelineEngineFactory.createPipelineEngine(
              variables, runConfigName, metadataProvider, pipelineMeta);
      System.out.println(">>>>>> Pipeline executing starting...");
      pipelineEngine.execute();
      pipelineEngine.waitUntilFinished();
      System.out.println(">>>>>> Execution finished...");
      System.exit(0);
    } catch (Exception e) {
      System.err.println("Error running Beam pipeline: " + e.getMessage());
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
