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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;

import java.io.IOException;

public class MainBeam {

  public static void main(String[] args) {
    try {
      System.out.println("Argument 1 : Pipeline filename (.hpl)   : " + args[0]);
      System.out.println("Argument 2 : Metadata filename (.json)  : " + args[1]);
      System.out.println("Argument 3 : Pipeline run configuration : " + args[2]);

      System.out.println(">>>>>> Initializing Hop...");
      HopEnvironment.init();

      // Read the pipeline XML and metadata JSON (optionally from Hadoop FS)
      //
      Configuration hadoopConfiguration = new Configuration();
      String pipelineMetaXml = readFileIntoString(args[0], hadoopConfiguration, "UTF-8");
      String metadataJson = readFileIntoString(args[1], hadoopConfiguration, "UTF-8");
      String runConfigName = args[2];

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

      String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
      System.out.println(">>>>>> HADOOP_CONF_DIR='" + hadoopConfDir + "'");

      System.out.println(">>>>>> Building Apache Beam Pipeline...");
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin beamInputPlugin =
          registry.getPlugin( TransformPluginType.class, BeamConst.STRING_BEAM_INPUT_PLUGIN_ID);
      if (beamInputPlugin != null) {
        System.out.println(">>>>>> Found Beam Input transform plugin class loader");
      } else {
        throw new HopException("ERROR: Unable to find Beam Input transform plugin. Is it in the fat jar? ");
      }

      IVariables variables = Variables.getADefaultVariableSpace();

      // Execute it...
      //
      IPipelineEngine<PipelineMeta> pipelineEngine = PipelineEngineFactory.createPipelineEngine( variables, runConfigName, metadataProvider, pipelineMeta );
      System.out.println(">>>>>> Pipeline executing starting...");
      pipelineEngine.execute();
      pipelineEngine.waitUntilFinished();
      System.out.println(">>>>>> Execution finished...");
      System.exit( 0 );
    } catch (Exception e) {
      System.err.println("Error running Beam pipeline: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static String readFileIntoString(
      String filename, Configuration hadoopConfiguration, String encoding) throws IOException {
    Path path = new Path(filename);
    FileSystem fileSystem = FileSystem.get(path.toUri(), hadoopConfiguration);
    FSDataInputStream inputStream = fileSystem.open(path);
    String fileContent = IOUtils.toString(inputStream, encoding);
    return fileContent;
  }
}
