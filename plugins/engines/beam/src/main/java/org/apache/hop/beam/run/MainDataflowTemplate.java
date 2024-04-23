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
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.commons.io.IOUtils;
import org.apache.hop.beam.pipeline.HopPipelineMetaToBeamPipelineConverter;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.PipelineMeta;

public class MainDataflowTemplate {
  public static final String EXPERIMENT_APACHE_HOP_PIPELINE = "apache_hop_pipeline";

  public interface Options extends StreamingOptions {
    @Description("Google Storage location with the Hop Metadata")
    @Validation.Required
    String getHopMetadataLocation();

    void setHopMetadataLocation(String value);

    @Description("Google Storage location of the pipeline you want to start")
    @Validation.Required
    String getHopPipelineLocation();

    void setHopPipelineLocation(String value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    System.out.println(">>>>>>");
    System.out.println(options.toString());

    try {
      HopEnvironment.init();

      // Read metadata from external source
      String pipelineMetaXml = readFileIntoString(options.getHopPipelineLocation(), "UTF-8");
      String metadataJson = readFileIntoString(options.getHopMetadataLocation(), "UTF-8");

      // default variable space
      IVariables variables = Variables.getADefaultVariableSpace();

      // Inflate the metadata:
      //
      SerializableMetadataProvider metadataProvider =
          new SerializableMetadataProvider(metadataJson);

      System.out.println(">>>>>> Loading pipeline metadata");
      PipelineMeta pipelineMeta =
          new PipelineMeta(
              XmlHandler.loadXmlString(pipelineMetaXml, PipelineMeta.XML_TAG), metadataProvider);

      HopPipelineMetaToBeamPipelineConverter hopPipelineMetaToBeamPipelineConverter =
          new HopPipelineMetaToBeamPipelineConverter(
              variables, pipelineMeta, metadataProvider, options, null, null);

      var pipeline = hopPipelineMetaToBeamPipelineConverter.createPipeline();
      pipeline.run();

    } catch (Exception e) {
      System.out.println(e);
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
