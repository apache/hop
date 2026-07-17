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

package org.apache.hop.spark.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.spark.pkg.SparkProjectPackage;
import picocli.CommandLine;

/**
 * hop-conf option to build a Native Spark project package zip from the active project home ({@code
 * PROJECT_HOME}) and metadata provider.
 *
 * <pre>
 * hop-conf.sh -j my-project --export-spark-project=/tmp/my-project-spark.zip
 * </pre>
 */
@ConfigPlugin(
    id = "ExportSparkProjectConfigPlugin",
    description = "Export a Native Spark project package zip (definitions + metadata.json)")
public class ExportSparkProjectConfigPlugin implements IConfigOptions {

  @CommandLine.Option(
      names = {"-esp", "--export-spark-project"},
      description =
          "Export a Native Spark project package zip from PROJECT_HOME (pipelines, workflows, "
              + "small resources under project/, plus root metadata.json). Skip bulk datasets/. "
              + "Use with -j/--project so PROJECT_HOME and metadata are active. "
              + "MainSpark: --HopProjectPackage=<this zip>.")
  private String exportSparkProjectZip;

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    if (StringUtils.isEmpty(exportSparkProjectZip)) {
      return false;
    }
    String projectHome = variables.getVariable("PROJECT_HOME");
    if (StringUtils.isEmpty(projectHome)) {
      throw new HopException(
          "PROJECT_HOME is not set. Enable a project first (e.g. hop-conf -j my-project "
              + "--export-spark-project=/path/package.zip).");
    }
    String zip = variables.resolve(exportSparkProjectZip);
    log.logBasic("Exporting Native Spark project package from PROJECT_HOME=" + projectHome);
    log.logBasic("Destination zip: " + zip);
    SparkProjectPackage.exportProject(
        projectHome, zip, hasHopMetadataProvider.getMetadataProvider(), variables);
    log.logBasic(
        "Spark project package written. Submit with MainSpark "
            + "--HopProjectPackage="
            + zip
            + " --HopPipelinePath=<relative.hpl> --HopRunConfigurationName=<config>");
    return true;
  }
}
