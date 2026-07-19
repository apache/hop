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

package org.apache.hop.beam.config;

import java.io.File;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.beam.gui.HopBeamGuiPlugin;
import org.apache.hop.beam.pipeline.fatjar.FatJarBuilder;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import picocli.CommandLine;

@ConfigPlugin(
    id = "GenerateFatJarConfigPlugin",
    description = "Allows you to create a fat jar using the current Hop software installation")
public class GenerateFatJarConfigPlugin implements IConfigOptions {

  @CommandLine.Option(
      names = {"-fj", "--generate-fat-jar"},
      description =
          "Specify the filename of the fat jar to generate from your current software installation")
  private String fatJarFilename;

  @CommandLine.Option(
      names = {"-scv", "--spark-client-version"},
      description =
          "Spark client pack version to embed (directory lib/spark-clients/<version>), "
              + "or special tokens: 'native' (embed Spark 4 from plugins/engines/spark/lib for hop-run), "
              + "'native-provided' (exclude Spark/Scala — use with spark-submit; cluster provides Spark). "
              + "When omitted, uses HOP_SPARK_CLIENT_VERSION or the default pack at lib/spark-client. "
              + "For Beam packs, client and Spark cluster minor versions must match for client-mode submit.")
  private String sparkClientVersion;

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    try {
      boolean changed = false;
      if (StringUtils.isNotEmpty(fatJarFilename)) {
        createFatJar(log, variables);
        changed = true;
      }
      return changed;
    } catch (Exception e) {
      throw new HopException("Error handling environment configuration options", e);
    }
  }

  private void createFatJar(ILogChannel log, IVariables variables) throws HopException {
    String realFatJarFilename = variables.resolve(fatJarFilename);
    log.logBasic("Generating a Hop fat jar file in : " + realFatJarFilename);

    String resolvedSparkClientVersion =
        HopBeamGuiPlugin.resolveSparkClientVersion(
            StringUtils.isNotEmpty(sparkClientVersion)
                ? variables.resolve(sparkClientVersion)
                : null);

    if (HopBeamGuiPlugin.isNativeSparkClientVersion(resolvedSparkClientVersion)) {
      boolean provided =
          HopBeamGuiPlugin.isNativeProvidedSparkClientVersion(resolvedSparkClientVersion);
      if (provided) {
        log.logBasic(
            "Native Spark fat jar mode (native-provided): excluding Beam Spark 3 / Scala 2.12 "
                + "and all Spark/Scala runtime jars. Use with spark-submit — cluster provides Spark.");
      } else {
        log.logBasic(
            "Native Spark 4 fat jar mode (native): excluding Beam Spark 3 client pack and Scala "
                + "2.12 jars; including plugins/engines/spark/lib (Spark 4.x / Scala 2.13). "
                + "For spark-submit prefer --spark-client-version=native-provided.");
      }
      File nativeSparkPlugin = new File("plugins/engines/spark");
      if (!nativeSparkPlugin.isDirectory()) {
        throw new HopException(
            "Native Spark engine plugin not found at "
                + nativeSparkPlugin.getPath()
                + " — install plugins/engines/spark (with lib/) before generating a native fat jar");
      }
      File nativeLib = new File(nativeSparkPlugin, "lib");
      if (!nativeLib.isDirectory() && !provided) {
        log.logBasic(
            "WARNING: plugins/engines/spark/lib is missing; fat jar may lack Spark 4 runtime jars");
      }
    } else {
      File packDir = HopBeamGuiPlugin.resolveSparkClientPackDir(resolvedSparkClientVersion);
      if (StringUtils.isNotBlank(resolvedSparkClientVersion)) {
        log.logBasic(
            "Using Spark client pack version "
                + resolvedSparkClientVersion
                + " from "
                + packDir.getPath());
        if (!packDir.isDirectory()) {
          throw new HopException(
              "Spark client pack not found: "
                  + packDir.getPath()
                  + " — materialise it with tools/spark-client-pack/materialize-pack.sh");
        }
      } else {
        log.logBasic("Using default Spark client pack from " + packDir.getPath());
      }
    }

    List<String> installedJarFilenames =
        HopBeamGuiPlugin.findInstalledJarFilenames(resolvedSparkClientVersion);
    log.logBasic(
        "Found " + installedJarFilenames.size() + " jar files to combine into one fat jar file.");

    FatJarBuilder fatJarBuilder =
        new FatJarBuilder(log, variables, realFatJarFilename, installedJarFilenames);
    fatJarBuilder.setExtraTransformPluginClasses(null);
    fatJarBuilder.setExtraXpPluginClasses(null);
    fatJarBuilder.buildTargetJar();

    log.logBasic("Created fat jar.");
  }

  /**
   * Gets fatJarFilename
   *
   * @return value of fatJarFilename
   */
  public String getFatJarFilename() {
    return fatJarFilename;
  }

  /**
   * @param fatJarFilename The fatJarFilename to set
   */
  public void setFatJarFilename(String fatJarFilename) {
    this.fatJarFilename = fatJarFilename;
  }

  public String getSparkClientVersion() {
    return sparkClientVersion;
  }

  public void setSparkClientVersion(String sparkClientVersion) {
    this.sparkClientVersion = sparkClientVersion;
  }
}
