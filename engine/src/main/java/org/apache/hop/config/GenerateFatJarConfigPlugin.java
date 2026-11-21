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

package org.apache.hop.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.fatjar.FatJarEngine;
import org.apache.hop.fatjar.FatJarGenerator;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import picocli.CommandLine;

@ConfigPlugin(
    id = "GenerateFatJarConfigPlugin",
    description = "Allows you to create a fat jar using the current Hop software installation")
@Getter
@Setter
public class GenerateFatJarConfigPlugin implements IConfigOptions {

  /** Former {@code --spark-client-version} token for a native Spark jar with the Spark runtime. */
  static final String NATIVE = "native";

  @CommandLine.Option(
      names = {"-fj", "--generate-fat-jar"},
      description =
          "Specify the filename of the fat jar to generate from your current software installation")
  private String fatJarFilename;

  @CommandLine.Option(
      names = {"-fje", "--fat-jar-engine"},
      description =
          "The engine the fat jar is for: 'spark' (native Spark engine: without what a Spark"
              + " distribution ships, which the cluster provides, without Beam, and with the SWT"
              + " jar of one platform) or 'beam' (the Beam engines). Default: 'beam'.")
  private String fatJarEngine;

  @CommandLine.Option(
      names = {"-fjsr", "--include-spark-runtime"},
      description =
          "With --fat-jar-engine=spark: also embed Spark 4 and Scala, for a JVM without Spark that"
              + " starts the Spark session itself. Not for spark-submit or a cluster.")
  private boolean includeSparkRuntime;

  @CommandLine.Option(
      names = {"-scv", "--spark-client-version"},
      description =
          "With --fat-jar-engine=beam: the Spark client pack of the Beam Spark runner (folder"
              + " lib/spark-clients/<version>). When omitted, uses HOP_SPARK_CLIENT_VERSION or the"
              + " default pack at lib/spark-client. The former values 'native' and"
              + " 'native-provided' still select --fat-jar-engine=spark (with and without"
              + " --include-spark-runtime).")
  private String sparkClientVersion;

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    if (StringUtils.isEmpty(fatJarFilename)) {
      return false;
    }
    String version =
        StringUtils.isNotEmpty(sparkClientVersion)
            ? variables.resolve(sparkClientVersion)
            : FatJarGenerator.sparkClientVersionFromEnvironment();
    boolean runtime = includeSparkRuntime;
    FatJarEngine engine;
    if (StringUtils.isNotEmpty(fatJarEngine)) {
      engine = FatJarEngine.lookup(variables.resolve(fatJarEngine));
      if (engine == null) {
        throw new HopException(
            "Unknown fat jar engine '" + fatJarEngine + "': use 'spark' or 'beam'.");
      }
    } else if (isNativeToken(version)) {
      engine = FatJarEngine.SPARK;
      runtime = NATIVE.equalsIgnoreCase(version.trim());
      log.logBasic(
          "--spark-client-version="
              + version.trim()
              + " is the former way to ask for a native Spark fat jar: use --fat-jar-engine=spark"
              + (runtime ? " --include-spark-runtime" : "")
              + " instead.");
    } else {
      engine = FatJarEngine.BEAM;
    }
    if (engine == FatJarEngine.SPARK && version != null && !isNativeToken(version)) {
      log.logBasic(
          "Ignoring --spark-client-version=" + version + ": it only applies to a Beam fat jar.");
    }

    log.logBasic(
        "Generating a Hop fat jar for "
            + engine.getDescription()
            + " in : "
            + variables.resolve(fatJarFilename));
    FatJarGenerator.generate(
        log,
        variables,
        engine,
        fatJarFilename,
        engine == FatJarEngine.BEAM ? version : null,
        engine == FatJarEngine.SPARK && runtime);
    log.logBasic("Created fat jar.");
    return true;
  }

  static boolean isNativeToken(String version) {
    return FatJarGenerator.isNativeSparkToken(version);
  }
}
