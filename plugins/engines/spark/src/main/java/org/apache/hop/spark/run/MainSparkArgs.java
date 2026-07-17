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

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;

/**
 * Parsed arguments for {@link MainSpark} (positional MainBeam-style or named {@code --Hop*} form).
 */
public final class MainSparkArgs {

  public static final String USAGE =
      "Usage:\n"
          + "  MainSpark <pipeline.hpl> <metadata.json> <runConfigName> [env-config.json]\n"
          + "  MainSpark --HopPipelinePath=... --HopMetadataPath=... --HopRunConfigurationName=..."
          + " [--HopConfigFile=...]\n"
          + "  MainSpark --HopProjectPackage=package.zip --HopPipelinePath=relative/or/path.hpl"
          + " --HopRunConfigurationName=... [--HopMetadataPath=...] [--HopConfigFile=...]";

  private final String pipelinePath;
  private final String metadataPath;
  private final String runConfigName;
  private final String environmentFile;
  private final String projectPackage;

  public MainSparkArgs(
      String pipelinePath,
      String metadataPath,
      String runConfigName,
      String environmentFile,
      String projectPackage) {
    this.pipelinePath = pipelinePath;
    this.metadataPath = metadataPath;
    this.runConfigName = runConfigName;
    this.environmentFile = environmentFile;
    this.projectPackage = projectPackage;
  }

  public String getPipelinePath() {
    return pipelinePath;
  }

  public String getMetadataPath() {
    return metadataPath;
  }

  public String getRunConfigName() {
    return runConfigName;
  }

  public String getEnvironmentFile() {
    return environmentFile;
  }

  /** Optional Spark project package zip URI ({@code --HopProjectPackage}). */
  public String getProjectPackage() {
    return projectPackage;
  }

  public boolean hasProjectPackage() {
    return StringUtils.isNotEmpty(projectPackage);
  }

  /**
   * Parse CLI arguments. Named form is used when the first argument starts with {@code --}.
   *
   * @throws HopException if required arguments are missing
   */
  public static MainSparkArgs parse(String[] args) throws HopException {
    if (args == null || args.length == 0) {
      throw new HopException("No arguments provided.\n" + USAGE);
    }

    String pipelinePath = null;
    String metadataPath = null;
    String runConfigName = null;
    String environmentFile = null;
    String projectPackage = null;

    if (args[0].startsWith("--")) {
      for (String arg : args) {
        String[] split = arg.split("=", 2);
        String key = split.length > 0 ? split[0] : null;
        String value = split.length > 1 ? split[1] : null;
        if (key == null) {
          continue;
        }
        switch (key) {
          case "--HopPipelinePath":
            pipelinePath = value;
            break;
          case "--HopMetadataPath":
            metadataPath = value;
            break;
          case "--HopRunConfigurationName":
            runConfigName = value;
            break;
          case "--HopConfigFile":
            environmentFile = value;
            break;
          case "--HopProjectPackage":
            projectPackage = value;
            break;
          default:
            break;
        }
      }
    } else {
      if (args.length < 3) {
        throw new HopException(
            "Expected at least 3 positional arguments: pipeline.hpl metadata.json runConfigName\n"
                + USAGE);
      }
      pipelinePath = args[0];
      metadataPath = args[1];
      runConfigName = args[2];
      if (args.length > 3) {
        environmentFile = args[3];
      }
    }

    if (StringUtils.isEmpty(pipelinePath) || StringUtils.isEmpty(runConfigName)) {
      throw new HopException("Pipeline path and run configuration name are required.\n" + USAGE);
    }
    if (StringUtils.isEmpty(projectPackage) && StringUtils.isEmpty(metadataPath)) {
      throw new HopException(
          "Metadata path is required unless --HopProjectPackage is set (package supplies"
              + " metadata.json).\n"
              + USAGE);
    }

    return new MainSparkArgs(
        pipelinePath, metadataPath, runConfigName, environmentFile, projectPackage);
  }
}
