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
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.spark.pkg.SparkProjectPackage;
import picocli.CommandLine;

/**
 * hop-conf option to build a Native Spark project package zip from a project home folder and its
 * {@code metadata/} directory.
 *
 * <p>Preferred usage (no Hop project registration required):
 *
 * <pre>
 * hop-conf.sh --export-spark-project=/tmp/demo.zip \
 *   --export-spark-project-home=/path/to/spark-demo
 * </pre>
 *
 * <p>Alternatively enable a registered project first with <strong>{@code -j}</strong> (not {@code
 * --project} / {@code -p}, which only name a project for create/delete/list management):
 *
 * <pre>
 * hop-conf.sh -j my-project --export-spark-project=/tmp/demo.zip
 * </pre>
 */
@ConfigPlugin(
    id = "ExportSparkProjectConfigPlugin",
    description = "Export a Native Spark project package zip (definitions + metadata.json)")
public class ExportSparkProjectConfigPlugin implements IConfigOptions {

  @CommandLine.Option(
      names = {"-esp", "--export-spark-project"},
      description =
          "Export a Native Spark project package zip (project/ tree + root metadata.json). "
              + "Skip bulk datasets/. Pair with --export-spark-project-home=<folder> or enable a "
              + "project with -j <name> first (not --project/-p). MainSpark: --HopProjectPackage=<zip>.")
  private String exportSparkProjectZip;

  @CommandLine.Option(
      names = {"--export-spark-project-home"},
      description =
          "Project home folder to package. Defaults to PROJECT_HOME when a project was enabled "
              + "with -j <name>. Prefer this for scripts so you need not register the project.")
  private String exportSparkProjectHome;

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    if (StringUtils.isEmpty(exportSparkProjectZip)) {
      return false;
    }

    String projectHome = resolveProjectHome(variables);
    String zip = variables.resolve(exportSparkProjectZip);

    log.logBasic("Exporting Native Spark project package from home=" + projectHome);
    log.logBasic("Destination zip: " + zip);

    IHopMetadataProvider metadataProvider =
        resolveMetadataProvider(projectHome, hasHopMetadataProvider, variables, log);

    SparkProjectPackage.exportProject(projectHome, zip, metadataProvider, variables);
    log.logBasic(
        "Spark project package written. Submit with MainSpark "
            + "--HopProjectPackage="
            + zip
            + " --HopPipelinePath=<relative.hpl> --HopRunConfigurationName=<config>");
    return true;
  }

  private String resolveProjectHome(IVariables variables) throws HopException {
    if (StringUtils.isNotEmpty(exportSparkProjectHome)) {
      return variables.resolve(exportSparkProjectHome);
    }
    String projectHome = variables.getVariable("PROJECT_HOME");
    if (StringUtils.isNotEmpty(projectHome)) {
      return variables.resolve(projectHome);
    }
    throw new HopException(
        "No project home for export. Either pass "
            + "--export-spark-project-home=/path/to/project "
            + "or enable a registered project first with -j <project-name> "
            + "(short option -j). Note: --project / -p only names a project for "
            + "create/delete/list and does not set PROJECT_HOME.");
  }

  /**
   * Prefer {@code <projectHome>/metadata} on disk so export works without enabling a Hop project.
   * Fall back to the hop-conf metadata provider (used when -j already reconfigured it).
   */
  private IHopMetadataProvider resolveMetadataProvider(
      String projectHome,
      IHasHopMetadataProvider hasHopMetadataProvider,
      IVariables variables,
      ILogChannel log)
      throws HopException {
    try {
      String metaFolder = projectHome;
      if (!metaFolder.endsWith("/") && !metaFolder.endsWith("\\")) {
        metaFolder = metaFolder + Const.FILE_SEPARATOR;
      }
      metaFolder = metaFolder + "metadata";
      FileObject metaFo = HopVfs.getFileObject(metaFolder);
      if (metaFo.exists() && metaFo.getType() == FileType.FOLDER) {
        log.logBasic("Loading metadata from " + metaFolder);
        return new JsonMetadataProvider(Encr.getEncoder(), metaFolder, variables);
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to open project metadata folder under " + projectHome, e);
    }

    if (hasHopMetadataProvider != null && hasHopMetadataProvider.getMetadataProvider() != null) {
      log.logBasic("Using hop-conf metadata provider (no metadata/ folder under project home)");
      return hasHopMetadataProvider.getMetadataProvider();
    }
    throw new HopException(
        "No metadata found under "
            + projectHome
            + "/metadata and no hop-conf metadata provider is available");
  }
}
