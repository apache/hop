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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.gui.HopBeamGuiPlugin;
import org.apache.hop.beam.pipeline.fatjar.FatJarBuilder;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import picocli.CommandLine;

import java.util.List;

@ConfigPlugin(
  id = "GenerateFatJarConfigPlugin",
  description = "Allows you to create a fat jar using the current Hop software installation"
)
public class GenerateFatJarConfigPlugin implements IConfigOptions {

  @CommandLine.Option( names = { "-fj", "--generate-fat-jar" }, description = "Specify the filename of the fat jar to generate from your current software installation" )
  private String fatJarFilename;

  @Override public boolean handleOption( ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables ) throws HopException {
    try {
      boolean changed = false;
      if ( StringUtils.isNotEmpty( fatJarFilename ) ) {
        createFatJar( log, variables );
        changed = true;
      }
      return changed;
    } catch ( Exception e ) {
      throw new HopException( "Error handling environment configuration options", e );
    }

  }

  private void createFatJar( ILogChannel log, IVariables variables ) throws HopException {
    String realFatJarFilename = variables.resolve( fatJarFilename );
    log.logBasic( "Generating a Hop fat jar file in : " + realFatJarFilename);

    List<String> installedJarFilenames = HopBeamGuiPlugin.findInstalledJarFilenames();
    log.logBasic( "Found " + installedJarFilenames.size()+" jar files to combine into one fat jar file.");

    FatJarBuilder fatJarBuilder = new FatJarBuilder( variables, realFatJarFilename, installedJarFilenames );
    fatJarBuilder.setExtraTransformPluginClasses( null );
    fatJarBuilder.setExtraXpPluginClasses( null );
    fatJarBuilder.buildTargetJar();

    log.logBasic( "Created fat jar." );
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
  public void setFatJarFilename( String fatJarFilename ) {
    this.fatJarFilename = fatJarFilename;
  }
}
