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

import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.hop.Hop;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.ParameterException;

@Command(versionProvider = HopVersionProvider.class)
public class HopConfig extends HopConfigBase implements Runnable, IHasHopMetadataProvider {

  public static void main(String[] args) {
    HopConfig hopConfig = new HopConfig();

    try {
      HopEnvironment.init();

      hopConfig.cmd = new CommandLine(hopConfig);

      Hop.addMixinPlugins(hopConfig.cmd, ConfigPlugin.CATEGORY_CONFIG);

      hopConfig.variables = Variables.getADefaultVariableSpace();

      hopConfig.buildLogChannel();

      hopConfig.buildMetadataProvider();

      CommandLine.ParseResult parseResult = hopConfig.cmd.parseArgs(args);
      if (CommandLine.printHelpIfRequested(parseResult)) {
        System.exit(1);
      } else {
        hopConfig.run();
        System.exit(0);
      }
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      hopConfig.cmd.usage(System.err);
      e.getCommandLine().usage(System.err);
      System.exit(9);
    } catch (ExecutionException e) {
      System.err.println("Error found during execution!");
      System.err.println(Const.getStackTracker(e));

      System.exit(1);
    } catch (Exception e) {
      System.err.println("General error found, something went horribly wrong!");
      System.err.println(Const.getStackTracker(e));

      System.exit(2);
    }
  }
}
