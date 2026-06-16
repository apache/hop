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

package org.apache.hop.run;

import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.JarCache;
import org.apache.hop.hop.Hop;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataInstance;
import org.apache.hop.metadata.util.HopMetadataUtil;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.ParameterException;

@Getter
@Setter
@Command(versionProvider = HopVersionProvider.class)
public class HopRun extends HopRunBase implements Runnable, IHasHopMetadataProvider {

  public HopRun() {
    super();
  }

  public static void main(String[] args) {

    // Silence verbose JUL loggers from third-party JDBC drivers (e.g. Microsoft SQL Server)
    // that write INFO messages to stderr via the default ConsoleHandler. Hop uses its own
    // logging system (HopLogStore) so this JUL output is unwanted noise. (Fixes #7297)
    //
    silenceJulJdbcLoggers();

    HopRun hopRun = new HopRun();

    try {
      // Create the command line options...
      //
      hopRun.cmd = new CommandLine(hopRun);

      if (args.length > 0) {
        hopRun.prepareInternalOptions(new CommandLine(hopRun), args);
      }

      // Apply the system properties to the JVM
      //
      hopRun.applySystemProperties();

      // Initialize the Hop environment: load plugins and more
      //
      HopEnvironment.init();

      // Picks up the system settings in the variables
      //
      hopRun.buildVariableSpace();

      // Initialize the logging backend
      //
      HopLogStore.init();

      // Clear the jar file cache so that we don't waste memory...
      //
      JarCache.getInstance().clear();

      // Set up the metadata to use
      //
      hopRun.metadataProvider = HopMetadataUtil.getStandardHopMetadataProvider(hopRun.variables);
      HopMetadataInstance.setMetadataProvider(hopRun.metadataProvider);

      Hop.addMixinPlugins(hopRun.cmd, ConfigPlugin.CATEGORY_RUN);

      // This will calculate the option values and put them in HopRun or the plugin classes
      //
      CommandLine.ParseResult parseResult = hopRun.cmd.parseArgs(args);

      if (CommandLine.printHelpIfRequested(parseResult)) {
        System.exit(1);
      } else {
        hopRun.run();
        System.out.println("HopRun exit.");
        if (hopRun.isFinishedWithoutError()) {
          System.exit(0);
        } else {
          System.exit(1);
        }
      }
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      hopRun.cmd.usage(System.err);
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

  public void applySystemProperties() {
    // Set some System properties if there were any
    //
    if (systemProperties != null) {
      for (String parameter : systemProperties) {
        String[] split = parameter.split("=", 2);
        String key = split.length > 0 ? split[0] : null;
        String value = split.length > 1 ? split[1] : null;
        if (StringUtils.isNotEmpty(key) && StringUtils.isNotEmpty(value)) {
          System.setProperty(key, value);
        }
      }
    }
  }

  private void prepareInternalOptions(CommandLine cmd, String[] args) {
    for (String arg : args) {
      if (arg.startsWith("-h") || arg.startsWith("--help")) {
        return;
      }
    }

    String[] helpArgs = new String[args.length + 1];
    System.arraycopy(args, 0, helpArgs, 0, args.length);
    helpArgs[args.length] = "-h";

    cmd.parseArgs(helpArgs);
  }

  /**
   * Suppress INFO-level JUL output from third-party JDBC drivers that would otherwise flood stderr
   * via the default {@link java.util.logging.ConsoleHandler}. Hop uses its own logging subsystem
   * ({@link HopLogStore}) so JUL console output is unwanted noise.
   *
   * <p>This must be called early in {@link #main(String[])} — before {@link HopEnvironment#init()}
   * — so the loggers are silenced before any driver classes are loaded.
   */
  private static void silenceJulJdbcLoggers() {
    // Microsoft SQL Server JDBC driver (com.microsoft.sqlserver.jdbc.TDSTokenHandler et al.)
    Logger.getLogger("com.microsoft.sqlserver.jdbc").setLevel(Level.WARNING);
  }
}
