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

package org.apache.hop.driver;

import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.hop.plugin.HopCommand;
import org.apache.hop.hop.plugin.IHopCommand;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * The {@code hop driver} command: manage JDBC drivers. Holds the {@code list} and {@code install}
 * subcommands.
 *
 * <p>Apache Hop does not bundle restricted (proprietary / GPL / LGPL) JDBC drivers. This command
 * downloads them from Maven Central onto this machine, at your explicit request, after you accept
 * the vendor license. See {@code hop driver install --help}.
 */
@SuppressWarnings("java:S106")
@Command(
    name = "driver",
    description = "Manage JDBC drivers (list, install)",
    versionProvider = HopVersionProvider.class,
    mixinStandardHelpOptions = true,
    subcommands = {DriverListCommand.class, DriverInstallCommand.class})
@HopCommand(id = "driver", description = "Manage JDBC drivers (list, install)")
public class HopDriver implements IHopCommand, Runnable {

  @Spec private CommandSpec spec;

  @Override
  public void initialize(
      CommandLine cmd, IVariables variables, MultiMetadataProvider metadataProvider)
      throws HopException {
    // Nothing to initialize: the subcommands are self-contained.
  }

  @Override
  public void run() {
    // No subcommand given: show usage.
    spec.commandLine().usage(System.out);
  }
}
