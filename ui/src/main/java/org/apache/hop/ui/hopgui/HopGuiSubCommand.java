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

package org.apache.hop.ui.hopgui;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.hop.plugin.HopSubCommand;
import org.apache.hop.hop.plugin.IHopSubCommand;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import picocli.CommandLine;

@Getter
@Setter
@CommandLine.Command(
    versionProvider = HopVersionProvider.class,
    mixinStandardHelpOptions = true,
    description = "The Hop GUI")
@HopSubCommand(id = "gui", description = "The Hop GUI")
public class HopGuiSubCommand implements Runnable, IHopSubCommand {
  @CommandLine.Unmatched private String[] unmatchedArguments;

  public HopGuiSubCommand() {}

  @Override
  public void initialize(
      CommandLine cmd, IVariables variables, MultiMetadataProvider metadataProvider)
      throws HopException {
    // Nothing specific
  }

  @Override
  public void run() {
    if (unmatchedArguments == null) {
      unmatchedArguments = new String[] {};
    }
    HopGui.main(unmatchedArguments);
  }
}
