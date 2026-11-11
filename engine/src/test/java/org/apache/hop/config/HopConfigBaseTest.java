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

package org.apache.hop.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/** hop-conf only hands a config plugin its options when one of them was given (#9007). */
class HopConfigBaseTest {

  @Test
  void onlyPluginsWithAGivenOptionAreHandled() {
    TestConfig config = new TestConfig();
    CommandLine cmd = new CommandLine(config);
    FirstOptions first = new FirstOptions();
    SecondOptions second = new SecondOptions();
    cmd.addMixin("first", first);
    cmd.addMixin("second", second);
    config.setCmd(cmd);

    cmd.parseArgs("--first=value");
    config.run();

    assertEquals(1, first.calls);
    assertEquals(0, second.calls);
  }

  @Test
  void noPluginIsHandledWithoutOptions() {
    TestConfig config = new TestConfig();
    CommandLine cmd = new CommandLine(config);
    FirstOptions first = new FirstOptions();
    cmd.addMixin("first", first);
    config.setCmd(cmd);

    cmd.parseArgs("--level=Basic");
    config.run();

    assertEquals(0, first.calls);
  }

  @Test
  void optionsOfTheConfSubcommandAreMatched() {
    // 'hop conf ...' parses from the root command: the subcommand keeps its own parse result.
    //
    CommandLine root = new CommandLine(new RootCommand());
    TestConfig config = new TestConfig();
    CommandLine sub = new CommandLine(config);
    root.addSubcommand("conf", sub);
    FirstOptions first = new FirstOptions();
    SecondOptions second = new SecondOptions();
    sub.addMixin("first", first);
    sub.addMixin("second", second);
    config.setCmd(sub);

    root.parseArgs("conf", "--second");
    config.run();

    assertEquals(0, first.calls);
    assertEquals(1, second.calls);
  }

  @Test
  void pluginsWithAGivenPositionalParameterAreHandled() {
    TestConfig config = new TestConfig();
    CommandLine cmd = new CommandLine(config);
    FirstOptions first = new FirstOptions();
    PositionalOptions positional = new PositionalOptions();
    cmd.addMixin("first", first);
    cmd.addMixin("positional", positional);
    config.setCmd(cmd);

    cmd.parseArgs("value");
    config.run();

    assertEquals(0, first.calls);
    assertEquals(1, positional.calls);
  }

  @Test
  void unknownParseResultKeepsHandlingEveryPlugin() {
    assertTrue(HopConfigBase.hasMatchedOption(null, null));
    CommandLine cmd = new CommandLine(new FirstOptions());
    CommandLine.ParseResult parseResult = cmd.parseArgs();
    assertFalse(HopConfigBase.hasMatchedOption(parseResult, cmd.getCommandSpec()));
  }

  @CommandLine.Command
  static class RootCommand {}

  @CommandLine.Command
  static class TestConfig extends HopConfigBase {}

  static class FirstOptions implements IConfigOptions {
    @CommandLine.Option(names = "--first")
    String first;

    int calls;

    @Override
    public boolean handleOption(
        ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables) {
      calls++;
      return true;
    }
  }

  static class PositionalOptions implements IConfigOptions {
    @CommandLine.Parameters(arity = "0..1")
    String value;

    int calls;

    @Override
    public boolean handleOption(
        ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables) {
      calls++;
      return true;
    }
  }

  static class SecondOptions implements IConfigOptions {
    @CommandLine.Option(names = "--second")
    boolean second;

    int calls;

    @Override
    public boolean handleOption(
        ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables) {
      calls++;
      return true;
    }
  }
}
