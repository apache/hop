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

package org.apache.hop.hop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.config.plugin.ConfigPluginType;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.Plugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * A configuration plugin that can't be loaded or mixed in is skipped. The command line still starts
 * and the options of the other plugins still work.
 */
class HopAddMixinPluginsTest {

  private static final String CATEGORY = "add-mixin-plugins-test";

  private final List<IPlugin> registered = new ArrayList<>();

  @BeforeAll
  static void setUpClass() {
    // The skipped plugins are logged, as on the command line.
    HopLogStore.init();
  }

  @AfterEach
  void tearDown() {
    for (IPlugin plugin : registered) {
      PluginRegistry.getInstance().removePlugin(ConfigPluginType.class, plugin);
    }
  }

  @Test
  void aPluginWhoseClassIsMissingIsSkipped() throws Exception {
    register("a-working", WorkingOptions.class.getName());
    register("b-missing", "org.apache.hop.hop.DoesNotExistOptions");

    assertWorkingOptionStillParses();
  }

  @Test
  void aPluginThatFailsToBeCreatedIsSkipped() throws Exception {
    register("a-working", WorkingOptions.class.getName());
    register("b-failing", FailingOptions.class.getName());

    assertWorkingOptionStillParses();
  }

  @Test
  void aPluginPicocliRefusesIsSkipped() throws Exception {
    register("a-working", WorkingOptions.class.getName());
    register("b-no-options", NoOptions.class.getName());

    assertWorkingOptionStillParses();
  }

  @Test
  void aPluginWithADuplicateOptionIsSkipped() throws Exception {
    register("a-working", WorkingOptions.class.getName());
    register("b-duplicate", DuplicateOptions.class.getName());

    CommandLine cmd = assertWorkingOptionStillParses();
    assertFalse(cmd.getMixins().containsKey("b-duplicate"));
  }

  private CommandLine assertWorkingOptionStillParses() throws Exception {
    CommandLine cmd = new CommandLine(new TestCommand());
    Hop.addMixinPlugins(cmd, CATEGORY);

    assertTrue(cmd.getMixins().containsKey("a-working"), "mixins: " + cmd.getMixins().keySet());
    assertEquals(1, cmd.getMixins().size(), "mixins: " + cmd.getMixins().keySet());

    cmd.parseArgs("--working-option", "value");
    WorkingOptions options = (WorkingOptions) cmd.getMixins().get("a-working");
    assertEquals("value", options.workingOption);
    return cmd;
  }

  private void register(String id, String className) throws Exception {
    Map<Class<?>, String> classMap = new HashMap<>();
    classMap.put(IConfigOptions.class, className);
    Plugin plugin =
        new Plugin(
            new String[] {id},
            ConfigPluginType.class,
            IConfigOptions.class,
            CATEGORY,
            id,
            id,
            null,
            false,
            true,
            classMap,
            new ArrayList<>(),
            null,
            new String[0],
            null,
            false);
    PluginRegistry.getInstance().registerPlugin(ConfigPluginType.class, plugin);
    registered.add(plugin);
  }

  @Command(name = "test")
  public static class TestCommand {}

  public static class WorkingOptions implements IConfigOptions {
    @Option(names = "--working-option")
    String workingOption;

    @Override
    public boolean handleOption(
        ILogChannel log, IHasHopMetadataProvider metadataProvider, IVariables variables) {
      return false;
    }
  }

  /** Declares the same option name as {@link WorkingOptions}. */
  public static class DuplicateOptions extends WorkingOptions {}

  /** Declares nothing picocli accepts as a mixin. */
  public static class NoOptions implements IConfigOptions {
    @Override
    public boolean handleOption(
        ILogChannel log, IHasHopMetadataProvider metadataProvider, IVariables variables) {
      return false;
    }
  }

  public static class FailingOptions extends WorkingOptions {
    public FailingOptions() {
      throw new IllegalStateException("broken plugin");
    }
  }
}
