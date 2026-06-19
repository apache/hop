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

package org.apache.hop.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.variables.DescribedVariable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HopEnvironmentSystemPropertyTest {

  private static final String TEST_PROPERTY_NAME = "HOP_TEST_VAR_7174";
  private static final String COMMAND_LINE_VALUE = "1";
  private static final String CONFIG_FILE_VALUE = "0";

  @BeforeEach
  void setUp() {
    // Reset HopEnvironment before each test
    HopEnvironment.reset();
    // Remove test property if it exists
    System.clearProperty(TEST_PROPERTY_NAME);
  }

  @AfterEach
  void tearDown() {
    HopEnvironment.reset();
    System.clearProperty(TEST_PROPERTY_NAME);
  }

  @Test
  void testCommandLinePropertyNotOverriddenByConfig() {
    // Set a command-line property before initialization
    System.setProperty(TEST_PROPERTY_NAME, COMMAND_LINE_VALUE);

    // Add the same property to the config with a different value
    HopConfig hopConfig = HopConfig.getInstance();
    hopConfig.setDescribedVariable(
        new DescribedVariable(TEST_PROPERTY_NAME, CONFIG_FILE_VALUE, "Test variable"));

    // Initialize the environment
    try {
      HopEnvironment.init();
    } catch (Exception e) {
      // OK, environment might already be initialized or have other issues
    }

    // Verify that the command-line value is preserved, not overwritten
    String actualValue = System.getProperty(TEST_PROPERTY_NAME);
    assertEquals(
        COMMAND_LINE_VALUE,
        actualValue,
        "Command-line property should not be overwritten by config file value");
  }

  @Test
  void testConfigPropertyAppliedWhenNotSetViaCommandLine() {
    // Make sure the property is not set
    System.clearProperty(TEST_PROPERTY_NAME);

    // Add the property to the config
    HopConfig hopConfig = HopConfig.getInstance();
    hopConfig.setDescribedVariable(
        new DescribedVariable(TEST_PROPERTY_NAME, CONFIG_FILE_VALUE, "Test variable"));

    // Initialize the environment
    try {
      HopEnvironment.init();
    } catch (Exception e) {
      // OK, environment might already be initialized or have other issues
    }

    // Verify that the config value is applied
    String actualValue = System.getProperty(TEST_PROPERTY_NAME);
    assertEquals(
        CONFIG_FILE_VALUE,
        actualValue,
        "Config file property should be applied when not set via command-line");
  }
}
